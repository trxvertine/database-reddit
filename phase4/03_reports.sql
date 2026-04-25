-- =============================================================================
-- Phase 4 | 03_reports.sql
-- Real-life data-operation queries aligned with the Phase 3 business logic.
-- -----------------------------------------------------------------------------
-- The synthetic dataset spans 2023-01-01 .. 2025-01-01. Any query that would
-- normally filter on "NOW() - interval" instead filters relative to
--   reference_now := (SELECT MAX(created_at) FROM post)
-- so that the reports return meaningful rows against the historical data.
--
-- Each section prints a short header via \echo so the output is easy to read.
-- =============================================================================

\pset pager off
\pset border 2
\pset null '·'

-- -----------------------------------------------------------------------------
-- Helper: reference_now() as a temporary view so every query reads from the
-- same clock. Falls back to NOW() if the post table is empty.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_ref_now AS
SELECT COALESCE((SELECT MAX(created_at) FROM post), NOW()) AS ref_now;


-- =============================================================================
-- SECTION A - Analytical & reporting queries (Phase 3 §Analytical)
-- =============================================================================

-- A1. Top Contributors by Karma -----------------------------------------------
-- 10 users with the highest denormalized karma plus their post/comment counts
-- and average post score. Feeds the "verified contributor" badge.
\echo '=== A1. Top 10 contributors by karma ==='
SELECT  u.username,
        u.karma,
        COUNT(DISTINCT p.id)                     AS post_count,
        COUNT(DISTINCT c.id)                     AS comment_count,
        ROUND(AVG(p.score)::NUMERIC, 2)          AS avg_post_score
  FROM  "user" u
  LEFT  JOIN post    p ON p.author_id = u.id
  LEFT  JOIN comment c ON c.author_id = u.id
 GROUP  BY u.id, u.username, u.karma
 ORDER  BY u.karma DESC
 LIMIT  10;

-- A2. Most Active Groups (last 30 days of simulated time) ---------------------
\echo '=== A2. Most active groups (last 30 days) ==='
WITH window_cfg AS (SELECT ref_now - INTERVAL '30 days' AS since FROM v_ref_now)
SELECT  g.name,
        COUNT(DISTINCT p.id)                            AS new_posts,
        g.member_count                                  AS followers,
        (SELECT COUNT(*) FROM moderator m WHERE m.subreddit_id = g.id) AS mods
  FROM  "group" g
  JOIN  post    p ON p.subreddit_id = g.id
  CROSS JOIN window_cfg w
 WHERE  p.created_at >= w.since
 GROUP  BY g.id, g.name, g.member_count
 ORDER  BY new_posts DESC
 LIMIT  15;

-- A3. Trending Posts (Hot Score) ---------------------------------------------
-- Formula: score / (age_in_hours + 2)^1.8 on posts from the last 48h of
-- simulated time. Borrowed from Reddit's early ranking algorithm.
\echo '=== A3. Trending posts (hot score, last 48h) ==='
WITH cfg AS (SELECT ref_now AS now_ FROM v_ref_now)
SELECT  p.title,
        g.name                                    AS subreddit,
        p.score,
        ROUND(EXTRACT(EPOCH FROM (c.now_ - p.created_at))/3600, 1) AS age_h,
        ROUND((p.score::NUMERIC /
               POWER(EXTRACT(EPOCH FROM (c.now_ - p.created_at))/3600 + 2, 1.8)
              )::NUMERIC, 4)                      AS hot_score
  FROM  post p
  JOIN  "group" g ON g.id = p.subreddit_id
  CROSS JOIN cfg c
 WHERE  p.created_at >= c.now_ - INTERVAL '48 hours'
 ORDER  BY hot_score DESC
 LIMIT  25;

-- A4. Engagement Rate per Group (last 90 days, min 5 posts) -------------------
\echo '=== A4. Engagement rate per group (comments/post, last 90 days) ==='
WITH cfg AS (SELECT ref_now - INTERVAL '90 days' AS since FROM v_ref_now)
SELECT  g.name,
        COUNT(DISTINCT p.id)                                AS posts_90d,
        COUNT(c.id)                                         AS comments_90d,
        ROUND(COUNT(c.id)::NUMERIC / NULLIF(COUNT(DISTINCT p.id), 0), 2)
                                                            AS comments_per_post
  FROM  "group" g
  JOIN  post    p ON p.subreddit_id = g.id
  LEFT  JOIN comment c
         ON c.post_id = p.id
        AND c.created_at >= (SELECT since FROM cfg)
 WHERE  p.created_at >= (SELECT since FROM cfg)
 GROUP  BY g.id, g.name
HAVING  COUNT(DISTINCT p.id) >= 5
 ORDER  BY comments_per_post DESC
 LIMIT  20;

-- A5. Inactive Users (re-engagement target list) ------------------------------
-- Users whose account is >30 days old and who have not posted/commented in
-- the last 90 days of simulated time.
\echo '=== A5. Inactive users (no activity in 90d, account > 30d old) ==='
WITH cfg AS (SELECT ref_now - INTERVAL '90 days' AS activity_cutoff,
                    ref_now - INTERVAL '30 days' AS account_cutoff
               FROM v_ref_now)
SELECT  u.id, u.username, u.email, u.created_at
  FROM  "user" u, cfg
 WHERE  u.created_at < cfg.account_cutoff
   AND  NOT EXISTS (SELECT 1 FROM post    p WHERE p.author_id = u.id AND p.created_at >= cfg.activity_cutoff)
   AND  NOT EXISTS (SELECT 1 FROM comment c WHERE c.author_id = u.id AND c.created_at >= cfg.activity_cutoff)
 ORDER  BY u.created_at
 LIMIT  25;

-- A6. Report Resolution Time by Moderator, grouped by month -------------------
\echo '=== A6. Report resolution time by moderator & month ==='
SELECT  u.username                                                AS moderator,
        DATE_TRUNC('month', r.resolved_at)::DATE                  AS month,
        COUNT(*)                                                  AS reports_handled,
        ROUND(AVG(EXTRACT(EPOCH FROM (r.resolved_at - r.created_at))/3600)::NUMERIC, 2)
                                                                  AS avg_hours_to_resolve
  FROM  report r
  JOIN  "user" u ON u.id = r.resolved_by
 WHERE  r.status <> 'pending'
 GROUP  BY u.username, DATE_TRUNC('month', r.resolved_at)
 ORDER  BY month DESC, reports_handled DESC
 LIMIT  20;

-- A7. Most Reported Content (pending) - UNION between posts and comments -----
\echo '=== A7. Top 20 targets by pending reports ==='
(
  SELECT  'post'::TEXT        AS kind,
          p.id                AS target_id,
          LEFT(p.title, 60)   AS preview,
          g.name              AS subreddit,
          COUNT(r.id)         AS pending_reports
    FROM  report r
    JOIN  post   p ON p.id = r.target_id AND r.target_type = 'post'
    JOIN  "group" g ON g.id = p.subreddit_id
   WHERE  r.status = 'pending'
   GROUP  BY p.id, p.title, g.name
)
UNION ALL
(
  SELECT  'comment'::TEXT     AS kind,
          c.id                AS target_id,
          LEFT(c.body, 60)    AS preview,
          g.name              AS subreddit,
          COUNT(r.id)         AS pending_reports
    FROM  report r
    JOIN  comment c ON c.id = r.target_id AND r.target_type = 'comment'
    JOIN  post    p ON p.id = c.post_id
    JOIN  "group" g ON g.id = p.subreddit_id
   WHERE  r.status = 'pending'
   GROUP  BY c.id, c.body, g.name
)
ORDER  BY pending_reports DESC
LIMIT  20;

-- A8. Notification Read Rate by Type -----------------------------------------
\echo '=== A8. Notification read rate by type ==='
SELECT  type,
        COUNT(*)                                                         AS total,
        SUM(CASE WHEN is_read THEN 1 ELSE 0 END)                         AS read_count,
        ROUND(100.0 * SUM(CASE WHEN is_read THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                         AS read_pct
  FROM  notification
 GROUP  BY type
 ORDER  BY read_pct DESC;

-- A9. Co-followers Recommendation ("people you may know") --------------------
-- For a specific user (first one in the table here, swap at will), find other
-- users who share >=3 followed groups but aren't already followed.
\echo '=== A9. Co-followers recommendation (sample user) ==='
WITH target_user AS (SELECT id FROM "user" ORDER BY karma DESC LIMIT 1),
     my_groups   AS (SELECT subreddit_id FROM follow WHERE user_id = (SELECT id FROM target_user))
SELECT  u.id, u.username,
        COUNT(DISTINCT f.subreddit_id) AS shared_groups
  FROM  follow f
  JOIN  "user" u ON u.id = f.user_id
 WHERE  f.subreddit_id IN (SELECT subreddit_id FROM my_groups)
   AND  u.id <> (SELECT id FROM target_user)
   AND  u.id NOT IN (SELECT user_id FROM follow
                      WHERE subreddit_id IN (SELECT subreddit_id FROM my_groups)
                        AND user_id = (SELECT id FROM target_user))
 GROUP  BY u.id, u.username
HAVING  COUNT(DISTINCT f.subreddit_id) >= 3
 ORDER  BY shared_groups DESC
 LIMIT  10;

-- A10. Tag Usage Frequency per Group (last 60 days, ranked with window fn) ----
\echo '=== A10. Tag usage per group, ranked (last 60 days) ==='
WITH cfg AS (SELECT ref_now - INTERVAL '60 days' AS since FROM v_ref_now),
     usage AS (
        SELECT  g.id   AS group_id,
                g.name AS group_name,
                t.id   AS tag_id,
                t.text AS tag_text,
                COUNT(pt.post_id) AS uses
          FROM  "group" g
          JOIN  tag t ON t.subreddit_id = g.id
          LEFT  JOIN post_tag pt ON pt.tag_id = t.id
                                  AND pt.assigned_at >= (SELECT since FROM cfg)
         GROUP  BY g.id, g.name, t.id, t.text
     )
SELECT  group_name, tag_text, uses,
        RANK() OVER (PARTITION BY group_id ORDER BY uses DESC) AS rnk
  FROM  usage
 WHERE  uses > 0
 ORDER  BY group_name, rnk
 LIMIT  50;


-- =============================================================================
-- SECTION B - Join-type demonstrations (Phase 3 §JoinQuerySpecifications)
-- =============================================================================

-- B1. INNER JOIN: post feed with author + subreddit ---------------------------
\echo '=== B1. INNER JOIN (post feed) ==='
SELECT  p.title, u.username AS author, g.name AS subreddit, p.score
  FROM  post p
  INNER JOIN "user"  u ON u.id = p.author_id
  INNER JOIN "group" g ON g.id = p.subreddit_id
 ORDER  BY p.created_at DESC
 LIMIT  10;

-- B2. LEFT OUTER JOIN: contributor leaderboard --------------------------------
\echo '=== B2. LEFT JOIN (users with post counts, zero-post users included) ==='
SELECT  u.username, COUNT(p.id) AS posts
  FROM  "user" u
  LEFT  JOIN post p ON p.author_id = u.id
 GROUP  BY u.id, u.username
 ORDER  BY posts DESC, u.username
 LIMIT  10;

-- B3. RIGHT JOIN: groups with moderator count (include unmoderated) ----------
\echo '=== B3. RIGHT JOIN (groups missing moderators surface as 0) ==='
SELECT  g.name, COUNT(m.user_id) AS mod_count
  FROM  moderator m
 RIGHT  JOIN "group" g ON g.id = m.subreddit_id
 GROUP  BY g.id, g.name
HAVING  COUNT(m.user_id) = 0
 ORDER  BY g.name
 LIMIT  10;

-- B4. FULL JOIN: user<->group creator audit -----------------------------------
\echo '=== B4. FULL JOIN (users vs groups, creator audit) ==='
SELECT  u.username, g.name AS created_group
  FROM  "user" u
  FULL  JOIN "group" g ON g.created_by = u.id
 WHERE  u.id IS NULL OR g.id IS NULL       -- only unmatched rows
 LIMIT  10;

-- B5. SELF JOIN: reply threads ------------------------------------------------
\echo '=== B5. SELF JOIN (comment + its parent) ==='
SELECT  child.id   AS reply_id,
        LEFT(child.body,  40) AS reply_preview,
        parent.id  AS parent_id,
        LEFT(parent.body, 40) AS parent_preview
  FROM  comment child
  JOIN  comment parent ON parent.id = child.parent_comment_id
 LIMIT  10;

-- B6. CROSS JOIN (bounded): missing tag suggestions for one group -------------
-- We pick a single small group to keep the Cartesian bounded.
\echo '=== B6. CROSS JOIN bounded (tag suggestions for one group) ==='
WITH target AS (
    SELECT id FROM "group"
     WHERE id IN (SELECT subreddit_id FROM tag)
     ORDER BY member_count DESC
     LIMIT 1
)
SELECT  LEFT(p.title, 50) AS post_title, t.text AS suggested_tag
  FROM  post p
 CROSS  JOIN tag t
 WHERE  p.subreddit_id = (SELECT id FROM target)
   AND  t.subreddit_id = (SELECT id FROM target)
   AND  NOT EXISTS (SELECT 1 FROM post_tag pt
                     WHERE pt.post_id = p.id AND pt.tag_id = t.id)
 LIMIT  15;


-- =============================================================================
-- SECTION C - Subquery demonstrations (Phase 3 §SubquerySpecifications)
-- =============================================================================

-- C1. Scalar subquery in SELECT ----------------------------------------------
\echo '=== C1. Scalar subquery (live comment count per post) ==='
SELECT  p.title,
        p.comment_count                                           AS denorm_count,
        (SELECT COUNT(*) FROM comment c WHERE c.post_id = p.id)   AS live_count
  FROM  post p
 ORDER  BY p.created_at DESC
 LIMIT  10;

-- C2. Correlated subquery in WHERE -------------------------------------------
\echo '=== C2. Correlated subquery (users who downvoted recently) ==='
WITH cfg AS (SELECT ref_now - INTERVAL '7 days' AS since FROM v_ref_now)
SELECT  u.username
  FROM  "user" u
 WHERE  EXISTS (
        SELECT 1
          FROM vote v, cfg
         WHERE v.user_id   = u.id
           AND v.value     = -1
           AND v.created_at >= cfg.since)
 ORDER  BY u.username
 LIMIT  10;

-- C3. IN subquery ------------------------------------------------------------
\echo '=== C3. IN subquery (posts in NSFW groups) ==='
SELECT  p.id, LEFT(p.title, 60) AS title
  FROM  post p
 WHERE  p.subreddit_id IN (SELECT id FROM "group" WHERE is_nsfw = TRUE)
 LIMIT  10;

-- C4. NOT IN subquery --------------------------------------------------------
\echo '=== C4. NOT IN subquery (users who never filed a report) ==='
SELECT  u.id, u.username
  FROM  "user" u
 WHERE  u.id NOT IN (SELECT reporter_id FROM report WHERE reporter_id IS NOT NULL)
 LIMIT  10;

-- C5. Derived table in FROM --------------------------------------------------
\echo '=== C5. Derived table (avg karma of top 10% vs whole platform) ==='
SELECT  (SELECT ROUND(AVG(karma)::NUMERIC, 2) FROM "user")        AS platform_avg,
        (SELECT ROUND(AVG(sub.karma)::NUMERIC, 2)
           FROM (SELECT karma FROM "user"
                  ORDER BY karma DESC
                  LIMIT (SELECT CEIL(COUNT(*)/10.0) FROM "user")) sub) AS top10_avg;

-- C6. EXISTS subquery --------------------------------------------------------
\echo '=== C6. EXISTS (groups with pending reports) ==='
SELECT  g.name
  FROM  "group" g
 WHERE  EXISTS (
        SELECT 1
          FROM post p
          JOIN report r ON r.target_id = p.id AND r.target_type = 'post'
         WHERE p.subreddit_id = g.id
           AND r.status = 'pending')
 ORDER  BY g.name
 LIMIT  10;

-- =============================================================================
-- End of reports.
-- =============================================================================

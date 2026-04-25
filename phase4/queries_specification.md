# Queries Specification — Phase 4

**Team:** Marc Petrosyan, Vahan Zakaryan, Sergey Hayriyan
**Database:** `reddit_db` (PostgreSQL 14+)

This document catalogues every query delivered in `03_reports.sql`. For each one
it states the business purpose, the tables touched, the SQL construct being
demonstrated, and a short justification for the shape of the query. All
time-based filters reference the helper view `v_ref_now`, which pins "now" to
the maximum `post.created_at` in the synthetic dataset so that `INTERVAL '30
days'`-style predicates continue to return meaningful rows.

> Convention: section numbers below mirror the labels printed by `\echo` inside
> `03_reports.sql` (A1…A10, B1…B6, C1…C6).

---

## Section A — Analytical & Reporting Queries

### A1. Top Contributors by Karma
- **Purpose.** Return the 10 users with the highest karma, along with their
  post count, comment count, and average post score. Feeds the "verified
  contributor" badge system.
- **Tables:** `user`, `post`, `comment`.
- **Techniques:** `LEFT JOIN`, aggregation (`COUNT DISTINCT`, `AVG`),
  `ORDER BY … LIMIT`.
- **Why this shape?** `LEFT JOIN` ensures a user with zero posts or comments
  still appears if their denormalized karma is high (e.g. legacy awards).

### A2. Most Active Groups (last 30 days)
- **Purpose.** Rank subreddits by new posts in a 30-day window, alongside
  current followers and moderator count. Used by the editorial team for
  front-page curation.
- **Tables:** `group`, `post`, `follow`, `moderator`.
- **Techniques:** date filtering, `JOIN`, `GROUP BY`, correlated scalar
  subquery (`mods`).

### A3. Trending Posts (hot-score formula)
- **Purpose.** Produce the ranked feed for the 48-hour front page.
- **Formula.** `score / (age_in_hours + 2) ^ 1.8`, matching the early Reddit
  algorithm.
- **Tables:** `post`, `group`.
- **Techniques:** computed expression in `SELECT`/`ORDER BY`, `EXTRACT(EPOCH …)`.

### A4. Engagement Rate per Group
- **Purpose.** Comments-per-post over a 90-day window, excluding groups with
  fewer than 5 posts in the window (otherwise sample size is misleading).
- **Tables:** `group`, `post`, `comment`.
- **Techniques:** `LEFT JOIN`, ratio aggregation, `HAVING`.

### A5. Inactive Users
- **Purpose.** Target list for re-engagement emails. Users whose account is
  >30 days old and who have not posted or commented in the last 90 days.
- **Tables:** `user`, `post`, `comment`.
- **Techniques:** `NOT EXISTS` (two branches), timestamp arithmetic.

### A6. Report Resolution Time by Moderator
- **Purpose.** Performance review data: average hours to resolve + total
  reports handled, broken down by calendar month.
- **Tables:** `report`, `user`.
- **Techniques:** `DATE_TRUNC('month', …)`, `EXTRACT(EPOCH …)`,
  `GROUP BY` on truncated date.

### A7. Most Reported Content (pending)
- **Purpose.** Priority queue for moderator review. Combines posts and
  comments into a single ranked list.
- **Tables:** `report`, `post`, `comment`, `group`.
- **Techniques:** `UNION ALL` of two structurally compatible subqueries,
  final `ORDER BY` across the union.

### A8. Notification Read Rate by Type
- **Purpose.** Evaluate which notification types drive engagement.
- **Tables:** `notification`.
- **Techniques:** conditional aggregation (`SUM(CASE …)`), percentage
  arithmetic.

### A9. Co-followers Recommendation
- **Purpose.** Lightweight "people you may know" — users who follow ≥3 of
  the same groups as the target user but are not already known to them.
- **Tables:** `follow`, `user`.
- **Techniques:** CTEs, self-reference via `NOT IN`, threshold via `HAVING`.

### A10. Tag Usage Frequency per Group
- **Purpose.** Moderator tool for tag curation — retire dead tags, promote
  active ones. 60-day window.
- **Tables:** `tag`, `post_tag`, `group`.
- **Techniques:** window function `RANK() OVER (PARTITION BY group_id …)`,
  `LEFT JOIN` to include zero-use tags.

---

## Section B — Join-Type Demonstrations

| # | Join | Tables | Business use |
|---|------|--------|--------------|
| B1 | `INNER JOIN` | `post`, `user`, `group` | Standard content feed: post title + author + subreddit |
| B2 | `LEFT JOIN` | `user`, `post` | Leaderboard including users who have never posted |
| B3 | `RIGHT JOIN` | `moderator`, `group` | Detect unmaintained communities (0 moderators) |
| B4 | `FULL JOIN` | `user`, `group` | Data-integrity audit: users with no groups AND groups with no creator |
| B5 | `SELF JOIN` | `comment` × `comment` | Render a reply + its parent side-by-side |
| B6 | `CROSS JOIN` (bounded) | `post`, `tag` | Tag suggestions — Cartesian bounded to one group |

---

## Section C — Subquery Demonstrations

| # | Kind | Where it lives | Business use |
|---|------|----------------|--------------|
| C1 | Scalar | `SELECT` | Live comment count per post, to cross-check the denormalized counter |
| C2 | Correlated | `WHERE EXISTS` | Users who have downvoted anything in the past 7 days |
| C3 | `IN` | `WHERE` | Posts belonging to NSFW groups |
| C4 | `NOT IN` | `WHERE` | Users who have never filed a report (trust upgrade candidates) |
| C5 | Derived | `FROM` | Average karma of the top decile vs. the platform average |
| C6 | `EXISTS` | `WHERE` | Groups with at least one pending report (moderator alert) |

---

## Stored Objects Referenced (installed by `04_advanced.sql`)

| Kind | Signature | Purpose |
|------|-----------|---------|
| function | `get_post_hot_score(UUID) → DOUBLE PRECISION` | Front-page hot-score formula |
| function | `get_user_karma(UUID) → INTEGER` | Live karma (audit of denormalized column) |
| function | `cast_vote(user, target, type, value)` | Atomic vote insert/update/delete with row lock |
| function | `register_user(username, email, pw, initial_group?)` | All-or-nothing signup with uniqueness guard |
| function | `demote_moderator(group, user)` | Advisory-lock-protected demotion; preserves "≥1 moderator" rule |
| procedure | `archive_old_content(days)` | Nightly maintenance — freeze stale low-engagement posts |
| procedure | `resolve_report(report, mod, status)` | Validates moderator, updates report, archives content on `approved` |
| procedure | `promote_moderator(group, user, perms, by)` | Authorization-checked promotion |

## Trigger Inventory (installed by `04_advanced.sql`)

| Trigger | Timing / event | Affects | Role |
|---------|----------------|---------|------|
| `trg_vote_update_score` | AFTER INSERT/UPDATE/DELETE on `vote` | `post.score`, `comment.score` | Denormalized score maintenance |
| `trg_comment_increment_count` | AFTER INSERT/DELETE on `comment` | `post.comment_count` | Denormalized counter |
| `trg_follow_member_count` | AFTER INSERT/DELETE on `follow` | `group.member_count` | Denormalized counter |
| `trg_prevent_archived_comment` | BEFORE INSERT on `comment` | — | Enforces "no comments on archived posts" |
| `trg_notify_on_comment` | AFTER INSERT on `comment` | `notification` | Generates `post_comment` / `comment_reply` rows |
| `trg_post_self_upvote` | AFTER INSERT on `post` | `vote` | Author auto-upvote (Reddit convention) |
| `trg_tag_same_group` | BEFORE INSERT on `post_tag` | — | Tag must belong to the post's subreddit |

## Index Inventory

| Index | Kind | Rationale |
|-------|------|-----------|
| `idx_post_subreddit_created` | B-Tree composite | Group feed: `WHERE subreddit_id = ? ORDER BY created_at DESC` |
| `idx_comment_post_id` | B-Tree | "All comments for this post" page load |
| `idx_comment_parent` | B-Tree partial | Thread traversal; skips top-level rows |
| `idx_vote_user_target` | B-Tree composite | One-vote-per-target constraint check |
| `idx_vote_target` | B-Tree | Score-rebuild aggregations |
| `idx_report_status_pending` | B-Tree **partial** | Moderator queue (resolved rows never queried) |
| `idx_notification_user_unread` | B-Tree **partial** | Notification bell (unread hot set only) |
| `idx_post_author_created`, `idx_comment_author_created` | B-Tree composite | Author timeline |
| `idx_follow_subreddit` | B-Tree | Reverse lookup: followers of a group |
| `idx_tag_subreddit` | B-Tree | Tag browser per group |
| `idx_report_target` | B-Tree composite | Find reports for a specific target |
| `idx_post_title_trgm` | **GIN / pg_trgm** | Substring search `ILIKE '%kw%'` on titles |
| `idx_comment_body_trgm` | **GIN / pg_trgm** | Substring search on comment bodies |
| `idx_notification_created_brin` | **BRIN** | Append-only time-series, ~1% size of equivalent B-Tree |
| `idx_vote_id_hash` | **HASH** | Exact-match lookups by vote id |
| `idx_user_karma_cover` | B-Tree with `INCLUDE` | Leaderboard via index-only scan |

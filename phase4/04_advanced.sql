-- =============================================================================
-- Phase 4 | 04_advanced.sql
-- PL/pgSQL functions, procedures, triggers, advanced indexes, and examples of
-- transaction management + concurrency control.
-- -----------------------------------------------------------------------------
-- Run AFTER 02_dml.sql. Installing triggers before the bulk COPY would slow
-- the load by orders of magnitude and may trip business-rule checks against
-- half-loaded data.
-- =============================================================================

\echo '=== Installing advanced objects ==='

-- =============================================================================
-- SECTION I. ADVANCED INDEXING STRATEGIES
-- -----------------------------------------------------------------------------
-- The "standard" B-Tree indexes live in 01_ddl.sql. Below are the specialised
-- access paths that benefit specific query shapes.
-- =============================================================================

-- GIN trigram index for substring search on post titles and comment bodies.
-- Needed so moderator search (ILIKE '%keyword%') does not do a seq scan.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

DROP INDEX IF EXISTS idx_post_title_trgm;
CREATE INDEX idx_post_title_trgm
    ON post USING GIN (title gin_trgm_ops);

DROP INDEX IF EXISTS idx_comment_body_trgm;
CREATE INDEX idx_comment_body_trgm
    ON comment USING GIN (body gin_trgm_ops);

-- BRIN index for append-only time-series style access.
-- Notifications are written in roughly monotonic order, so BRIN is ~1% of the
-- size of a B-Tree while still pruning time-range scans effectively.
DROP INDEX IF EXISTS idx_notification_created_brin;
CREATE INDEX idx_notification_created_brin
    ON notification USING BRIN (created_at);

-- Hash index for exact-match lookups on vote.id (no range queries needed).
-- PostgreSQL 10+ hash indexes are WAL-logged and safe to use.
DROP INDEX IF EXISTS idx_vote_id_hash;
CREATE INDEX idx_vote_id_hash
    ON vote USING HASH (id);

-- Covering/INCLUDE index: lets "index-only scans" satisfy the top-karma query
-- without touching the heap.
DROP INDEX IF EXISTS idx_user_karma_cover;
CREATE INDEX idx_user_karma_cover
    ON "user" (karma DESC) INCLUDE (username);


-- =============================================================================
-- SECTION II. STORED FUNCTIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- get_post_hot_score(post_id) -> FLOAT
-- Reddit-style "hot" ranking. Combines signed log(score) and age decay.
-- Used by front-page feed query so application code doesn't re-implement it.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_post_hot_score(p_id UUID)
RETURNS DOUBLE PRECISION
LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_score INTEGER;
    v_age_h DOUBLE PRECISION;
    v_sign  INTEGER;
BEGIN
    SELECT  score,
            EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600.0
      INTO  v_score, v_age_h
      FROM  post
     WHERE  id = p_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'post % not found', p_id;
    END IF;

    v_sign := CASE WHEN v_score > 0 THEN  1
                   WHEN v_score < 0 THEN -1
                   ELSE 0
              END;

    RETURN v_sign * LOG(GREATEST(ABS(v_score), 1))
         + (v_age_h / POWER(v_age_h + 2, 1.8));
END;
$$;

-- -----------------------------------------------------------------------------
-- get_user_karma(user_id) -> INTEGER
-- Live karma = sum of all scores on the user's posts + comments.
-- Useful as a cross-check against the denormalised "user".karma column.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_user_karma(u_id UUID)
RETURNS INTEGER
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(SUM(score), 0)::INTEGER
      FROM (
            SELECT score FROM post    WHERE author_id = u_id
            UNION ALL
            SELECT score FROM comment WHERE author_id = u_id
           ) AS scores;
$$;

-- -----------------------------------------------------------------------------
-- cast_vote(user_id, target_id, target_type, value) -> vote.id
-- Single atomic entry point for voting. Implements the "one vote per target,
-- re-voting updates, zeroing deletes" rule described in Phase 3 business logic.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION cast_vote(p_user UUID,
                                     p_target UUID,
                                     p_target_type vote_target,
                                     p_value SMALLINT)
RETURNS UUID
LANGUAGE plpgsql AS $$
DECLARE
    v_existing_id    UUID;
    v_existing_value SMALLINT;
    v_new_id         UUID;
BEGIN
    IF p_value NOT IN (-1, 0, 1) THEN
        RAISE EXCEPTION 'vote value must be -1, 0, or 1 (got %)', p_value;
    END IF;

    SELECT id, value INTO v_existing_id, v_existing_value
      FROM vote
     WHERE user_id = p_user
       AND target_id = p_target
       AND target_type = p_target_type
     FOR UPDATE;                                  -- lock row for the txn

    IF v_existing_id IS NULL THEN
        IF p_value = 0 THEN
            RETURN NULL;                          -- nothing to rescind
        END IF;
        INSERT INTO vote (user_id, target_id, target_type, value)
             VALUES (p_user, p_target, p_target_type, p_value)
          RETURNING id INTO v_new_id;
        RETURN v_new_id;
    ELSIF p_value = 0 THEN
        DELETE FROM vote WHERE id = v_existing_id;
        RETURN v_existing_id;
    ELSIF p_value <> v_existing_value THEN
        UPDATE vote SET value = p_value WHERE id = v_existing_id;
        RETURN v_existing_id;
    ELSE
        RETURN v_existing_id;                     -- no-op: same vote again
    END IF;
END;
$$;


-- =============================================================================
-- SECTION III. STORED PROCEDURES (CALL ...)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- archive_old_content(cutoff_days)
-- Scheduled maintenance: freeze stale low-engagement posts and their comments.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE archive_old_content(p_days INTEGER)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE post
       SET is_archived = TRUE
     WHERE created_at < NOW() - (p_days || ' days')::INTERVAL
       AND comment_count < 5
       AND is_archived = FALSE;

    UPDATE comment
       SET is_archived = TRUE
     WHERE post_id IN (SELECT id FROM post WHERE is_archived = TRUE)
       AND is_archived = FALSE;
END;
$$;

-- -----------------------------------------------------------------------------
-- resolve_report(report_id, mod_id, new_status)
-- Validates that the resolver moderates the target's group, updates the report,
-- and archives the offending content when status = 'approved'. One transaction.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE resolve_report(p_report UUID,
                                           p_mod    UUID,
                                           p_status report_status)
LANGUAGE plpgsql AS $$
DECLARE
    v_target UUID;
    v_type   report_target;
    v_group  UUID;
BEGIN
    IF p_status = 'pending' THEN
        RAISE EXCEPTION 'cannot resolve a report back to pending';
    END IF;

    SELECT target_id, target_type
      INTO v_target, v_type
      FROM report
     WHERE id = p_report
       FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'report % not found', p_report;
    END IF;

    -- Derive the group that owns the target (post directly, comment via post)
    IF v_type = 'post' THEN
        SELECT subreddit_id INTO v_group FROM post WHERE id = v_target;
    ELSE
        SELECT p.subreddit_id
          INTO v_group
          FROM comment c
          JOIN post p ON p.id = c.post_id
         WHERE c.id = v_target;
    END IF;

    IF v_group IS NULL THEN
        RAISE EXCEPTION 'target content no longer exists';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM moderator
                    WHERE user_id = p_mod
                      AND subreddit_id = v_group) THEN
        RAISE EXCEPTION 'user % is not a moderator of group %', p_mod, v_group;
    END IF;

    UPDATE report
       SET status      = p_status,
           resolved_by = p_mod,
           resolved_at = NOW()
     WHERE id = p_report;

    IF p_status = 'approved' THEN
        IF v_type = 'post' THEN
            UPDATE post    SET is_archived = TRUE WHERE id = v_target;
        ELSE
            UPDATE comment SET is_archived = TRUE WHERE id = v_target;
        END IF;
    END IF;
END;
$$;

-- -----------------------------------------------------------------------------
-- promote_moderator(group_id, user_id, permissions, added_by)
-- Adds a moderator, enforcing that the promoter is already a moderator of the
-- group (or the group creator). Guards the "at least one moderator" invariant.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE promote_moderator(p_group UUID,
                                              p_user  UUID,
                                              p_perms TEXT DEFAULT 'posts,comments',
                                              p_by    UUID DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE
    v_creator UUID;
BEGIN
    SELECT created_by INTO v_creator FROM "group" WHERE id = p_group;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'group % not found', p_group;
    END IF;

    IF p_by IS NOT NULL
       AND p_by <> v_creator
       AND NOT EXISTS (SELECT 1 FROM moderator
                        WHERE subreddit_id = p_group AND user_id = p_by) THEN
        RAISE EXCEPTION 'promoter % is not authorized for group %', p_by, p_group;
    END IF;

    INSERT INTO moderator (user_id, subreddit_id, permissions)
    VALUES (p_user, p_group, p_perms)
    ON CONFLICT (user_id, subreddit_id) DO UPDATE
       SET permissions = EXCLUDED.permissions;
END;
$$;


-- =============================================================================
-- SECTION IV. TRIGGERS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- trg_vote_update_score
-- Keeps post.score / comment.score consistent with vote inserts/updates/deletes.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_vote_update_score()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_delta INTEGER;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_delta := NEW.value;
        IF NEW.target_type = 'post' THEN
            UPDATE post    SET score = score + v_delta WHERE id = NEW.target_id;
        ELSE
            UPDATE comment SET score = score + v_delta WHERE id = NEW.target_id;
        END IF;
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        -- Only the value should ever change in practice, but guard regardless.
        v_delta := NEW.value - OLD.value;
        IF NEW.target_type = 'post' THEN
            UPDATE post    SET score = score + v_delta WHERE id = NEW.target_id;
        ELSE
            UPDATE comment SET score = score + v_delta WHERE id = NEW.target_id;
        END IF;
        RETURN NEW;

    ELSE   -- DELETE
        v_delta := -OLD.value;
        IF OLD.target_type = 'post' THEN
            UPDATE post    SET score = score + v_delta WHERE id = OLD.target_id;
        ELSE
            UPDATE comment SET score = score + v_delta WHERE id = OLD.target_id;
        END IF;
        RETURN OLD;
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_vote_update_score ON vote;
CREATE TRIGGER trg_vote_update_score
AFTER INSERT OR UPDATE OR DELETE ON vote
FOR EACH ROW EXECUTE FUNCTION fn_vote_update_score();


-- -----------------------------------------------------------------------------
-- trg_comment_increment_count
-- post.comment_count ± 1 on comment insert/delete.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_comment_increment_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE post SET comment_count = comment_count + 1 WHERE id = NEW.post_id;
        RETURN NEW;
    ELSE
        UPDATE post SET comment_count = GREATEST(comment_count - 1, 0)
         WHERE id = OLD.post_id;
        RETURN OLD;
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_comment_increment_count ON comment;
CREATE TRIGGER trg_comment_increment_count
AFTER INSERT OR DELETE ON comment
FOR EACH ROW EXECUTE FUNCTION fn_comment_increment_count();


-- -----------------------------------------------------------------------------
-- trg_follow_member_count
-- group.member_count ± 1 on follow insert/delete.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_follow_member_count()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE "group" SET member_count = member_count + 1
         WHERE id = NEW.subreddit_id;
        RETURN NEW;
    ELSE
        UPDATE "group" SET member_count = GREATEST(member_count - 1, 0)
         WHERE id = OLD.subreddit_id;
        RETURN OLD;
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_follow_member_count ON follow;
CREATE TRIGGER trg_follow_member_count
AFTER INSERT OR DELETE ON follow
FOR EACH ROW EXECUTE FUNCTION fn_follow_member_count();


-- -----------------------------------------------------------------------------
-- trg_prevent_archived_comment
-- BEFORE INSERT guard: archived posts do not accept new comments.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_prevent_archived_comment()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_archived BOOLEAN;
BEGIN
    SELECT is_archived INTO v_archived FROM post WHERE id = NEW.post_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'post % does not exist', NEW.post_id;
    END IF;
    IF v_archived THEN
        RAISE EXCEPTION 'post % is archived and does not accept new comments', NEW.post_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_archived_comment ON comment;
CREATE TRIGGER trg_prevent_archived_comment
BEFORE INSERT ON comment
FOR EACH ROW EXECUTE FUNCTION fn_prevent_archived_comment();


-- -----------------------------------------------------------------------------
-- trg_notify_on_comment
-- On comment insert: notify the post author (post_comment) or the parent
-- comment's author (comment_reply). Skip self-notifications.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_notify_on_comment()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_recipient UUID;
    v_type      notification_type;
    v_target    notification_target := 'comment';
BEGIN
    IF NEW.parent_comment_id IS NULL THEN
        SELECT author_id INTO v_recipient FROM post WHERE id = NEW.post_id;
        v_type := 'post_comment';
    ELSE
        SELECT author_id INTO v_recipient FROM comment WHERE id = NEW.parent_comment_id;
        v_type := 'comment_reply';
    END IF;

    IF v_recipient IS NULL OR v_recipient = NEW.author_id THEN
        RETURN NEW;
    END IF;

    INSERT INTO notification (user_id, actor_id, target_id, type, target_type)
    VALUES (v_recipient, NEW.author_id, NEW.id, v_type, v_target);

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_notify_on_comment ON comment;
CREATE TRIGGER trg_notify_on_comment
AFTER INSERT ON comment
FOR EACH ROW EXECUTE FUNCTION fn_notify_on_comment();


-- -----------------------------------------------------------------------------
-- trg_post_self_upvote
-- Reddit convention: a new post starts with an auto-upvote from its author.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_post_self_upvote()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO vote (user_id, target_id, target_type, value)
    VALUES (NEW.author_id, NEW.id, 'post', 1)
    ON CONFLICT (user_id, target_id, target_type) DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_post_self_upvote ON post;
CREATE TRIGGER trg_post_self_upvote
AFTER INSERT ON post
FOR EACH ROW EXECUTE FUNCTION fn_post_self_upvote();


-- -----------------------------------------------------------------------------
-- trg_tag_same_group
-- BEFORE INSERT on post_tag: tag must belong to the same subreddit as the post.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_tag_same_group()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    v_post_group UUID;
    v_tag_group  UUID;
BEGIN
    SELECT subreddit_id INTO v_post_group FROM post WHERE id = NEW.post_id;
    SELECT subreddit_id INTO v_tag_group  FROM tag  WHERE id = NEW.tag_id;
    IF v_post_group IS DISTINCT FROM v_tag_group THEN
        RAISE EXCEPTION
            'tag % does not belong to the same group as post %', NEW.tag_id, NEW.post_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_tag_same_group ON post_tag;
CREATE TRIGGER trg_tag_same_group
BEFORE INSERT ON post_tag
FOR EACH ROW EXECUTE FUNCTION fn_tag_same_group();


-- =============================================================================
-- SECTION V. TRANSACTION MANAGEMENT
-- -----------------------------------------------------------------------------
-- End-to-end demonstration: a user registration that must be all-or-nothing.
-- SAVEPOINTs let us retry a failed insert (e.g. duplicate username) without
-- abandoning the whole transaction.
-- =============================================================================

CREATE OR REPLACE FUNCTION register_user(p_username VARCHAR,
                                         p_email    VARCHAR,
                                         p_password TEXT,
                                         p_initial_group UUID DEFAULT NULL)
RETURNS UUID
LANGUAGE plpgsql AS $$
DECLARE
    v_user_id UUID;
BEGIN
    INSERT INTO "user" (username, email, password_hash)
         VALUES (p_username, p_email, p_password)
      RETURNING id INTO v_user_id;

    IF p_initial_group IS NOT NULL THEN
        INSERT INTO follow (user_id, subreddit_id)
             VALUES (v_user_id, p_initial_group)
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN v_user_id;
EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'username or email already taken';
END;
$$;


-- =============================================================================
-- SECTION VI. CONCURRENCY CONTROL EXAMPLE
-- -----------------------------------------------------------------------------
-- Two patterns, both illustrated below:
--
--   1. SELECT ... FOR UPDATE  - row-level pessimistic lock. cast_vote() above
--      already uses it: the existing vote row (if any) is locked for the
--      duration of the transaction, preventing two concurrent vote updates
--      from racing and clobbering each other.
--
--   2. pg_advisory_xact_lock() - application-defined mutex. Useful when the
--      resource being protected does not map cleanly to a single row. The
--      function below serialises "promote to moderator" attempts per group
--      so two admins can not race past the "at least one moderator" rule.
-- =============================================================================

CREATE OR REPLACE FUNCTION demote_moderator(p_group UUID, p_user UUID)
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_remaining INTEGER;
BEGIN
    -- Advisory lock keyed on the group UUID: any other session trying to
    -- add/remove moderators for the same group will block here.
    PERFORM pg_advisory_xact_lock(hashtextextended(p_group::TEXT, 0));

    DELETE FROM moderator
     WHERE subreddit_id = p_group AND user_id = p_user;

    SELECT COUNT(*) INTO v_remaining
      FROM moderator
     WHERE subreddit_id = p_group;

    IF v_remaining = 0 THEN
        RAISE EXCEPTION 'cannot demote last moderator of group %', p_group;
        -- RAISE rolls back the DELETE since we are inside a txn.
    END IF;
END;
$$;


-- =============================================================================
-- SECTION VII. PERFORMANCE ANALYSIS HELPERS (EXPLAIN ANALYZE examples)
-- -----------------------------------------------------------------------------
-- These are reference queries, not executed at install time. Copy-paste into a
-- psql session to compare plans before/after an index is dropped. Each one
-- exercises one of the indexes installed in 01_ddl.sql or above.
-- =============================================================================

/*  ---- Before/after examples (run manually, wrap in EXPLAIN ANALYZE) ----

-- idx_post_subreddit_created: group-feed query
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM post
 WHERE subreddit_id = (SELECT id FROM "group" LIMIT 1)
 ORDER BY created_at DESC LIMIT 20;

-- idx_vote_user_target: one-vote-per-target constraint check
EXPLAIN (ANALYZE, BUFFERS)
SELECT 1 FROM vote
 WHERE user_id = (SELECT id FROM "user"  LIMIT 1)
   AND target_id = (SELECT id FROM post LIMIT 1)
   AND target_type = 'post';

-- idx_report_status_pending (partial): moderator queue
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM report
 WHERE status = 'pending'
 ORDER BY created_at DESC LIMIT 50;

-- idx_notification_user_unread (partial): notification bell
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM notification
 WHERE user_id = (SELECT id FROM "user" LIMIT 1)
   AND is_read = FALSE
 ORDER BY created_at DESC LIMIT 20;

-- idx_post_title_trgm (GIN): substring search
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, title FROM post
 WHERE title ILIKE '%python%' LIMIT 20;
*/

\echo '=== 04_advanced.sql installed. Triggers, functions, procedures active. ==='

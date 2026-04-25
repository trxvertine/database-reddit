-- =============================================================================
-- Phase 4 | 01_ddl.sql
-- Reddit-style database schema (PostgreSQL 14+)
-- Authors: Marc Petrosyan, Vahan Zakaryan, Sergey Hayriyan
-- -----------------------------------------------------------------------------
-- Creates all enum types, tables, constraints, and indexes described in the
-- Phase 3 specification. Safe to re-run: the script drops the schema first.
-- =============================================================================

-- pgcrypto gives us gen_random_uuid() without pulling in uuid-ossp
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------------------------------------
-- Clean slate (for idempotent re-runs during development)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS notification CASCADE;
DROP TABLE IF EXISTS report        CASCADE;
DROP TABLE IF EXISTS post_tag      CASCADE;
DROP TABLE IF EXISTS tag           CASCADE;
DROP TABLE IF EXISTS vote          CASCADE;
DROP TABLE IF EXISTS comment       CASCADE;
DROP TABLE IF EXISTS post          CASCADE;
DROP TABLE IF EXISTS moderator     CASCADE;
DROP TABLE IF EXISTS follow        CASCADE;
DROP TABLE IF EXISTS "group"       CASCADE;
DROP TABLE IF EXISTS "user"        CASCADE;

DROP TYPE IF EXISTS notification_target CASCADE;
DROP TYPE IF EXISTS notification_type   CASCADE;
DROP TYPE IF EXISTS report_status       CASCADE;
DROP TYPE IF EXISTS report_target       CASCADE;
DROP TYPE IF EXISTS vote_target         CASCADE;
DROP TYPE IF EXISTS post_type           CASCADE;

-- -----------------------------------------------------------------------------
-- Enum types
-- -----------------------------------------------------------------------------
CREATE TYPE post_type           AS ENUM ('text', 'link', 'image');
CREATE TYPE vote_target         AS ENUM ('post', 'comment');
CREATE TYPE report_target       AS ENUM ('post', 'comment');
CREATE TYPE report_status       AS ENUM ('pending', 'approved', 'dismissed');
CREATE TYPE notification_type   AS ENUM ('comment_reply', 'post_comment',
                                         'mention', 'award_received', 'mod_action');
CREATE TYPE notification_target AS ENUM ('post', 'comment', 'user');

-- =============================================================================
-- Core tables
-- =============================================================================

-- user ------------------------------------------------------------------------
CREATE TABLE "user" (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    username      VARCHAR(50) NOT NULL UNIQUE,
    email         VARCHAR(254) NOT NULL UNIQUE,
    password_hash TEXT        NOT NULL,
    avatar_url    TEXT,
    karma         INTEGER     NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT user_username_len CHECK (char_length(username) BETWEEN 3 AND 50),
    CONSTRAINT user_email_format CHECK (email ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$')
);

-- group -----------------------------------------------------------------------
CREATE TABLE "group" (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    created_by    UUID        NOT NULL REFERENCES "user"(id) ON DELETE RESTRICT,
    name          VARCHAR(50) NOT NULL UNIQUE,
    description   TEXT,
    banner_url    TEXT,
    member_count  INTEGER     NOT NULL DEFAULT 0,
    is_nsfw       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT group_name_len     CHECK (char_length(name) BETWEEN 2 AND 50),
    CONSTRAINT group_member_count CHECK (member_count >= 0)
);

-- follow (user <-> group subscription) ----------------------------------------
CREATE TABLE follow (
    user_id       UUID        NOT NULL REFERENCES "user"(id)  ON DELETE CASCADE,
    subreddit_id  UUID        NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    subscribed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, subreddit_id)
);

-- moderator (user <-> group with permission string) ---------------------------
CREATE TABLE moderator (
    user_id       UUID        NOT NULL REFERENCES "user"(id)  ON DELETE CASCADE,
    subreddit_id  UUID        NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    permissions   TEXT        NOT NULL DEFAULT 'all',
    added_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, subreddit_id)
);

-- post ------------------------------------------------------------------------
CREATE TABLE post (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    author_id      UUID         NOT NULL REFERENCES "user"(id)  ON DELETE CASCADE,
    subreddit_id   UUID         NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    type           post_type    NOT NULL,
    title          VARCHAR(300) NOT NULL,
    body           TEXT,
    url            TEXT,
    score          INTEGER      NOT NULL DEFAULT 0,
    comment_count  INTEGER      NOT NULL DEFAULT 0,
    is_archived    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT post_title_nonempty  CHECK (char_length(title) > 0),
    CONSTRAINT post_comment_count   CHECK (comment_count >= 0)
);

-- comment ---------------------------------------------------------------------
CREATE TABLE comment (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    post_id           UUID        NOT NULL REFERENCES post(id)   ON DELETE CASCADE,
    author_id         UUID        NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    parent_comment_id UUID        REFERENCES comment(id)         ON DELETE CASCADE,
    body              TEXT        NOT NULL,
    score             INTEGER     NOT NULL DEFAULT 0,
    is_archived       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT comment_body_nonempty CHECK (char_length(body) > 0),
    CONSTRAINT comment_not_self      CHECK (parent_comment_id IS NULL OR parent_comment_id <> id)
);

-- vote (polymorphic: post OR comment) -----------------------------------------
CREATE TABLE vote (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID        NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    target_id   UUID        NOT NULL,
    target_type vote_target NOT NULL,
    value       SMALLINT    NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT vote_value_range CHECK (value IN (-1, 1)),
    CONSTRAINT vote_one_per_target UNIQUE (user_id, target_id, target_type)
);

-- tag (per-group tag definitions) ---------------------------------------------
CREATE TABLE tag (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    subreddit_id UUID        NOT NULL REFERENCES "group"(id) ON DELETE CASCADE,
    text         VARCHAR(50) NOT NULL,
    color        CHAR(7)     NOT NULL,
    CONSTRAINT tag_color_hex CHECK (color ~ '^#[0-9A-Fa-f]{6}$')
    -- Note: we deliberately do NOT enforce UNIQUE(subreddit_id, text). Different
    -- tag rows may share display text inside the same group; what identifies
    -- a tag is its UUID, and post_tag references it by that UUID.
);

-- post_tag (M:N between post and tag) -----------------------------------------
CREATE TABLE post_tag (
    post_id     UUID        NOT NULL REFERENCES post(id) ON DELETE CASCADE,
    tag_id      UUID        NOT NULL REFERENCES tag(id)  ON DELETE CASCADE,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (post_id, tag_id)
);

-- report (polymorphic: post OR comment) ---------------------------------------
CREATE TABLE report (
    id           UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    reporter_id  UUID          NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    resolved_by  UUID          REFERENCES "user"(id)          ON DELETE SET NULL,
    target_id    UUID          NOT NULL,
    target_type  report_target NOT NULL,
    reason       VARCHAR(200)  NOT NULL,
    status       report_status NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ,
    CONSTRAINT report_resolution_consistency
        CHECK ((status = 'pending' AND resolved_by IS NULL AND resolved_at IS NULL)
            OR (status <> 'pending' AND resolved_by IS NOT NULL AND resolved_at IS NOT NULL))
);

-- notification (polymorphic: post/comment/user) -------------------------------
CREATE TABLE notification (
    id           UUID                PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      UUID                NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
    actor_id     UUID                REFERENCES "user"(id)          ON DELETE SET NULL,
    target_id    UUID                NOT NULL,
    type         notification_type   NOT NULL,
    target_type  notification_target NOT NULL,
    is_read      BOOLEAN             NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    CONSTRAINT notif_not_self CHECK (actor_id IS NULL OR actor_id <> user_id)
);

-- =============================================================================
-- Indexes (performance-critical paths from Phase 3 analysis)
-- =============================================================================

-- Feed query: "recent posts in a group", ordered newest-first
CREATE INDEX idx_post_subreddit_created
    ON post (subreddit_id, created_at DESC);

-- Comment-fetch query: "all comments for this post"
CREATE INDEX idx_comment_post_id
    ON comment (post_id);

-- Threaded rendering: walking up to a parent comment
CREATE INDEX idx_comment_parent
    ON comment (parent_comment_id) WHERE parent_comment_id IS NOT NULL;

-- Vote score updates and constraint checks
CREATE INDEX idx_vote_user_target
    ON vote (user_id, target_id, target_type);

-- Score aggregation when rebuilding denormalized counters
CREATE INDEX idx_vote_target
    ON vote (target_type, target_id);

-- Moderator dashboard: only pending reports are hot
CREATE INDEX idx_report_status_pending
    ON report (created_at DESC) WHERE status = 'pending';

-- Notification bell: "my unread notifications, newest first"
CREATE INDEX idx_notification_user_unread
    ON notification (user_id, created_at DESC) WHERE is_read = FALSE;

-- Author timeline: "posts/comments by user X"
CREATE INDEX idx_post_author_created    ON post    (author_id, created_at DESC);
CREATE INDEX idx_comment_author_created ON comment (author_id, created_at DESC);

-- Follow lookups in both directions
CREATE INDEX idx_follow_subreddit ON follow (subreddit_id);

-- Tag browsing inside a group
CREATE INDEX idx_tag_subreddit ON tag (subreddit_id);

-- Report routing: find reports for a specific target
CREATE INDEX idx_report_target ON report (target_type, target_id);

-- =============================================================================
-- Done. Use 02_dml.sql to populate and 04_advanced.sql for triggers/functions.
-- =============================================================================

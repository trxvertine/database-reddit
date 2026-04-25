# Reflective Report — Phase 4
**Team:** Marc Petrosyan, Vahan Zakaryan, Sergey Hayriyan
**Project:** Reddit-style database (PostgreSQL)

Over four phases we took a Reddit-style social platform from a whiteboard
sketch to a working PostgreSQL database populated with ~86,000 rows of
synthetic data, a full set of analytical reports, and a layer of procedural
code enforcing the platform's business rules. This report reflects on the
techniques, tools, and technologies we used and what we took away from each.

## Tools and technologies

The core stack was intentionally small. **PostgreSQL 14+** served as the
database engine: its rich type system (native enums, arrays, timestamps with
time zone) meant most of our domain constraints could live in the schema
rather than in application code. We leaned on three extensions —
**pgcrypto** for `gen_random_uuid()`, **pg_trgm** for trigram-based substring
search, and the built-in **BRIN** and **HASH** index access methods — to
demonstrate that the right index type matters as much as the right column.
For data generation we chose **Python** with the **Faker** library, seeded
deterministically so the dataset would be reproducible across team members.
Our ERD was authored in **Mermaid**, which gave us a text-based source we
could diff in Git alongside the schema itself.

## What we learned about schema design

The most important design decision was the polymorphic discriminator pattern
used for `vote`, `report`, and `notification`. Each of those tables has a
`target_id UUID` and a `target_type` enum instead of separate foreign keys
per target kind. This kept the schema compact, but it also taught us its
real cost: PostgreSQL cannot enforce a true foreign key against a union of
target tables, so the integrity story moves into triggers and procedures.
We made peace with that trade-off because the alternatives (one table per
target kind, or a nullable FK per kind) would have bloated the schema and
forced ugly query fan-outs. Denormalized counters — `post.score`,
`post.comment_count`, `group.member_count` — were another important
learning: they are cheap reads and expensive writes. We ended up with a
clear rule of thumb: if a column is read on every page load and written a
few times per second, it belongs behind a trigger.

## What we learned about procedural programming

Writing the procedural layer in **PL/pgSQL** forced us to think carefully
about when logic belongs in the database versus the application. The
`resolve_report` procedure is a good example: it validates that the caller
moderates the targeted group, updates the report, and archives the content
— all as one atomic unit. Pushing that into the database made the
"only moderators can resolve reports" invariant impossible to violate,
even through a buggy application client. The `cast_vote` function taught
us about concurrency control in practice: without `SELECT ... FOR UPDATE`
on the existing vote row, two clients racing on the same target could each
see "no vote yet" and both insert, violating the one-vote-per-target unique
constraint. With the row lock, the second client blocks, re-reads, and does
the right thing. We also used **advisory locks** (`pg_advisory_xact_lock`)
in `demote_moderator` to illustrate when row-level locks are not enough —
protecting an invariant that spans rows ("at least one moderator per group").

## What we learned about indexing

Building indexes before we had data let us plan, but running `EXPLAIN
ANALYZE` on the populated database is what made the lessons concrete.
Two specialised indexes stood out. A **partial B-Tree** on
`report(status) WHERE status = 'pending'` is dramatically smaller than a
full index on `status` because the vast majority of rows are resolved and
therefore irrelevant to the moderator dashboard query. A **GIN index on
`title gin_trgm_ops`** turned a `LIKE '%python%'` from a sequential scan
into a sub-millisecond lookup — the kind of improvement that only an
access-method change (B-Tree → GIN), not a tweak to the query, can
deliver. The **BRIN index on `notification.created_at`** was our
reminder that not every index needs to be big: on an append-only,
roughly sorted column, a ~20 KB BRIN summary pruned time-range scans
almost as effectively as a 40 MB B-Tree.

## What we learned about workflow

Splitting the deliverable into four numbered scripts (`01_ddl`, `02_dml`,
`03_reports`, `04_advanced`) paid off every time a teammate broke
something. Because `01_ddl` drops and recreates the schema, any one of us
could reset the database from scratch in under a minute and re-run the
others. One non-obvious sequencing lesson: triggers must be installed
*after* the bulk COPY, not before — otherwise every row in the 22 000-row
`comment.csv` fires `trg_notify_on_comment` and `trg_comment_increment_count`,
turning a second-long load into a minutes-long one. Denormalized counters
are backfilled in `02_dml.sql` precisely so that the triggers in
`04_advanced.sql` start from consistent state.

## Closing

The single biggest takeaway is that a relational database is not just a
storage system — when pushed, it is a small, strict application server.
Schema constraints, triggers, procedures, and the right indexes together
enforce most of what the application would otherwise need to enforce
itself, with less code and stronger guarantees. Phase 4 was where those
four phases of design finally behaved like one system.

"""
Reddit-like DB — Phase 4 Data Generation Script
================================================
Generates referentially-correct CSV files for all 11 tables.
Load order matters; follow the LOAD ORDER comment at the bottom.

Target row counts (slightly irregular, as per Phase 3 doc):
  user         2 100
  group        1 050
  follow       6 300   (junction)
  moderator      820   (junction)
  post         7 500
  comment     22 000
  vote        26 500
  tag          1 500
  post_tag     4 200   (junction)
  report       1 250
  notification 13 000

Usage:
  python generate_data.py
  → writes one CSV per table into ./csv_output/

PostgreSQL load (run in order):
  psql -d yourdb -c "\COPY \"user\" FROM 'csv_output/user.csv' CSV HEADER"
  ... (see full COPY commands printed at end of this script)
"""

import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUT_DIR = "csv_output"
os.makedirs(OUT_DIR, exist_ok=True)

# ── helpers ──────────────────────────────────────────────────────────────────

SIM_START = datetime(2023, 1, 1, tzinfo=timezone.utc)
SIM_END   = datetime(2025, 1, 1, tzinfo=timezone.utc)
SIM_SPAN  = (SIM_END - SIM_START).total_seconds()

def rand_ts(after=None, before=None):
    """Random timestamptz between after and before (defaults: SIM_START..SIM_END)."""
    lo = after  or SIM_START
    hi = before or SIM_END
    delta = (hi - lo).total_seconds()
    if delta <= 0:
        return lo
    return lo + timedelta(seconds=random.uniform(0, delta))

def ts_str(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S+00")

def uid():
    return str(uuid.uuid4())

def write_csv(filename, fieldnames, rows):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  wrote {len(rows):>7,}  rows  →  {path}")

# ── realistic data pools ─────────────────────────────────────────────────────

COMMUNITY_TOPICS = [
    "technology", "programming", "python", "linux", "gaming", "science",
    "worldnews", "politics", "history", "philosophy", "mathematics",
    "datascience", "machinelearning", "cybersecurity", "electronics",
    "books", "movies", "music", "sports", "football", "basketball",
    "fitness", "nutrition", "cooking", "travel", "photography",
    "art", "design", "architecture", "economics", "finance",
    "investing", "cryptocurrency", "startups", "entrepreneurship",
    "environment", "climate", "space", "biology", "chemistry",
    "medicine", "psychology", "education", "languages", "writing",
    "journalism", "law", "engineering", "robotics", "opensource",
    "devops", "webdev", "mobiledev", "gamedev", "dataviz",
    "Armenia", "Yerevan", "Caucasus", "europe", "asia",
    "comedy", "anime", "cats", "dogs", "DIY", "gardening",
]

POST_TITLE_TEMPLATES = [
    "Why {topic} matters more than ever",
    "I spent 6 months studying {topic} — here's what I learned",
    "TIL something fascinating about {topic}",
    "Hot take: {topic} is overrated",
    "Best resources for learning {topic} in {year}?",
    "The state of {topic} in {year}",
    "Anyone else frustrated with {topic} lately?",
    "A beginner's question about {topic}",
    "Deep dive: how {topic} actually works",
    "Weekly discussion: {topic}",
    "Looking for recommendations on {topic}",
    "My experience with {topic} after 2 years",
    "Is {topic} worth it?",
    "{topic} — a comprehensive guide",
    "Unpopular opinion about {topic}",
    "What's your favourite aspect of {topic}?",
    "I built something with {topic}",
    "Explaining {topic} to a 5-year-old",
]

REPORT_REASONS = [
    "Spam or self-promotion",
    "Harassment or personal attack",
    "Misinformation",
    "Off-topic content",
    "Duplicate post",
    "Violates community rules",
    "Inappropriate content",
    "Soliciting votes or engagement",
    "Brigading",
    "Doxxing or privacy violation",
]

PERMISSIONS_OPTIONS = [
    "all",
    "posts,comments",
    "posts,comments,bans",
    "posts",
    "comments,reports",
    "reports,bans",
]

TAG_WORDS = [
    "discussion", "question", "news", "tutorial", "rant", "meta",
    "weekly", "beginner", "advanced", "project", "resource",
    "help", "showcase", "humor", "serious", "AMA", "review",
    "announcement", "guide", "opinion",
]

TAG_COLORS = ["#FF5733","#33FF57","#3357FF","#FF33A1","#FFD700",
              "#00CED1","#FF8C00","#9400D3","#2E8B57","#DC143C",
              "#1E90FF","#FF69B4","#8B4513","#20B2AA","#778899"]

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 1 — user
# ─────────────────────────────────────────────────────────────────────────────
print("\n[1/11] Generating users...")

N_USERS = 2_100
users = []
used_usernames = set()
used_emails = set()

for _ in range(N_USERS):
    while True:
        username = fake.user_name() + str(random.randint(1, 9999))
        if username not in used_usernames:
            used_usernames.add(username)
            break
    while True:
        email = fake.email()
        if email not in used_emails:
            used_emails.add(email)
            break

    # log-normal karma: most users have low karma, a few have very high
    karma = int(random.lognormvariate(3.5, 2.0))
    karma = min(karma, 500_000)

    users.append({
        "id":            uid(),
        "username":      username,
        "email":         email,
        "password_hash": fake.sha256(),
        "avatar_url":    f"https://avatars.example.com/{fake.uuid4()}.jpg",
        "karma":         karma,
        "created_at":    ts_str(rand_ts()),
    })

write_csv("user.csv",
    ["id","username","email","password_hash","avatar_url","karma","created_at"],
    users)

user_ids   = [u["id"] for u in users]
user_dates = {u["id"]: datetime.strptime(u["created_at"], "%Y-%m-%d %H:%M:%S+00")
              .replace(tzinfo=timezone.utc)
              for u in users}

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 2 — group  (reserved word — quote in SQL)
# ─────────────────────────────────────────────────────────────────────────────
print("[2/11] Generating groups...")

N_GROUPS = 1_050
groups = []
used_group_names = set()

for i in range(N_GROUPS):
    base = random.choice(COMMUNITY_TOPICS)
    suffix = "" if base not in used_group_names else str(random.randint(2, 99))
    name = base + suffix
    used_group_names.add(name)

    creator_id   = random.choice(user_ids)
    creator_date = user_dates[creator_id]
    created      = rand_ts(after=creator_date)

    groups.append({
        "id":           uid(),
        "created_by":   creator_id,
        "name":         name,
        "description":  fake.paragraph(nb_sentences=3),
        "banner_url":   f"https://banners.example.com/{fake.uuid4()}.jpg",
        "member_count": 0,   # will be updated by trigger; seed as 0
        "is_nsfw":      random.random() < 0.06,   # ~6% NSFW
        "created_at":   ts_str(created),
    })

write_csv("group.csv",
    ["id","created_by","name","description","banner_url",
     "member_count","is_nsfw","created_at"],
    groups)

group_ids   = [g["id"] for g in groups]
group_dates = {g["id"]: datetime.strptime(g["created_at"], "%Y-%m-%d %H:%M:%S+00")
               .replace(tzinfo=timezone.utc)
               for g in groups}

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 3 — follow
# ─────────────────────────────────────────────────────────────────────────────
print("[3/11] Generating follows...")

N_FOLLOWS = 6_300
follows = []
follow_pairs = set()

attempts = 0
while len(follows) < N_FOLLOWS and attempts < N_FOLLOWS * 10:
    attempts += 1
    u  = random.choice(user_ids)
    g  = random.choice(group_ids)
    if (u, g) in follow_pairs:
        continue
    follow_pairs.add((u, g))

    lo = max(user_dates[u], group_dates[g])
    follows.append({
        "user_id":       u,
        "subreddit_id":  g,
        "subscribed_at": ts_str(rand_ts(after=lo)),
    })

write_csv("follow.csv",
    ["user_id","subreddit_id","subscribed_at"],
    follows)

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 4 — moderator
# ─────────────────────────────────────────────────────────────────────────────
print("[4/11] Generating moderators...")

N_MODS = 820
moderators = []
mod_pairs  = set()

# ensure every group has at least 1 moderator
for g in groups:
    u  = g["created_by"]
    gid = g["id"]
    if (u, gid) not in mod_pairs:
        mod_pairs.add((u, gid))
        lo = group_dates[gid]
        moderators.append({
            "user_id":      u,
            "subreddit_id": gid,
            "permissions":  "all",
            "added_at":     ts_str(rand_ts(after=lo)),
        })

# fill up to N_MODS with random additional moderators
extra_attempts = 0
while len(moderators) < N_MODS and extra_attempts < N_MODS * 20:
    extra_attempts += 1
    u   = random.choice(user_ids)
    gid = random.choice(group_ids)
    if (u, gid) in mod_pairs:
        continue
    mod_pairs.add((u, gid))
    lo = max(user_dates[u], group_dates[gid])
    moderators.append({
        "user_id":      u,
        "subreddit_id": gid,
        "permissions":  random.choice(PERMISSIONS_OPTIONS),
        "added_at":     ts_str(rand_ts(after=lo)),
    })

write_csv("moderator.csv",
    ["user_id","subreddit_id","permissions","added_at"],
    moderators)

mod_group_users = {}   # group_id -> list of user_ids who are mods
for m in moderators:
    mod_group_users.setdefault(m["subreddit_id"], []).append(m["user_id"])

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 5 — post
# ─────────────────────────────────────────────────────────────────────────────
print("[5/11] Generating posts...")

N_POSTS   = 7_500
POST_TYPES = ["text", "link", "image"]
# weights: text 55%, link 25%, image 20%
POST_TYPE_W = [0.55, 0.25, 0.20]

posts = []

for _ in range(N_POSTS):
    author_id   = random.choice(user_ids)
    group_id    = random.choice(group_ids)
    author_date = user_dates[author_id]
    group_date  = group_dates[group_id]
    lo          = max(author_date, group_date)
    created     = rand_ts(after=lo)

    topic    = random.choice(COMMUNITY_TOPICS)
    year     = random.choice([2023, 2024])
    template = random.choice(POST_TITLE_TEMPLATES)
    title    = template.format(topic=topic, year=year)

    post_type = random.choices(POST_TYPES, weights=POST_TYPE_W, k=1)[0]
    body = fake.paragraph(nb_sentences=random.randint(2, 8)) if post_type == "text" else ""
    url  = fake.url() if post_type in ("link", "image") else ""

    posts.append({
        "id":            uid(),
        "author_id":     author_id,
        "subreddit_id":  group_id,
        "post_type":     post_type,
        "title":         title[:300],
        "body":          body,
        "url":           url,
        "score":         0,    # maintained by trigger
        "comment_count": 0,    # maintained by trigger
        "is_archived":   random.random() < 0.08,   # 8% archived
        "created_at":    ts_str(created),
    })

write_csv("post.csv",
    ["id","author_id","subreddit_id","post_type","title",
     "body","url","score","comment_count","is_archived","created_at"],
    posts)

post_ids     = [p["id"] for p in posts]
post_dates   = {p["id"]: datetime.strptime(p["created_at"],"%Y-%m-%d %H:%M:%S+00")
                .replace(tzinfo=timezone.utc) for p in posts}
post_authors = {p["id"]: p["author_id"] for p in posts}
post_archived = {p["id"]: p["is_archived"] for p in posts}

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 6 — comment
# ─────────────────────────────────────────────────────────────────────────────
print("[6/11] Generating comments (this takes a moment)...")

N_COMMENTS = 22_000

comments   = []
# track comments per post so we can build reply chains
post_comment_ids = {}   # post_id -> [comment_id, ...]

active_posts = [p for p in posts if not p["is_archived"]]

for i in range(N_COMMENTS):
    post      = random.choice(active_posts)
    post_id   = post["id"]
    author_id = random.choice(user_ids)
    post_date = post_dates[post_id]
    created   = rand_ts(after=post_date)

    # 30% chance of being a reply if the post already has comments
    parent_id = None
    existing  = post_comment_ids.get(post_id, [])
    if existing and random.random() < 0.30:
        parent_id = random.choice(existing)

    cid = uid()
    comments.append({
        "id":                cid,
        "post_id":           post_id,
        "author_id":         author_id,
        "parent_comment_id": parent_id if parent_id else "",
        "body":              fake.paragraph(nb_sentences=random.randint(1, 5)),
        "score":             0,
        "is_archived":       random.random() < 0.04,
        "created_at":        ts_str(created),
    })

    post_comment_ids.setdefault(post_id, []).append(cid)

write_csv("comment.csv",
    ["id","post_id","author_id","parent_comment_id",
     "body","score","is_archived","created_at"],
    comments)

comment_ids     = [c["id"] for c in comments]
comment_authors = {c["id"]: c["author_id"] for c in comments}
comment_dates   = {c["id"]: datetime.strptime(c["created_at"],"%Y-%m-%d %H:%M:%S+00")
                   .replace(tzinfo=timezone.utc) for c in comments}
comment_posts   = {c["id"]: c["post_id"] for c in comments}

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 7 — vote
# ─────────────────────────────────────────────────────────────────────────────
print("[7/11] Generating votes...")

N_VOTES = 26_500

votes      = []
vote_pairs = set()   # (user_id, target_id) — one vote per user per target

# 40% on posts, 60% on comments — as specified in enum distribution
n_post_votes    = int(N_VOTES * 0.40)
n_comment_votes = N_VOTES - n_post_votes

def gen_votes(targets, target_type, n):
    generated = []
    attempts  = 0
    while len(generated) < n and attempts < n * 8:
        attempts += 1
        user_id   = random.choice(user_ids)
        target_id = random.choice(targets)
        if (user_id, target_id) in vote_pairs:
            continue
        vote_pairs.add((user_id, target_id))

        # 90% upvote bias
        value = 1 if random.random() < 0.90 else -1

        if target_type == "post":
            lo = post_dates.get(target_id, SIM_START)
        else:
            lo = comment_dates.get(target_id, SIM_START)

        generated.append({
            "id":          uid(),
            "user_id":     user_id,
            "target_id":   target_id,
            "target_type": target_type,
            "value":       value,
            "created_at":  ts_str(rand_ts(after=lo)),
        })
    return generated

votes += gen_votes(post_ids,    "post",    n_post_votes)
votes += gen_votes(comment_ids, "comment", n_comment_votes)

write_csv("vote.csv",
    ["id","user_id","target_id","target_type","value","created_at"],
    votes)

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 8 — tag
# ─────────────────────────────────────────────────────────────────────────────
print("[8/11] Generating tags...")

N_TAGS = 1_500
tags   = []

# distribute tags across groups (~1.4 tags per group on average)
for _ in range(N_TAGS):
    group_id = random.choice(group_ids)
    tags.append({
        "id":           uid(),
        "subreddit_id": group_id,
        "text":         random.choice(TAG_WORDS),
        "color":        random.choice(TAG_COLORS),
    })

write_csv("tag.csv",
    ["id","subreddit_id","text","color"],
    tags)

# index tags by subreddit for fast lookup
tags_by_group = {}
for t in tags:
    tags_by_group.setdefault(t["subreddit_id"], []).append(t["id"])

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 9 — post_tag
# ─────────────────────────────────────────────────────────────────────────────
print("[9/11] Generating post_tags...")

N_POST_TAGS = 4_200
post_tags   = []
pt_pairs    = set()

# index posts by subreddit
posts_by_group = {}
for p in posts:
    posts_by_group.setdefault(p["subreddit_id"], []).append(p)

attempts = 0
while len(post_tags) < N_POST_TAGS and attempts < N_POST_TAGS * 10:
    attempts += 1
    group_id = random.choice(group_ids)
    group_posts = posts_by_group.get(group_id, [])
    group_tags  = tags_by_group.get(group_id, [])
    if not group_posts or not group_tags:
        continue

    post    = random.choice(group_posts)
    tag_id  = random.choice(group_tags)
    post_id = post["id"]

    if (post_id, tag_id) in pt_pairs:
        continue
    pt_pairs.add((post_id, tag_id))

    post_tags.append({
        "post_id":     post_id,
        "tag_id":      tag_id,
        "assigned_at": ts_str(post_dates[post_id]),
    })

write_csv("post_tag.csv",
    ["post_id","tag_id","assigned_at"],
    post_tags)

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 10 — report
# ─────────────────────────────────────────────────────────────────────────────
print("[10/11] Generating reports...")

N_REPORTS = 1_250

# report_status distribution: 60% pending, 25% dismissed, 15% approved
REPORT_STATUSES  = ["pending", "dismissed", "approved"]
REPORT_STATUS_W  = [0.60,      0.25,        0.15]

# report_target: post or comment
REPORT_TARGETS   = ["post", "comment"]
REPORT_TARGET_W  = [0.50, 0.50]

reports = []

for _ in range(N_REPORTS):
    reporter_id  = random.choice(user_ids)
    target_type  = random.choices(REPORT_TARGETS, weights=REPORT_TARGET_W, k=1)[0]

    if target_type == "post":
        target_id  = random.choice(post_ids)
        lo         = post_dates[target_id]
    else:
        target_id  = random.choice(comment_ids)
        lo         = comment_dates[target_id]

    # reporter must have seen content after it was created
    reporter_date = user_dates[reporter_id]
    lo            = max(lo, reporter_date)
    created       = rand_ts(after=lo)

    status = random.choices(REPORT_STATUSES, weights=REPORT_STATUS_W, k=1)[0]

    resolved_by = ""
    resolved_at = ""

    if status in ("dismissed", "approved"):
        # find a moderator who could have resolved this
        # for simplicity pick any user (mod validation is in procedure)
        resolved_by = random.choice(user_ids)
        resolved_at = ts_str(rand_ts(
            after=created,
            before=min(created + timedelta(days=14), SIM_END)
        ))

    reports.append({
        "id":          uid(),
        "reporter_id": reporter_id,
        "resolved_by": resolved_by,
        "target_id":   target_id,
        "target_type": target_type,
        "reason":      random.choice(REPORT_REASONS),
        "status":      status,
        "created_at":  ts_str(created),
        "resolved_at": resolved_at,
    })

write_csv("report.csv",
    ["id","reporter_id","resolved_by","target_id","target_type",
     "reason","status","created_at","resolved_at"],
    reports)

# ─────────────────────────────────────────────────────────────────────────────
# TABLE 11 — notification
# ─────────────────────────────────────────────────────────────────────────────
print("[11/11] Generating notifications...")

N_NOTIFS = 13_000

# notification_type distribution: 45/30/15/8/2
NOTIF_TYPES   = ["comment_reply","post_comment","mention","award_received","mod_action"]
NOTIF_TYPE_W  = [0.45, 0.30, 0.15, 0.08, 0.02]

# notification_target distribution: post 35%, comment 50%, user 15%
NOTIF_TARGETS  = ["post", "comment", "user"]
NOTIF_TARGET_W = [0.35, 0.50, 0.15]

# is_read: ~65% read
notifications = []

for _ in range(N_NOTIFS):
    notif_type   = random.choices(NOTIF_TYPES,   weights=NOTIF_TYPE_W,   k=1)[0]
    target_type  = random.choices(NOTIF_TARGETS,  weights=NOTIF_TARGET_W, k=1)[0]

    # pick recipient
    recipient_id = random.choice(user_ids)

    # pick actor — must be different from recipient
    actor_id = recipient_id
    while actor_id == recipient_id:
        actor_id = random.choice(user_ids)

    # pick a coherent target_id
    if target_type == "post":
        target_id = random.choice(post_ids)
        lo        = post_dates[target_id]
    elif target_type == "comment":
        target_id = random.choice(comment_ids)
        lo        = comment_dates[target_id]
    else:
        target_id = random.choice(user_ids)
        lo        = user_dates[target_id]

    lo      = max(lo, user_dates[recipient_id], user_dates[actor_id])
    created = rand_ts(after=lo)

    is_read = random.random() < 0.65

    notifications.append({
        "id":          uid(),
        "user_id":     recipient_id,
        "actor_id":    actor_id,
        "target_id":   target_id,
        "type":        notif_type,
        "target_type": target_type,
        "is_read":     is_read,
        "created_at":  ts_str(created),
    })

write_csv("notification.csv",
    ["id","user_id","actor_id","target_id","type",
     "target_type","is_read","created_at"],
    notifications)

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY + COPY COMMANDS
# ─────────────────────────────────────────────────────────────────────────────

total = (N_USERS + N_GROUPS + len(follows) + len(moderators) +
         N_POSTS + N_COMMENTS + len(votes) + N_TAGS +
         len(post_tags) + N_REPORTS + N_NOTIFS)

print(f"\n{'='*55}")
print(f"  Total rows generated: {total:,}")
print(f"{'='*55}")

print("""
PostgreSQL COPY commands (run in this order):
─────────────────────────────────────────────
\\COPY "user"         FROM 'csv_output/user.csv'         CSV HEADER NULL ''
\\COPY "group"        FROM 'csv_output/group.csv'        CSV HEADER NULL ''
\\COPY follow         FROM 'csv_output/follow.csv'       CSV HEADER NULL ''
\\COPY moderator      FROM 'csv_output/moderator.csv'    CSV HEADER NULL ''
\\COPY post           FROM 'csv_output/post.csv'         CSV HEADER NULL ''
\\COPY comment        FROM 'csv_output/comment.csv'      CSV HEADER NULL ''
\\COPY vote           FROM 'csv_output/vote.csv'         CSV HEADER NULL ''
\\COPY tag            FROM 'csv_output/tag.csv'          CSV HEADER NULL ''
\\COPY post_tag       FROM 'csv_output/post_tag.csv'     CSV HEADER NULL ''
\\COPY report         FROM 'csv_output/report.csv'       CSV HEADER NULL ''
\\COPY notification   FROM 'csv_output/notification.csv' CSV HEADER NULL ''

Notes:
  • NULL '' maps empty strings to SQL NULL (for optional FKs like
    parent_comment_id, resolved_by, resolved_at).
  • Load triggers should be DISABLED during seeding, then re-enabled.
    The score and comment_count columns are seeded as 0; run the
    recalculation queries below after loading to backfill them.
  • member_count on "group" is also 0 — update after loading follow.

Recalculation queries (run after all COPY commands):
─────────────────────────────────────────────────────
-- Post scores
UPDATE post p
SET score = (SELECT COALESCE(SUM(v.value), 0)
             FROM vote v
             WHERE v.target_id = p.id AND v.target_type = 'post');

-- Comment scores
UPDATE comment c
SET score = (SELECT COALESCE(SUM(v.value), 0)
             FROM vote v
             WHERE v.target_id = c.id AND v.target_type = 'comment');

-- Post comment counts
UPDATE post p
SET comment_count = (SELECT COUNT(*)
                     FROM comment c WHERE c.post_id = p.id);

-- Group member counts
UPDATE "group" g
SET member_count = (SELECT COUNT(*)
                    FROM follow f WHERE f.subreddit_id = g.id);
""")

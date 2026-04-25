# Phase 4 Data Gen Script
# Dumps out CSVs for the database. Make sure to follow the load order at the bottom!

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

# --- Helpers ---

SIM_START = datetime(2023, 1, 1, tzinfo=timezone.utc)
SIM_END = datetime(2025, 1, 1, tzinfo=timezone.utc)

def rand_ts(after=None, before=None):
    # Generates a random timestamp between two dates
    lo = after or SIM_START
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
    print(f"-> Wrote {len(rows):,} rows to {path}")

# --- Data Pools ---
# Grabbed a bunch of standard sub-reddit topics
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
    "Spam or self-promotion", "Harassment or personal attack", "Misinformation",
    "Off-topic content", "Duplicate post", "Violates community rules",
    "Inappropriate content", "Soliciting votes or engagement", "Brigading",
    "Doxxing or privacy violation"
]

TAG_WORDS = [
    "discussion", "question", "news", "tutorial", "rant", "meta",
    "weekly", "beginner", "advanced", "project", "resource",
    "help", "showcase", "humor", "serious", "AMA", "review",
    "announcement", "guide", "opinion",
]

TAG_COLORS = [
    "#FF5733", "#33FF57", "#3357FF", "#FF33A1", "#FFD700",
    "#00CED1", "#FF8C00", "#9400D3", "#2E8B57", "#DC143C",
    "#1E90FF", "#FF69B4", "#8B4513", "#20B2AA", "#778899"
]

print("Starting data generation...")

# --- Users ---
N_USERS = 2100
users = []
used_usernames = set()
used_emails = set()

for _ in range(N_USERS):
    # Brute forcing uniqueness since it's only 2k rows
    username = f"{fake.user_name()}{random.randint(1, 9999)}"
    while username in used_usernames:
        username = f"{fake.user_name()}{random.randint(1, 9999)}"
    used_usernames.add(username)

    email = fake.email()
    while email in used_emails:
        email = fake.email()
    used_emails.add(email)

    # Log-normal distribution so a few users have crazy high karma
    karma = min(int(random.lognormvariate(3.5, 2.0)), 500_000)

    users.append({
        "id": uid(),
        "username": username,
        "email": email,
        "password_hash": fake.sha256(),
        "avatar_url": f"https://avatars.example.com/{fake.uuid4()}.jpg",
        "karma": karma,
        "created_at": ts_str(rand_ts()),
    })

write_csv("user.csv", ["id", "username", "email", "password_hash", "avatar_url", "karma", "created_at"], users)

# caching dates for later constraints
user_ids = [u["id"] for u in users]
user_dates = {u["id"]: datetime.strptime(u["created_at"], "%Y-%m-%d %H:%M:%S+00").replace(tzinfo=timezone.utc) for u in users}

# --- Groups ---
N_GROUPS = 1050
groups = []
used_group_names = set()

for _ in range(N_GROUPS):
    base = random.choice(COMMUNITY_TOPICS)
    name = f"{base}{random.randint(2, 99)}" if base in used_group_names else base
    used_group_names.add(name)

    creator_id = random.choice(user_ids)
    created = rand_ts(after=user_dates[creator_id])

    groups.append({
        "id": uid(),
        "created_by": creator_id,
        "name": name,
        "description": fake.paragraph(nb_sentences=3),
        "banner_url": f"https://banners.example.com/{fake.uuid4()}.jpg",
        "member_count": 0, # Will get updated by triggers later
        "is_nsfw": random.random() < 0.06,
        "created_at": ts_str(created),
    })

write_csv("group.csv", ["id", "created_by", "name", "description", "banner_url", "member_count", "is_nsfw", "created_at"], groups)

group_ids = [g["id"] for g in groups]
group_dates = {g["id"]: datetime.strptime(g["created_at"], "%Y-%m-%d %H:%M:%S+00").replace(tzinfo=timezone.utc) for g in groups}

# --- Follows ---
N_FOLLOWS = 6300
follows = []
follow_pairs = set()

for _ in range(N_FOLLOWS * 10):
    if len(follows) >= N_FOLLOWS:
        break
        
    u, g = random.choice(user_ids), random.choice(group_ids)
    if (u, g) in follow_pairs:
        continue
        
    follow_pairs.add((u, g))
    lo = max(user_dates[u], group_dates[g])
    
    follows.append({
        "user_id": u,
        "subreddit_id": g,
        "subscribed_at": ts_str(rand_ts(after=lo)),
    })

write_csv("follow.csv", ["user_id", "subreddit_id", "subscribed_at"], follows)

# --- Moderators ---
N_MODS = 820
moderators = []
mod_pairs = set()

# Make sure every group gets at least the creator as a mod
for g in groups:
    u, gid = g["created_by"], g["id"]
    if (u, gid) not in mod_pairs:
        mod_pairs.add((u, gid))
        moderators.append({
            "user_id": u,
            "subreddit_id": gid,
            "permissions": "all",
            "added_at": ts_str(rand_ts(after=group_dates[gid])),
        })

# Pad the rest
permissions_opts = ["all", "posts,comments", "posts,comments,bans", "posts", "comments,reports", "reports,bans"]

for _ in range(N_MODS * 20):
    if len(moderators) >= N_MODS:
        break
        
    u, gid = random.choice(user_ids), random.choice(group_ids)
    if (u, gid) in mod_pairs:
        continue
        
    mod_pairs.add((u, gid))
    lo = max(user_dates[u], group_dates[gid])
    moderators.append({
        "user_id": u,
        "subreddit_id": gid,
        "permissions": random.choice(permissions_opts),
        "added_at": ts_str(rand_ts(after=lo)),
    })

write_csv("moderator.csv", ["user_id", "subreddit_id", "permissions", "added_at"], moderators)

# --- Posts ---
N_POSTS = 7500
posts = []

for _ in range(N_POSTS):
    author_id = random.choice(user_ids)
    group_id = random.choice(group_ids)
    created = rand_ts(after=max(user_dates[author_id], group_dates[group_id]))

    topic = random.choice(COMMUNITY_TOPICS)
    title = random.choice(POST_TITLE_TEMPLATES).format(topic=topic, year=random.choice([2023, 2024]))

    # roughly 55% text, 25% link, 20% image
    post_type = random.choices(["text", "link", "image"], weights=[0.55, 0.25, 0.20], k=1)[0]
    
    posts.append({
        "id": uid(),
        "author_id": author_id,
        "subreddit_id": group_id,
        "post_type": post_type,
        "title": title[:300],
        "body": fake.paragraph(nb_sentences=random.randint(2, 8)) if post_type == "text" else "",
        "url": fake.url() if post_type in ("link", "image") else "",
        "score": 0,
        "comment_count": 0,
        "is_archived": random.random() < 0.08,
        "created_at": ts_str(created),
    })

write_csv("post.csv", ["id", "author_id", "subreddit_id", "post_type", "title", "body", "url", "score", "comment_count", "is_archived", "created_at"], posts)

post_ids = [p["id"] for p in posts]
post_dates = {p["id"]: datetime.strptime(p["created_at"], "%Y-%m-%d %H:%M:%S+00").replace(tzinfo=timezone.utc) for p in posts}

# --- Comments ---
print("Generating 22k comments... this takes a sec.")
N_COMMENTS = 22000
comments = []
post_comment_ids = {} # Keep track for reply chains

active_posts = [p for p in posts if not p["is_archived"]]

for _ in range(N_COMMENTS):
    post = random.choice(active_posts)
    post_id = post["id"]
    author_id = random.choice(user_ids)
    created = rand_ts(after=post_dates[post_id])

    # 30% chance to nest it if there's already comments
    parent_id = ""
    existing = post_comment_ids.get(post_id, [])
    if existing and random.random() < 0.30:
        parent_id = random.choice(existing)

    cid = uid()
    comments.append({
        "id": cid,
        "post_id": post_id,
        "author_id": author_id,
        "parent_comment_id": parent_id,
        "body": fake.paragraph(nb_sentences=random.randint(1, 5)),
        "score": 0,
        "is_archived": random.random() < 0.04,
        "created_at": ts_str(created),
    })
    post_comment_ids.setdefault(post_id, []).append(cid)

write_csv("comment.csv", ["id", "post_id", "author_id", "parent_comment_id", "body", "score", "is_archived", "created_at"], comments)
comment_ids = [c["id"] for c in comments]
comment_dates = {c["id"]: datetime.strptime(c["created_at"], "%Y-%m-%d %H:%M:%S+00").replace(tzinfo=timezone.utc) for c in comments}

# --- Votes ---
N_VOTES = 26500
votes = []
vote_pairs = set()

# Helper for votes to avoid copy-pasting the same logic
def gen_votes(targets, target_type, n):
    generated = []
    for _ in range(n * 8):
        if len(generated) >= n: break
            
        u, target = random.choice(user_ids), random.choice(targets)
        if (u, target) in vote_pairs:
            continue
            
        vote_pairs.add((u, target))
        # heavy upvote bias like real reddit
        value = 1 if random.random() < 0.90 else -1
        
        lo = post_dates.get(target, SIM_START) if target_type == "post" else comment_dates.get(target, SIM_START)
        
        generated.append({
            "id": uid(),
            "user_id": u,
            "target_id": target,
            "target_type": target_type,
            "value": value,
            "created_at": ts_str(rand_ts(after=lo)),
        })
    return generated

# 40% on posts, 60% on comments
votes += gen_votes(post_ids, "post", int(N_VOTES * 0.40))
votes += gen_votes(comment_ids, "comment", N_VOTES - int(N_VOTES * 0.40))

write_csv("vote.csv", ["id", "user_id", "target_id", "target_type", "value", "created_at"], votes)

# --- Tags ---
N_TAGS = 1500
tags = []
for _ in range(N_TAGS):
    tags.append({
        "id": uid(),
        "subreddit_id": random.choice(group_ids),
        "text": random.choice(TAG_WORDS),
        "color": random.choice(TAG_COLORS),
    })

write_csv("tag.csv", ["id", "subreddit_id", "text", "color"], tags)

tags_by_group = {}
for t in tags:
    tags_by_group.setdefault(t["subreddit_id"], []).append(t["id"])

# --- Post Tags ---
N_POST_TAGS = 4200
post_tags = []
pt_pairs = set()

# grouping by sub to make mapping easier
posts_by_group = {}
for p in posts:
    posts_by_group.setdefault(p["subreddit_id"], []).append(p)

for _ in range(N_POST_TAGS * 10):
    if len(post_tags) >= N_POST_TAGS:
        break
        
    group_id = random.choice(group_ids)
    group_posts = posts_by_group.get(group_id, [])
    group_tags = tags_by_group.get(group_id, [])
    
    if not group_posts or not group_tags:
        continue

    post = random.choice(group_posts)
    tag_id = random.choice(group_tags)

    if (post["id"], tag_id) in pt_pairs:
        continue
        
    pt_pairs.add((post["id"], tag_id))
    post_tags.append({
        "post_id": post["id"],
        "tag_id": tag_id,
        "assigned_at": ts_str(post_dates[post["id"]]),
    })

write_csv("post_tag.csv", ["post_id", "tag_id", "assigned_at"], post_tags)

# --- Reports ---
N_REPORTS = 1250
reports = []

for _ in range(N_REPORTS):
    reporter_id = random.choice(user_ids)
    target_type = random.choices(["post", "comment"], weights=[0.50, 0.50])[0]

    if target_type == "post":
        target_id = random.choice(post_ids)
        lo = post_dates[target_id]
    else:
        target_id = random.choice(comment_ids)
        lo = comment_dates[target_id]

    created = rand_ts(after=max(lo, user_dates[reporter_id]))
    status = random.choices(["pending", "dismissed", "approved"], weights=[0.60, 0.25, 0.15])[0]

    resolved_by, resolved_at = "", ""
    if status != "pending":
        # TODO: currently picking any random user as mod, need to fix this to only pick actual group mods later
        resolved_by = random.choice(user_ids)
        resolved_at = ts_str(rand_ts(after=created, before=min(created + timedelta(days=14), SIM_END)))

    reports.append({
        "id": uid(),
        "reporter_id": reporter_id,
        "resolved_by": resolved_by,
        "target_id": target_id,
        "target_type": target_type,
        "reason": random.choice(REPORT_REASONS),
        "status": status,
        "created_at": ts_str(created),
        "resolved_at": resolved_at,
    })

write_csv("report.csv", ["id", "reporter_id", "resolved_by", "target_id", "target_type", "reason", "status", "created_at", "resolved_at"], reports)

# --- Notifications ---
N_NOTIFS = 13000
notifications = []

notif_opts = ["comment_reply", "post_comment", "mention", "award_received", "mod_action"]
notif_weights = [0.45, 0.30, 0.15, 0.08, 0.02]

for _ in range(N_NOTIFS):
    n_type = random.choices(notif_opts, weights=notif_weights)[0]
    t_type = random.choices(["post", "comment", "user"], weights=[0.35, 0.50, 0.15])[0]

    recipient = random.choice(user_ids)
    actor = random.choice(user_ids)
    while actor == recipient:
        actor = random.choice(user_ids)

    if t_type == "post":
        target = random.choice(post_ids)
        lo = post_dates[target]
    elif t_type == "comment":
        target = random.choice(comment_ids)
        lo = comment_dates[target]
    else:
        target = random.choice(user_ids)
        lo = user_dates[target]

    created = rand_ts(after=max(lo, user_dates[recipient], user_dates[actor]))

    notifications.append({
        "id": uid(),
        "user_id": recipient,
        "actor_id": actor,
        "target_id": target,
        "type": n_type,
        "target_type": t_type,
        "is_read": random.random() < 0.65,
        "created_at": ts_str(created),
    })

write_csv("notification.csv", ["id", "user_id", "actor_id", "target_id", "type", "target_type", "is_read", "created_at"], notifications)

print("\nDone! generated all records successfully.")

sql_instructions = """
=========================================
DB Load Instructions
=========================================
Run these in psql (make sure triggers are DISABLED first!):

\COPY "user" FROM 'csv_output/user.csv' CSV HEADER NULL ''
\COPY "group" FROM 'csv_output/group.csv' CSV HEADER NULL ''
\COPY follow FROM 'csv_output/follow.csv' CSV HEADER NULL ''
\COPY moderator FROM 'csv_output/moderator.csv' CSV HEADER NULL ''
\COPY post FROM 'csv_output/post.csv' CSV HEADER NULL ''
\COPY comment FROM 'csv_output/comment.csv' CSV HEADER NULL ''
\COPY vote FROM 'csv_output/vote.csv' CSV HEADER NULL ''
\COPY tag FROM 'csv_output/tag.csv' CSV HEADER NULL ''
\COPY post_tag FROM 'csv_output/post_tag.csv' CSV HEADER NULL ''
\COPY report FROM 'csv_output/report.csv' CSV HEADER NULL ''
\COPY notification FROM 'csv_output/notification.csv' CSV HEADER NULL ''

Once that's done, re-enable triggers and run these fixes to backfill the counters:

UPDATE post p SET score = (SELECT COALESCE(SUM(v.value), 0) FROM vote v WHERE v.target_id = p.id AND v.target_type = 'post');
UPDATE comment c SET score = (SELECT COALESCE(SUM(v.value), 0) FROM vote v WHERE v.target_id = c.id AND v.target_type = 'comment');
UPDATE post p SET comment_count = (SELECT COUNT(*) FROM comment c WHERE c.post_id = p.id);
UPDATE "group" g SET member_count = (SELECT COUNT(*) FROM follow f WHERE f.subreddit_id = g.id);
"""

print(sql_instructions)

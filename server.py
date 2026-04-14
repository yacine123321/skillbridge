#!/usr/bin/env python3
"""
SkillBridge Backend — Python stdlib only (no pip required)
Run: python3 server.py
API runs on http://localhost:8000
"""

import sqlite3, json, hashlib, hmac, base64, uuid, time, os, re
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
PORT = 8000
DB_PATH = "skillbridge.db"
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production-" + uuid.uuid4().hex)
JWT_EXPIRY = 60 * 60 * 24 * 7  # 7 days
DAILY_EARN_LIMIT = 500
PLATFORM_FEE_PCT = 0.05
CREDIT_USD_VALUE = 0.10

# ─────────────────────────────────────────────
# DATABASE — setup
# ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.executescript("""
    -- USERS
    CREATE TABLE IF NOT EXISTS users (
        id          TEXT PRIMARY KEY,
        email       TEXT UNIQUE NOT NULL,
        name        TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        country     TEXT,
        phone       TEXT,
        bio         TEXT DEFAULT '',
        avatar_initials TEXT DEFAULT '',
        level       TEXT DEFAULT 'intermediate',
        credits     INTEGER DEFAULT 0,
        streak      INTEGER DEFAULT 0,
        last_active TEXT,
        daily_earned INTEGER DEFAULT 0,
        daily_reset TEXT,
        trust_score INTEGER DEFAULT 10,
        earned_month INTEGER DEFAULT 0,
        spent_month  INTEGER DEFAULT 0,
        burned_total INTEGER DEFAULT 0,
        affiliate_code TEXT UNIQUE,
        referred_by  TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        is_banned   INTEGER DEFAULT 0,
        ban_reason  TEXT,
        -- verification flags (0/1)
        v_email     INTEGER DEFAULT 0,
        v_phone     INTEGER DEFAULT 0,
        v_id        INTEGER DEFAULT 0,
        v_skill     INTEGER DEFAULT 0,
        -- device fingerprint for multi-account detection
        device_fp   TEXT
    );

    -- LISTINGS
    CREATE TABLE IF NOT EXISTS listings (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL REFERENCES users(id),
        title       TEXT NOT NULL,
        description TEXT NOT NULL,
        category    TEXT NOT NULL,
        level       TEXT NOT NULL,
        price_per_hour INTEGER NOT NULL,
        duration_hours REAL NOT NULL DEFAULT 1.0,
        status      TEXT DEFAULT 'pending',  -- pending|active|paused|rejected
        is_verified INTEGER DEFAULT 0,
        is_featured INTEGER DEFAULT 0,
        bookings    INTEGER DEFAULT 0,
        total_earned INTEGER DEFAULT 0,
        avg_rating  REAL DEFAULT 0,
        review_count INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now')),
        rejection_reason TEXT
    );

    -- ESCROW / SESSIONS
    CREATE TABLE IF NOT EXISTS escrows (
        id          TEXT PRIMARY KEY,
        buyer_id    TEXT NOT NULL REFERENCES users(id),
        seller_id   TEXT NOT NULL REFERENCES users(id),
        listing_id  TEXT NOT NULL REFERENCES listings(id),
        credits_held INTEGER NOT NULL,
        fee_amount  INTEGER NOT NULL,
        net_amount  INTEGER NOT NULL,
        status      TEXT DEFAULT 'held',  -- held|buyer_confirmed|seller_confirmed|released|refunded|disputed
        buyer_confirmed  INTEGER DEFAULT 0,
        seller_confirmed INTEGER DEFAULT 0,
        scheduled_at TEXT,
        completed_at TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        dispute_id  TEXT
    );

    -- REVIEWS
    CREATE TABLE IF NOT EXISTS reviews (
        id          TEXT PRIMARY KEY,
        escrow_id   TEXT NOT NULL REFERENCES escrows(id),
        reviewer_id TEXT NOT NULL REFERENCES users(id),
        reviewed_id TEXT NOT NULL REFERENCES users(id),
        listing_id  TEXT NOT NULL,
        rating      INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
        comment     TEXT,
        -- fraud prevention
        is_ai_flagged INTEGER DEFAULT 0,
        flag_reason TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(escrow_id, reviewer_id)  -- one review per escrow per user
    );

    -- TRANSACTIONS
    CREATE TABLE IF NOT EXISTS transactions (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL REFERENCES users(id),
        type        TEXT NOT NULL,  -- earn|spend|buy|burn|bonus|referral|dispute_refund
        amount      INTEGER NOT NULL,  -- positive=credit, negative=debit
        description TEXT,
        reference_id TEXT,  -- escrow_id, achievement_id, etc
        created_at  TEXT DEFAULT (datetime('now'))
    );

    -- DISPUTES
    CREATE TABLE IF NOT EXISTS disputes (
        id          TEXT PRIMARY KEY,
        escrow_id   TEXT NOT NULL REFERENCES escrows(id),
        filed_by    TEXT NOT NULL REFERENCES users(id),
        reason      TEXT NOT NULL,
        description TEXT NOT NULL,
        evidence_urls TEXT DEFAULT '[]',
        status      TEXT DEFAULT 'open',  -- open|under_review|resolved_buyer|resolved_seller|closed
        resolution  TEXT,
        credits_refunded INTEGER DEFAULT 0,
        moderator_notes TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        resolved_at TEXT
    );

    -- NOTIFICATIONS
    CREATE TABLE IF NOT EXISTS notifications (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL REFERENCES users(id),
        icon        TEXT,
        title       TEXT NOT NULL,
        body        TEXT,
        is_read     INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now'))
    );

    -- AFFILIATE REFERRALS
    CREATE TABLE IF NOT EXISTS referrals (
        id          TEXT PRIMARY KEY,
        referrer_id TEXT NOT NULL REFERENCES users(id),
        referred_id TEXT NOT NULL REFERENCES users(id),
        tier        INTEGER DEFAULT 1,  -- 1 or 2
        credits_paid INTEGER DEFAULT 0,
        qualified   INTEGER DEFAULT 0,  -- 1 after referee completes first session
        created_at  TEXT DEFAULT (datetime('now'))
    );

    -- ACHIEVEMENTS
    CREATE TABLE IF NOT EXISTS user_achievements (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL REFERENCES users(id),
        achievement_key TEXT NOT NULL,
        credits_awarded INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, achievement_key)
    );

    -- INVENTORY
    CREATE TABLE IF NOT EXISTS inventory (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL REFERENCES users(id),
        item_type   TEXT NOT NULL,  -- session|cert|membership|bundle|achievement
        name        TEXT NOT NULL,
        description TEXT,
        icon        TEXT DEFAULT '🎒',
        rarity      TEXT DEFAULT 'common',
        cost_paid   INTEGER DEFAULT 0,
        reference_id TEXT,  -- escrow_id for sessions
        is_used     INTEGER DEFAULT 0,
        used_at     TEXT,
        expires_at  TEXT,
        created_at  TEXT DEFAULT (datetime('now'))
    );

    -- SKILL PATHS PROGRESS
    CREATE TABLE IF NOT EXISTS path_progress (
        id          TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL REFERENCES users(id),
        path_key    TEXT NOT NULL,
        step_index  INTEGER DEFAULT 0,
        completed   INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, path_key)
    );

    -- FRAUD LOG
    CREATE TABLE IF NOT EXISTS fraud_log (
        id          TEXT PRIMARY KEY,
        user_id     TEXT,
        event_type  TEXT,
        details     TEXT,
        ip_address  TEXT,
        created_at  TEXT DEFAULT (datetime('now'))
    );

    -- INDEXES for performance
    CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
    CREATE INDEX IF NOT EXISTS idx_listings_cat ON listings(category);
    CREATE INDEX IF NOT EXISTS idx_escrows_buyer ON escrows(buyer_id);
    CREATE INDEX IF NOT EXISTS idx_escrows_seller ON escrows(seller_id);
    CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, is_read);
    CREATE INDEX IF NOT EXISTS idx_reviews_reviewed ON reviews(reviewed_id);
    """)

    conn.commit()
    conn.close()
    print("✓ Database initialised:", DB_PATH)

# ─────────────────────────────────────────────
# AUTH — JWT-like tokens (HMAC-SHA256)
# ─────────────────────────────────────────────
def make_token(user_id: str) -> str:
    payload = json.dumps({"uid": user_id, "exp": int(time.time()) + JWT_EXPIRY})
    b64 = base64.urlsafe_b64encode(payload.encode()).decode()
    sig = hmac.new(JWT_SECRET.encode(), b64.encode(), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"

def verify_token(token: str):
    try:
        b64, sig = token.rsplit(".", 1)
        expected = hmac.new(JWT_SECRET.encode(), b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(b64 + "==").decode())
        if payload["exp"] < int(time.time()):
            return None
        return payload["uid"]
    except Exception:
        return None

def hash_password(pw: str) -> str:
    salt = uuid.uuid4().hex
    h = hashlib.sha256((salt + pw).encode()).hexdigest()
    return f"{salt}:{h}"

def check_password(pw: str, stored: str) -> bool:
    try:
        salt, h = stored.split(":", 1)
        return hashlib.sha256((salt + pw).encode()).hexdigest() == h
    except Exception:
        return False

def gen_id() -> str:
    return uuid.uuid4().hex[:16]

def gen_affiliate_code(name: str) -> str:
    return (name[:4].upper().replace(" ", "") + "-" + uuid.uuid4().hex[:6].upper())

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def row_to_dict(row):
    if row is None:
        return None
    return dict(row)

def rows_to_list(rows):
    return [dict(r) for r in rows]

def add_transaction(conn, user_id, type_, amount, desc, ref=None):
    conn.execute(
        "INSERT INTO transactions(id,user_id,type,amount,description,reference_id) VALUES(?,?,?,?,?,?)",
        (gen_id(), user_id, type_, amount, desc, ref)
    )

def add_notification(conn, user_id, icon, title, body):
    conn.execute(
        "INSERT INTO notifications(id,user_id,icon,title,body) VALUES(?,?,?,?,?)",
        (gen_id(), user_id, icon, title, body)
    )

def check_daily_limit(conn, user_id, amount):
    """Returns (ok, remaining). Resets daily counter if new day."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    user = row_to_dict(conn.execute("SELECT daily_earned,daily_reset FROM users WHERE id=?", (user_id,)).fetchone())
    if user["daily_reset"] != today:
        conn.execute("UPDATE users SET daily_earned=0,daily_reset=? WHERE id=?", (today, user_id))
        earned = 0
    else:
        earned = user["daily_earned"] or 0
    remaining = DAILY_EARN_LIMIT - earned
    return (remaining >= amount, remaining)

def award_credits(conn, user_id, amount, type_, desc, ref=None):
    ok, _ = check_daily_limit(conn, user_id, amount)
    if not ok:
        return False, "Daily earning limit reached (500 cr/day)"
    conn.execute("UPDATE users SET credits=credits+?, earned_month=earned_month+?, daily_earned=daily_earned+? WHERE id=?",
                 (amount, amount, amount, user_id))
    add_transaction(conn, user_id, type_, amount, desc, ref)
    return True, "ok"

def deduct_credits(conn, user_id, amount, type_, desc, ref=None):
    user = row_to_dict(conn.execute("SELECT credits FROM users WHERE id=?", (user_id,)).fetchone())
    if user["credits"] < amount:
        return False, "Insufficient credits"
    conn.execute("UPDATE users SET credits=credits-?, spent_month=spent_month+? WHERE id=?",
                 (amount, amount, user_id))
    add_transaction(conn, user_id, type_, -amount, desc, ref)
    return True, "ok"

# ─────────────────────────────────────────────
# DYNAMIC CREDIT PRICING
# ─────────────────────────────────────────────
DEMAND_MULTIPLIERS = {
    "tech": 1.4, "lang": 1.1, "creative": 1.15,
    "business": 1.25, "life": 0.9
}
LEVEL_BASE = {
    "beginner": 12, "intermediate": 22, "advanced": 38, "expert": 65
}

def suggest_price(category, level):
    base = LEVEL_BASE.get(level, 22)
    mult = DEMAND_MULTIPLIERS.get(category, 1.0)
    return round(base * mult)

# ─────────────────────────────────────────────
# HTTP HANDLER
# ─────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {args[0]} {args[1]}")

    # ── CORS + routing ──
    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,PATCH,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,Authorization")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors()
        self.end_headers()

    def json_response(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def error(self, msg, status=400):
        self.json_response({"error": msg}, status)

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except Exception:
            return {}

    def get_user(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        return verify_token(auth[7:])

    def require_user(self):
        uid = self.get_user()
        if not uid:
            self.error("Unauthorized", 401)
        return uid

    # ── ROUTING ──
    def do_GET(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")
        qs = parse_qs(p.query)
        self.route(path, "GET", qs, {})

    def do_POST(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")
        body = self.read_body()
        self.route(path, "POST", {}, body)

    def do_PUT(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")
        body = self.read_body()
        self.route(path, "PUT", {}, body)

    def do_PATCH(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")
        body = self.read_body()
        self.route(path, "PATCH", {}, body)

    def do_DELETE(self):
        p = urlparse(self.path)
        path = p.path.rstrip("/")
        self.route(path, "DELETE", {}, {})

    def route(self, path, method, qs, body):
        routes = [
            # Auth
            ("POST", "/api/auth/register",   self.register),
            ("POST", "/api/auth/login",       self.login),
            ("GET",  "/api/auth/me",          self.me),
            # Users
            ("GET",  "/api/users",            self.list_users),
            ("GET",  r"/api/users/(\w+)",     self.get_user_profile),
            # Listings
            ("GET",  "/api/listings",         self.list_listings),
            ("POST", "/api/listings",         self.create_listing),
            ("GET",  r"/api/listings/(\w+)",  self.get_listing),
            ("PATCH",r"/api/listings/(\w+)",  self.update_listing),
            ("DELETE",r"/api/listings/(\w+)", self.delete_listing),
            ("GET",  "/api/listings/suggest-price", self.listing_suggest_price),
            # Escrow / Bookings
            ("POST", "/api/escrow",           self.create_escrow),
            ("GET",  "/api/escrow",           self.list_escrows),
            ("GET",  r"/api/escrow/(\w+)",    self.get_escrow),
            ("POST", r"/api/escrow/(\w+)/confirm", self.confirm_escrow),
            ("POST", r"/api/escrow/(\w+)/dispute", self.dispute_escrow),
            # Reviews
            ("POST", "/api/reviews",          self.create_review),
            ("GET",  r"/api/reviews/user/(\w+)", self.user_reviews),
            # Wallet
            ("GET",  "/api/wallet",           self.wallet_summary),
            ("GET",  "/api/wallet/transactions", self.list_transactions),
            ("POST", "/api/wallet/buy",       self.buy_credits),
            # Notifications
            ("GET",  "/api/notifications",    self.list_notifications),
            ("POST", "/api/notifications/read-all", self.read_all_notifications),
            # Inventory
            ("GET",  "/api/inventory",        self.list_inventory),
            ("POST", r"/api/inventory/(\w+)/use", self.use_inventory_item),
            # Disputes
            ("POST", "/api/disputes",         self.create_dispute),
            ("GET",  "/api/disputes",         self.list_disputes),
            # Affiliate
            ("GET",  "/api/affiliate",        self.affiliate_stats),
            # Achievements
            ("GET",  "/api/achievements",     self.list_achievements),
            # Leaderboard
            ("GET",  "/api/leaderboard",      self.leaderboard),
            # Verify
            ("POST", r"/api/verify/(\w+)",    self.run_verification),
            # Stats
            ("GET",  "/api/stats",            self.platform_stats),
            # Health
            ("GET",  "/api/health",           self.health),
        ]

        # Match route
        for route_method, pattern, handler in routes:
            if route_method != method:
                continue
            m = re.fullmatch(pattern, path)
            if m:
                try:
                    handler(qs=qs, body=body, args=m.groups())
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    self.error(f"Server error: {str(e)}", 500)
                return

        self.error("Not found", 404)

    # ══════════════════════════════════════════
    # AUTH ENDPOINTS
    # ══════════════════════════════════════════
    def register(self, qs, body, args):
        name = (body.get("name") or "").strip()
        email = (body.get("email") or "").strip().lower()
        password = body.get("password") or ""
        country = body.get("country") or ""
        phone = body.get("phone") or ""
        referred_by_code = body.get("referral_code") or ""
        device_fp = body.get("device_fp") or ""

        if not name or not email or not password:
            return self.error("Name, email, and password required")
        if len(password) < 6:
            return self.error("Password must be at least 6 characters")
        if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
            return self.error("Invalid email format")

        conn = get_db()
        try:
            # Check duplicate email
            if conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone():
                return self.error("Email already registered")

            # Check device fingerprint for multi-account abuse
            if device_fp and conn.execute("SELECT id FROM users WHERE device_fp=?", (device_fp,)).fetchone():
                conn.execute("INSERT INTO fraud_log(id,event_type,details) VALUES(?,?,?)",
                             (gen_id(), "duplicate_device", json.dumps({"email": email, "fp": device_fp})))
                conn.commit()
                return self.error("An account already exists on this device. One account per person is allowed.")

            uid = gen_id()
            aff_code = gen_affiliate_code(name)
            initials = "".join(p[0].upper() for p in name.split()[:2])

            conn.execute("""
                INSERT INTO users(id,email,name,password_hash,country,phone,avatar_initials,
                                  affiliate_code,device_fp,daily_reset,v_email)
                VALUES(?,?,?,?,?,?,?,?,?,?,1)
            """, (uid, email, name, hash_password(password), country, phone,
                  initials, aff_code, device_fp,
                  datetime.now(timezone.utc).strftime("%Y-%m-%d")))

            # Handle referral
            if referred_by_code:
                referrer = row_to_dict(conn.execute("SELECT id FROM users WHERE affiliate_code=?",
                                                     (referred_by_code,)).fetchone())
                if referrer:
                    conn.execute("UPDATE users SET referred_by=? WHERE id=?", (referrer["id"], uid))
                    conn.execute("INSERT INTO referrals(id,referrer_id,referred_id,tier) VALUES(?,?,?,1)",
                                 (gen_id(), referrer["id"], uid, 1))
                    # Check tier 2
                    referrer_user = row_to_dict(conn.execute("SELECT referred_by FROM users WHERE id=?",
                                                              (referrer["id"],)).fetchone())
                    if referrer_user and referrer_user.get("referred_by"):
                        conn.execute("INSERT INTO referrals(id,referrer_id,referred_id,tier) VALUES(?,?,?,2)",
                                     (gen_id(), referrer_user["referred_by"], uid, 2))

            conn.commit()
            token = make_token(uid)
            user = row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone())
            user.pop("password_hash", None)
            self.json_response({"token": token, "user": user}, 201)
        finally:
            conn.close()

    def login(self, qs, body, args):
        email = (body.get("email") or "").strip().lower()
        password = body.get("password") or ""
        if not email or not password:
            return self.error("Email and password required")

        conn = get_db()
        try:
            user = row_to_dict(conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone())
            if not user or not check_password(password, user["password_hash"]):
                return self.error("Invalid email or password", 401)
            if user["is_banned"]:
                return self.error(f"Account banned: {user.get('ban_reason','Policy violation')}", 403)

            conn.execute("UPDATE users SET last_active=datetime('now') WHERE id=?", (user["id"],))
            conn.commit()
            token = make_token(user["id"])
            user.pop("password_hash", None)
            self.json_response({"token": token, "user": user})
        finally:
            conn.close()

    def me(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            user = row_to_dict(conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone())
            if not user: return self.error("User not found", 404)
            user.pop("password_hash", None)
            # Reset daily limit if needed
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if user.get("daily_reset") != today:
                conn.execute("UPDATE users SET daily_earned=0,daily_reset=? WHERE id=?", (today, uid))
                conn.commit()
                user["daily_earned"] = 0
            self.json_response(user)
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # USERS
    # ══════════════════════════════════════════
    def list_users(self, qs, body, args):
        conn = get_db()
        try:
            users = rows_to_list(conn.execute(
                "SELECT id,name,avatar_initials,country,trust_score,credits,streak FROM users WHERE is_banned=0 LIMIT 50"
            ).fetchall())
            self.json_response(users)
        finally:
            conn.close()

    def get_user_profile(self, qs, body, args):
        uid = args[0]
        conn = get_db()
        try:
            user = row_to_dict(conn.execute(
                "SELECT id,name,avatar_initials,country,bio,trust_score,streak,earned_month,v_email,v_phone,v_id,v_skill,created_at FROM users WHERE id=?",
                (uid,)).fetchone())
            if not user: return self.error("User not found", 404)
            listings = rows_to_list(conn.execute(
                "SELECT id,title,category,price_per_hour,avg_rating,review_count,bookings FROM listings WHERE user_id=? AND status='active'",
                (uid,)).fetchall())
            reviews = rows_to_list(conn.execute(
                "SELECT r.rating,r.comment,r.created_at,u.name as reviewer_name FROM reviews r JOIN users u ON r.reviewer_id=u.id WHERE r.reviewed_id=? ORDER BY r.created_at DESC LIMIT 10",
                (uid,)).fetchall())
            self.json_response({"user": user, "listings": listings, "reviews": reviews})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # LISTINGS
    # ══════════════════════════════════════════
    def listing_suggest_price(self, qs, body, args):
        cat = (qs.get("category", ["tech"])[0])
        level = (qs.get("level", ["intermediate"])[0])
        self.json_response({"suggested_price": suggest_price(cat, level)})

    def list_listings(self, qs, body, args):
        cat = qs.get("category", [None])[0]
        sort = qs.get("sort", ["newest"])[0]
        limit = int(qs.get("limit", [20])[0])
        offset = int(qs.get("offset", [0])[0])
        search = qs.get("q", [None])[0]

        conn = get_db()
        try:
            where = ["l.status='active'"]
            params = []
            if cat:
                where.append("l.category=?")
                params.append(cat)
            if search:
                where.append("(l.title LIKE ? OR l.description LIKE ?)")
                params += [f"%{search}%", f"%{search}%"]

            order = {"newest": "l.created_at DESC", "price_asc": "l.price_per_hour ASC",
                     "price_desc": "l.price_per_hour DESC", "rating": "l.avg_rating DESC"}.get(sort, "l.created_at DESC")

            sql = f"""
                SELECT l.*, u.name as provider_name, u.avatar_initials, u.country as provider_country,
                       u.trust_score, u.v_skill as provider_verified
                FROM listings l JOIN users u ON l.user_id=u.id
                WHERE {' AND '.join(where)}
                ORDER BY l.is_featured DESC, {order}
                LIMIT ? OFFSET ?
            """
            params += [limit, offset]
            listings = rows_to_list(conn.execute(sql, params).fetchall())
            total = conn.execute(f"SELECT COUNT(*) FROM listings l WHERE {' AND '.join(where)}",
                                 params[:-2]).fetchone()[0]
            self.json_response({"listings": listings, "total": total})
        finally:
            conn.close()

    def create_listing(self, qs, body, args):
        uid = self.require_user()
        if not uid: return

        title = (body.get("title") or "").strip()
        description = (body.get("description") or "").strip()
        category = body.get("category") or "tech"
        level = body.get("level") or "intermediate"
        price = int(body.get("price_per_hour") or suggest_price(category, level))
        duration = float(body.get("duration_hours") or 1.0)

        if not title:
            return self.error("Title is required")
        if len(description) < 60:
            return self.error("Description must be at least 60 characters. Be specific about what you deliver.")
        if price < 5 or price > 500:
            return self.error("Price must be between 5 and 500 credits per hour")
        if duration not in [0.5, 1.0, 1.5, 2.0]:
            duration = 1.0

        conn = get_db()
        try:
            # Check user isn't banned
            user = row_to_dict(conn.execute("SELECT is_banned,trust_score FROM users WHERE id=?", (uid,)).fetchone())
            if user["is_banned"]:
                return self.error("Account is banned", 403)

            lid = gen_id()
            conn.execute("""
                INSERT INTO listings(id,user_id,title,description,category,level,price_per_hour,duration_hours,status)
                VALUES(?,?,?,?,?,?,?,?,'pending')
            """, (lid, uid, title, description, category, level, price, duration))
            conn.commit()

            # Auto-approve basic listings (in production: human + AI review queue)
            conn.execute("UPDATE listings SET status='active' WHERE id=?", (lid,))
            conn.commit()

            listing = row_to_dict(conn.execute("SELECT * FROM listings WHERE id=?", (lid,)).fetchone())
            self.json_response(listing, 201)
        finally:
            conn.close()

    def get_listing(self, qs, body, args):
        lid = args[0]
        conn = get_db()
        try:
            listing = row_to_dict(conn.execute(
                "SELECT l.*,u.name as provider_name,u.avatar_initials,u.country as provider_country,u.trust_score,u.bio as provider_bio FROM listings l JOIN users u ON l.user_id=u.id WHERE l.id=?",
                (lid,)).fetchone())
            if not listing: return self.error("Listing not found", 404)
            reviews = rows_to_list(conn.execute(
                "SELECT r.rating,r.comment,r.created_at,u.name as reviewer_name FROM reviews r JOIN users u ON r.reviewer_id=u.id WHERE r.listing_id=? ORDER BY r.created_at DESC LIMIT 10",
                (lid,)).fetchall())
            self.json_response({"listing": listing, "reviews": reviews})
        finally:
            conn.close()

    def update_listing(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        lid = args[0]
        conn = get_db()
        try:
            listing = row_to_dict(conn.execute("SELECT user_id FROM listings WHERE id=?", (lid,)).fetchone())
            if not listing: return self.error("Not found", 404)
            if listing["user_id"] != uid: return self.error("Forbidden", 403)
            allowed = ["title", "description", "price_per_hour", "duration_hours", "status"]
            updates = {k: v for k, v in body.items() if k in allowed}
            if not updates: return self.error("Nothing to update")
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(f"UPDATE listings SET {set_clause} WHERE id=?", list(updates.values()) + [lid])
            conn.commit()
            self.json_response({"ok": True})
        finally:
            conn.close()

    def delete_listing(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        lid = args[0]
        conn = get_db()
        try:
            listing = row_to_dict(conn.execute("SELECT user_id FROM listings WHERE id=?", (lid,)).fetchone())
            if not listing: return self.error("Not found", 404)
            if listing["user_id"] != uid: return self.error("Forbidden", 403)
            # Soft delete — pause, not remove (preserve escrow history)
            conn.execute("UPDATE listings SET status='paused' WHERE id=?", (lid,))
            conn.commit()
            self.json_response({"ok": True})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # ESCROW
    # ══════════════════════════════════════════
    def create_escrow(self, qs, body, args):
        uid = self.require_user()
        if not uid: return

        listing_id = body.get("listing_id")
        if not listing_id: return self.error("listing_id required")

        conn = get_db()
        try:
            listing = row_to_dict(conn.execute("SELECT * FROM listings WHERE id=? AND status='active'", (listing_id,)).fetchone())
            if not listing: return self.error("Listing not available")
            if listing["user_id"] == uid: return self.error("Cannot book your own listing")

            total = round(listing["price_per_hour"] * listing["duration_hours"])
            fee = max(1, round(total * PLATFORM_FEE_PCT))
            net = total - fee

            # Deduct from buyer
            ok, msg = deduct_credits(conn, uid, total, "spend", f"Escrow: {listing['title']}", listing_id)
            if not ok: return self.error(msg)

            eid = "ESC-" + gen_id()[:8].upper()
            conn.execute("""
                INSERT INTO escrows(id,buyer_id,seller_id,listing_id,credits_held,fee_amount,net_amount)
                VALUES(?,?,?,?,?,?,?)
            """, (eid, uid, listing["user_id"], listing_id, total, fee, net))

            # Add to buyer inventory
            inv_id = gen_id()
            conn.execute("""
                INSERT INTO inventory(id,user_id,item_type,name,description,icon,rarity,cost_paid,reference_id,expires_at)
                VALUES(?,?,?,?,?,?,?,?,?,date('now','+30 days'))
            """, (inv_id, uid, "session", listing["title"],
                  f"Session with {listing['provider_name'] if 'provider_name' in listing else 'provider'}. Contact them to schedule.",
                  "🎓", "rare", total, eid))

            # Notify seller
            add_notification(conn, listing["user_id"], "🔔", "New booking!",
                             f"Someone booked your listing: {listing['title']}. Escrow ID: {eid}")

            conn.execute("UPDATE listings SET bookings=bookings+1 WHERE id=?", (listing_id,))
            conn.commit()

            self.json_response({"escrow_id": eid, "credits_held": total, "fee": fee, "net": net, "inventory_id": inv_id}, 201)
        finally:
            conn.close()

    def list_escrows(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            escrows = rows_to_list(conn.execute("""
                SELECT e.*, l.title as listing_title,
                       buyer.name as buyer_name, seller.name as seller_name
                FROM escrows e
                JOIN listings l ON e.listing_id=l.id
                JOIN users buyer ON e.buyer_id=buyer.id
                JOIN users seller ON e.seller_id=seller.id
                WHERE e.buyer_id=? OR e.seller_id=?
                ORDER BY e.created_at DESC
            """, (uid, uid)).fetchall())
            self.json_response(escrows)
        finally:
            conn.close()

    def get_escrow(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        eid = args[0]
        conn = get_db()
        try:
            escrow = row_to_dict(conn.execute(
                "SELECT e.*,l.title,buyer.name as buyer_name,seller.name as seller_name FROM escrows e JOIN listings l ON e.listing_id=l.id JOIN users buyer ON e.buyer_id=buyer.id JOIN users seller ON e.seller_id=seller.id WHERE e.id=?",
                (eid,)).fetchone())
            if not escrow: return self.error("Not found", 404)
            if escrow["buyer_id"] != uid and escrow["seller_id"] != uid:
                return self.error("Forbidden", 403)
            self.json_response(escrow)
        finally:
            conn.close()

    def confirm_escrow(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        eid = args[0]
        conn = get_db()
        try:
            escrow = row_to_dict(conn.execute("SELECT * FROM escrows WHERE id=?", (eid,)).fetchone())
            if not escrow: return self.error("Not found", 404)
            if escrow["status"] == "released": return self.error("Already released")
            if escrow["status"] == "disputed": return self.error("Under dispute")

            if uid == escrow["buyer_id"]:
                conn.execute("UPDATE escrows SET buyer_confirmed=1 WHERE id=?", (eid,))
            elif uid == escrow["seller_id"]:
                conn.execute("UPDATE escrows SET seller_confirmed=1 WHERE id=?", (eid,))
            else:
                return self.error("Forbidden", 403)

            # Check if both confirmed → release
            updated = row_to_dict(conn.execute("SELECT * FROM escrows WHERE id=?", (eid,)).fetchone())
            if updated["buyer_confirmed"] and updated["seller_confirmed"]:
                # Release to seller
                ok, msg = award_credits(conn, escrow["seller_id"], escrow["net_amount"], "earn",
                                        f"Session completed: {eid}", eid)
                if not ok:
                    # Override daily limit for earned-not-bought credits
                    conn.execute("UPDATE users SET credits=credits+?,earned_month=earned_month+? WHERE id=?",
                                 (escrow["net_amount"], escrow["net_amount"], escrow["seller_id"]))
                    add_transaction(conn, escrow["seller_id"], "earn", escrow["net_amount"], f"Session: {eid}", eid)

                # Burn platform fee
                conn.execute("UPDATE users SET burned_total=burned_total+? WHERE id=?",
                             (escrow["fee_amount"], escrow["seller_id"]))

                conn.execute("UPDATE escrows SET status='released',completed_at=datetime('now') WHERE id=?", (eid,))

                # Mark inventory item as used
                conn.execute("UPDATE inventory SET is_used=1,used_at=datetime('now') WHERE reference_id=?", (eid,))

                # Update listing stats
                conn.execute("UPDATE listings SET total_earned=total_earned+? WHERE id=?",
                             (escrow["net_amount"], escrow["listing_id"]))

                add_notification(conn, escrow["seller_id"], "✓", "Credits released!",
                                 f"Escrow {eid} released — {escrow['net_amount']} credits added to your wallet")
                add_notification(conn, escrow["buyer_id"], "✓", "Session confirmed",
                                 f"Escrow {eid} complete. Leave a review to earn +2 credits!")

                # Check achievements for seller
                self._check_achievements(conn, escrow["seller_id"])

                conn.commit()
                return self.json_response({"status": "released", "credits_released": escrow["net_amount"]})

            conn.commit()
            side = "buyer" if uid == escrow["buyer_id"] else "seller"
            self.json_response({"status": "confirmed", "your_side": side, "waiting_for": "seller" if side == "buyer" else "buyer"})
        finally:
            conn.close()

    def dispute_escrow(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        eid = args[0]
        reason = body.get("reason", "Not specified")
        description = body.get("description", "")
        conn = get_db()
        try:
            escrow = row_to_dict(conn.execute("SELECT * FROM escrows WHERE id=?", (eid,)).fetchone())
            if not escrow: return self.error("Not found", 404)
            if escrow["buyer_id"] != uid and escrow["seller_id"] != uid:
                return self.error("Forbidden", 403)
            if escrow["status"] == "released": return self.error("Cannot dispute a released escrow")
            did = gen_id()
            conn.execute("INSERT INTO disputes(id,escrow_id,filed_by,reason,description) VALUES(?,?,?,?,?)",
                         (did, eid, uid, reason, description))
            conn.execute("UPDATE escrows SET status='disputed',dispute_id=? WHERE id=?", (did, eid))
            conn.commit()
            self.json_response({"dispute_id": did, "status": "open"}, 201)
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # REVIEWS
    # ══════════════════════════════════════════
    def create_review(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        escrow_id = body.get("escrow_id")
        rating = int(body.get("rating") or 0)
        comment = (body.get("comment") or "").strip()

        if not escrow_id or not rating: return self.error("escrow_id and rating required")
        if not 1 <= rating <= 5: return self.error("Rating must be 1–5")

        conn = get_db()
        try:
            escrow = row_to_dict(conn.execute("SELECT * FROM escrows WHERE id=?", (escrow_id,)).fetchone())
            if not escrow: return self.error("Escrow not found")
            if escrow["status"] != "released": return self.error("Can only review after session is confirmed complete")
            if escrow["buyer_id"] != uid and escrow["seller_id"] != uid:
                return self.error("Forbidden", 403)

            reviewed_id = escrow["seller_id"] if uid == escrow["buyer_id"] else escrow["buyer_id"]

            # Check duplicate
            if conn.execute("SELECT id FROM reviews WHERE escrow_id=? AND reviewer_id=?", (escrow_id, uid)).fetchone():
                return self.error("You already reviewed this session")

            # AI fraud signal: very short comment
            is_flagged = 1 if comment and len(comment) < 10 else 0
            flag_reason = "Comment too short — potential low-effort review" if is_flagged else None

            rid = gen_id()
            conn.execute("INSERT INTO reviews(id,escrow_id,reviewer_id,reviewed_id,listing_id,rating,comment,is_ai_flagged,flag_reason) VALUES(?,?,?,?,?,?,?,?,?)",
                         (rid, escrow_id, uid, reviewed_id, escrow["listing_id"], rating, comment, is_flagged, flag_reason))

            # Update listing avg rating
            avg = conn.execute("SELECT AVG(rating) FROM reviews WHERE listing_id=? AND is_ai_flagged=0",
                               (escrow["listing_id"],)).fetchone()[0]
            count = conn.execute("SELECT COUNT(*) FROM reviews WHERE listing_id=?", (escrow["listing_id"],)).fetchone()[0]
            conn.execute("UPDATE listings SET avg_rating=?,review_count=? WHERE id=?", (round(avg, 2), count, escrow["listing_id"]))

            # Award reviewer +2 credits
            award_credits(conn, uid, 2, "bonus", f"Review bonus — {escrow_id}", rid)

            add_notification(conn, reviewed_id, "⭐", "New review!", f"You received a {rating}-star review")

            conn.commit()
            self.json_response({"ok": True, "credits_earned": 2}, 201)
        finally:
            conn.close()

    def user_reviews(self, qs, body, args):
        uid = args[0]
        conn = get_db()
        try:
            reviews = rows_to_list(conn.execute(
                "SELECT r.*,u.name as reviewer_name,u.avatar_initials FROM reviews r JOIN users u ON r.reviewer_id=u.id WHERE r.reviewed_id=? AND r.is_ai_flagged=0 ORDER BY r.created_at DESC LIMIT 20",
                (uid,)).fetchall())
            self.json_response(reviews)
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # WALLET
    # ══════════════════════════════════════════
    def wallet_summary(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            user = row_to_dict(conn.execute(
                "SELECT credits,earned_month,spent_month,burned_total,daily_earned,daily_reset FROM users WHERE id=?",
                (uid,)).fetchone())
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if user["daily_reset"] != today:
                user["daily_earned"] = 0
            user["daily_remaining"] = DAILY_EARN_LIMIT - (user["daily_earned"] or 0)
            user["usd_value"] = round(user["credits"] * CREDIT_USD_VALUE, 2)
            self.json_response(user)
        finally:
            conn.close()

    def list_transactions(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            txs = rows_to_list(conn.execute(
                "SELECT * FROM transactions WHERE user_id=? ORDER BY created_at DESC LIMIT 50",
                (uid,)).fetchall())
            self.json_response(txs)
        finally:
            conn.close()

    def buy_credits(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        amount = int(body.get("amount") or 0)
        # Packages: 10=$1, 50=$4.50, 120=$9.99, 300=$22
        valid = [10, 50, 120, 300]
        if amount not in valid:
            return self.error(f"Invalid package. Choose from: {valid}")
        # In production: call Stripe API here. For now, simulate payment success.
        conn = get_db()
        try:
            # Credits bought are NOT subject to daily earning limit
            conn.execute("UPDATE users SET credits=credits+?,earned_month=earned_month+? WHERE id=?", (amount, amount, uid))
            add_transaction(conn, uid, "buy", amount, f"Bought {amount} credits", None)
            conn.commit()
            user = row_to_dict(conn.execute("SELECT credits FROM users WHERE id=?", (uid,)).fetchone())
            self.json_response({"credits_added": amount, "new_balance": user["credits"]})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # NOTIFICATIONS
    # ══════════════════════════════════════════
    def list_notifications(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            notifs = rows_to_list(conn.execute(
                "SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC LIMIT 30",
                (uid,)).fetchall())
            unread = conn.execute("SELECT COUNT(*) FROM notifications WHERE user_id=? AND is_read=0", (uid,)).fetchone()[0]
            self.json_response({"notifications": notifs, "unread": unread})
        finally:
            conn.close()

    def read_all_notifications(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            conn.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (uid,))
            conn.commit()
            self.json_response({"ok": True})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # INVENTORY
    # ══════════════════════════════════════════
    def list_inventory(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        filter_type = qs.get("type", [None])[0]
        conn = get_db()
        try:
            sql = "SELECT * FROM inventory WHERE user_id=?"
            params = [uid]
            if filter_type:
                sql += " AND item_type=?"
                params.append(filter_type)
            sql += " ORDER BY created_at DESC"
            items = rows_to_list(conn.execute(sql, params).fetchall())
            self.json_response(items)
        finally:
            conn.close()

    def use_inventory_item(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        item_id = args[0]
        conn = get_db()
        try:
            item = row_to_dict(conn.execute("SELECT * FROM inventory WHERE id=? AND user_id=?", (item_id, uid)).fetchone())
            if not item: return self.error("Item not found", 404)
            if item["is_used"]: return self.error("This item has already been used and cannot be used again")
            conn.execute("UPDATE inventory SET is_used=1,used_at=datetime('now') WHERE id=?", (item_id,))
            conn.commit()
            self.json_response({"ok": True, "message": "Item activated. Contact the provider to schedule your session."})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # DISPUTES
    # ══════════════════════════════════════════
    def create_dispute(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        escrow_id = body.get("escrow_id")
        reason = body.get("reason", "")
        description = body.get("description", "")
        if not escrow_id or not reason: return self.error("escrow_id and reason required")
        conn = get_db()
        try:
            # Penalty for false disputes tracked via resolution
            did = gen_id()
            conn.execute("INSERT INTO disputes(id,escrow_id,filed_by,reason,description) VALUES(?,?,?,?,?)",
                         (did, escrow_id, uid, reason, description))
            conn.execute("UPDATE escrows SET status='disputed',dispute_id=? WHERE id=?", (did, escrow_id))
            conn.commit()
            self.json_response({"dispute_id": did, "ticket": "DIS-" + did[:8].upper()}, 201)
        finally:
            conn.close()

    def list_disputes(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            disputes = rows_to_list(conn.execute(
                "SELECT d.*,e.credits_held FROM disputes d JOIN escrows e ON d.escrow_id=e.id WHERE d.filed_by=? OR e.buyer_id=? OR e.seller_id=? ORDER BY d.created_at DESC",
                (uid, uid, uid)).fetchall())
            self.json_response(disputes)
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # AFFILIATE
    # ══════════════════════════════════════════
    def affiliate_stats(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            user = row_to_dict(conn.execute("SELECT affiliate_code FROM users WHERE id=?", (uid,)).fetchone())
            refs = rows_to_list(conn.execute(
                "SELECT r.*,u.name,u.created_at as joined FROM referrals r JOIN users u ON r.referred_id=u.id WHERE r.referrer_id=? ORDER BY r.created_at DESC",
                (uid,)).fetchall())
            total_earned = conn.execute(
                "SELECT SUM(amount) FROM transactions WHERE user_id=? AND type='referral'", (uid,)).fetchone()[0] or 0
            self.json_response({
                "affiliate_code": user["affiliate_code"],
                "referrals": refs,
                "total_referrals": len(refs),
                "total_earned": total_earned,
                "link": f"https://skillbridge.io/?ref={user['affiliate_code']}"
            })
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # ACHIEVEMENTS (check & list)
    # ══════════════════════════════════════════
    ACHIEVEMENT_DEFS = [
        ("first_session", "First Session", 5, lambda stats: stats["sessions"] >= 1),
        ("week_streak", "Week Warrior", 10, lambda stats: stats["streak"] >= 7),
        ("first_review", "First Review", 3, lambda stats: stats["reviews_given"] >= 1),
        ("id_verified", "ID Verified", 8, lambda stats: stats["v_id"]),
        ("ten_sessions", "Veteran", 20, lambda stats: stats["sessions"] >= 10),
        ("hundred_credits", "Century", 15, lambda stats: stats["earned_total"] >= 100),
    ]

    def _check_achievements(self, conn, uid):
        """Check and award newly unlocked achievements."""
        stats = row_to_dict(conn.execute("""
            SELECT u.streak, u.trust_score, u.v_id, u.v_email, u.v_phone,
                   (SELECT COUNT(*) FROM escrows WHERE seller_id=u.id AND status='released') as sessions,
                   (SELECT COUNT(*) FROM reviews WHERE reviewer_id=u.id) as reviews_given,
                   (SELECT COALESCE(SUM(amount),0) FROM transactions WHERE user_id=u.id AND amount>0) as earned_total
            FROM users u WHERE u.id=?
        """, (uid,)).fetchone())

        for key, name, cr, condition in self.ACHIEVEMENT_DEFS:
            if not condition(stats):
                continue
            exists = conn.execute("SELECT id FROM user_achievements WHERE user_id=? AND achievement_key=?",
                                  (uid, key)).fetchone()
            if exists:
                continue
            conn.execute("INSERT INTO user_achievements(id,user_id,achievement_key,credits_awarded) VALUES(?,?,?,?)",
                         (gen_id(), uid, key, cr))
            conn.execute("UPDATE users SET credits=credits+?,earned_month=earned_month+? WHERE id=?", (cr, cr, uid))
            add_transaction(conn, uid, "bonus", cr, f"Achievement: {name}", key)
            add_notification(conn, uid, "🏆", f"Achievement unlocked: {name}!", f"+{cr} credits awarded")

    def list_achievements(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        conn = get_db()
        try:
            unlocked = rows_to_list(conn.execute(
                "SELECT achievement_key,credits_awarded,created_at FROM user_achievements WHERE user_id=?",
                (uid,)).fetchall())
            unlocked_keys = {a["achievement_key"] for a in unlocked}
            all_ach = [{"key": k, "name": n, "credits": cr, "unlocked": k in unlocked_keys}
                       for k, n, cr, _ in self.ACHIEVEMENT_DEFS]
            self.json_response({"achievements": all_ach, "unlocked_count": len(unlocked_keys)})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # LEADERBOARD
    # ══════════════════════════════════════════
    def leaderboard(self, qs, body, args):
        sort = qs.get("sort", ["credits"])[0]
        col = {"credits": "earned_month", "sessions": "sessions", "trust": "trust_score", "streak": "streak"}.get(sort, "earned_month")
        conn = get_db()
        try:
            if sort == "sessions":
                rows = rows_to_list(conn.execute("""
                    SELECT u.id,u.name,u.avatar_initials,u.country,u.trust_score,u.streak,
                           COUNT(e.id) as sessions, u.earned_month
                    FROM users u LEFT JOIN escrows e ON e.seller_id=u.id AND e.status='released'
                    WHERE u.is_banned=0
                    GROUP BY u.id ORDER BY sessions DESC LIMIT 20
                """).fetchall())
            else:
                rows = rows_to_list(conn.execute(
                    f"SELECT id,name,avatar_initials,country,trust_score,streak,earned_month FROM users WHERE is_banned=0 ORDER BY {col} DESC LIMIT 20"
                ).fetchall())
            self.json_response(rows)
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # VERIFICATION
    # ══════════════════════════════════════════
    def run_verification(self, qs, body, args):
        uid = self.require_user()
        if not uid: return
        vtype = args[0]
        conn = get_db()
        try:
            field_map = {"email": "v_email", "phone": "v_phone", "id": "v_id", "skill": "v_skill"}
            trust_map = {"email": 15, "phone": 15, "id": 20, "skill": 25}
            if vtype not in field_map:
                return self.error("Invalid verification type")
            already = conn.execute(f"SELECT {field_map[vtype]} FROM users WHERE id=?", (uid,)).fetchone()[0]
            if already:
                return self.error("Already verified")
            conn.execute(f"UPDATE users SET {field_map[vtype]}=1, trust_score=MIN(100,trust_score+?) WHERE id=?",
                         (trust_map[vtype], uid))
            conn.commit()
            # Check achievement
            self._check_achievements(conn, uid)
            conn.commit()
            self.json_response({"ok": True, "trust_points_added": trust_map[vtype]})
        finally:
            conn.close()

    # ══════════════════════════════════════════
    # PLATFORM STATS
    # ══════════════════════════════════════════
    def platform_stats(self, qs, body, args):
        conn = get_db()
        try:
            stats = {
                "total_users": conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
                "active_listings": conn.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0],
                "total_sessions": conn.execute("SELECT COUNT(*) FROM escrows WHERE status='released'").fetchone()[0],
                "total_credits_in_circulation": conn.execute("SELECT COALESCE(SUM(credits),0) FROM users").fetchone()[0],
                "total_credits_burned": conn.execute("SELECT COALESCE(SUM(burned_total),0) FROM users").fetchone()[0],
                "open_disputes": conn.execute("SELECT COUNT(*) FROM disputes WHERE status='open'").fetchone()[0],
                "dispute_rate_pct": 0,
            }
            total = stats["total_sessions"]
            disp = stats["open_disputes"]
            stats["dispute_rate_pct"] = round(disp / max(total, 1) * 100, 2)
            self.json_response(stats)
        finally:
            conn.close()

    def health(self, qs, body, args):
        self.json_response({"status": "ok", "version": "3.0.0", "db": DB_PATH, "time": datetime.now().isoformat()})


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"""
╔══════════════════════════════════════════╗
║   SkillBridge Backend v3.0               ║
║   http://localhost:{PORT}                   ║
║   Database: {DB_PATH}                ║
║   Press Ctrl+C to stop                   ║
╚══════════════════════════════════════════╝
    """)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")

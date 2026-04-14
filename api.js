/**
 * SkillBridge API Client
 * Drop this file in the same folder as your HTML.
 * Import with: <script src="api.js"></script>
 * 
 * All methods return: { data, error, status }
 */

const API_BASE = window.location.hostname === 'localhost'
  ? 'http://localhost:8000/api'
  : '/api';  // same origin in production

// ─────────────────────────────────────────────
// CORE HTTP CLIENT
// ─────────────────────────────────────────────
async function apiCall(method, path, body = null, auth = true) {
  const headers = { 'Content-Type': 'application/json' };
  if (auth) {
    const token = localStorage.getItem('sb_token');
    if (token) headers['Authorization'] = `Bearer ${token}`;
  }
  try {
    const res = await fetch(`${API_BASE}${path}`, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });
    const data = await res.json();
    if (!res.ok) return { data: null, error: data.error || 'Request failed', status: res.status };
    return { data, error: null, status: res.status };
  } catch (err) {
    return { data: null, error: 'Network error — is the server running?', status: 0 };
  }
}

const api = {
  get:    (path)        => apiCall('GET',    path),
  post:   (path, body)  => apiCall('POST',   path, body),
  put:    (path, body)  => apiCall('PUT',    path, body),
  patch:  (path, body)  => apiCall('PATCH',  path, body),
  delete: (path)        => apiCall('DELETE', path),

  // ── AUTH ──
  auth: {
    register: (data) => apiCall('POST', '/auth/register', data, false),
    login:    (data) => apiCall('POST', '/auth/login', data, false),
    me:       ()     => apiCall('GET',  '/auth/me'),
    logout:   ()     => { localStorage.removeItem('sb_token'); localStorage.removeItem('sb_user'); },
    saveSession: (token, user) => {
      localStorage.setItem('sb_token', token);
      localStorage.setItem('sb_user', JSON.stringify(user));
    },
    currentUser: () => {
      try { return JSON.parse(localStorage.getItem('sb_user')); } catch { return null; }
    },
    isLoggedIn: () => !!localStorage.getItem('sb_token'),
  },

  // ── USERS ──
  users: {
    list:    ()    => apiCall('GET', '/users'),
    profile: (id)  => apiCall('GET', `/users/${id}`),
  },

  // ── LISTINGS ──
  listings: {
    list:         (params = {}) => {
      const qs = new URLSearchParams(params).toString();
      return apiCall('GET', `/listings${qs ? '?' + qs : ''}`);
    },
    get:          (id)   => apiCall('GET',    `/listings/${id}`),
    create:       (data) => apiCall('POST',   '/listings', data),
    update:       (id, data) => apiCall('PATCH', `/listings/${id}`, data),
    delete:       (id)   => apiCall('DELETE', `/listings/${id}`),
    suggestPrice: (category, level) => apiCall('GET', `/listings/suggest-price?category=${category}&level=${level}`),
  },

  // ── ESCROW / BOOKINGS ──
  escrow: {
    create:  (listing_id) => apiCall('POST', '/escrow', { listing_id }),
    list:    ()           => apiCall('GET',  '/escrow'),
    get:     (id)         => apiCall('GET',  `/escrow/${id}`),
    confirm: (id)         => apiCall('POST', `/escrow/${id}/confirm`),
    dispute: (id, data)   => apiCall('POST', `/escrow/${id}/dispute`, data),
  },

  // ── REVIEWS ──
  reviews: {
    create:   (data) => apiCall('POST', '/reviews', data),
    forUser:  (id)   => apiCall('GET',  `/reviews/user/${id}`),
  },

  // ── WALLET ──
  wallet: {
    summary:      ()       => apiCall('GET',  '/wallet'),
    transactions: ()       => apiCall('GET',  '/wallet/transactions'),
    buyCredits:   (amount) => apiCall('POST', '/wallet/buy', { amount }),
  },

  // ── NOTIFICATIONS ──
  notifications: {
    list:    () => apiCall('GET',  '/notifications'),
    readAll: () => apiCall('POST', '/notifications/read-all'),
  },

  // ── INVENTORY ──
  inventory: {
    list: (type = null) => apiCall('GET', `/inventory${type ? '?type=' + type : ''}`),
    use:  (id)          => apiCall('POST', `/inventory/${id}/use`),
  },

  // ── DISPUTES ──
  disputes: {
    create: (data) => apiCall('POST', '/disputes', data),
    list:   ()     => apiCall('GET',  '/disputes'),
  },

  // ── AFFILIATE ──
  affiliate: {
    stats: () => apiCall('GET', '/affiliate'),
  },

  // ── ACHIEVEMENTS ──
  achievements: {
    list: () => apiCall('GET', '/achievements'),
  },

  // ── LEADERBOARD ──
  leaderboard: {
    get: (sort = 'credits') => apiCall('GET', `/leaderboard?sort=${sort}`),
  },

  // ── VERIFICATION ──
  verify: {
    run: (type) => apiCall('POST', `/verify/${type}`),
  },

  // ── PLATFORM ──
  stats: () => apiCall('GET', '/stats'),
  health: () => apiCall('GET', '/health'),
};

// ─────────────────────────────────────────────
// DEVICE FINGERPRINT (anti-multi-account)
// ─────────────────────────────────────────────
async function getDeviceFingerprint() {
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  ctx.textBaseline = 'top';
  ctx.font = '14px Arial';
  ctx.fillText('SkillBridge-FP', 2, 2);
  const canvasData = canvas.toDataURL();

  const fp = [
    navigator.userAgent,
    navigator.language,
    screen.width + 'x' + screen.height,
    new Date().getTimezoneOffset(),
    canvasData.slice(0, 100),
  ].join('|');

  // Simple hash
  let hash = 0;
  for (let i = 0; i < fp.length; i++) {
    hash = ((hash << 5) - hash) + fp.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash).toString(36);
}

// ─────────────────────────────────────────────
// REAL-TIME SYNC (poll every 30s)
// ─────────────────────────────────────────────
let _pollInterval = null;

function startSync(onUpdate) {
  if (_pollInterval) return;
  _pollInterval = setInterval(async () => {
    if (!api.auth.isLoggedIn()) return;
    const [me, notifs] = await Promise.all([api.auth.me(), api.notifications.list()]);
    if (me.data) {
      localStorage.setItem('sb_user', JSON.stringify(me.data));
      onUpdate && onUpdate({ user: me.data, notifications: notifs.data });
    }
  }, 30000);
}

function stopSync() {
  if (_pollInterval) { clearInterval(_pollInterval); _pollInterval = null; }
}

// ─────────────────────────────────────────────
// USAGE EXAMPLES (for reference)
// ─────────────────────────────────────────────
/*

// 1. Register
const fp = await getDeviceFingerprint();
const { data, error } = await api.auth.register({
  name: 'Amara Kofi',
  email: 'amara@example.com',
  password: 'securepassword',
  country: 'Ghana',
  phone: '+233501234567',
  device_fp: fp,
  referral_code: 'SB-ABC123' // optional
});
if (data) api.auth.saveSession(data.token, data.user);

// 2. Login
const { data } = await api.auth.login({ email: 'amara@example.com', password: '...' });
if (data) api.auth.saveSession(data.token, data.user);

// 3. Browse listings
const { data } = await api.listings.list({ category: 'tech', sort: 'rating', limit: 20 });
console.log(data.listings);

// 4. Create a listing
const { data } = await api.listings.create({
  title: 'Python for data science',
  description: 'Build real data pipelines. 10+ exercises. Pandas, NumPy, visualisation. 1 hour, fully interactive.',
  category: 'tech',
  level: 'advanced',
  price_per_hour: 42,
  duration_hours: 1
});

// 5. Book a session (creates escrow)
const { data } = await api.escrow.create('listing-id-here');
// data = { escrow_id, credits_held, fee, net, inventory_id }

// 6. Confirm session complete (releases escrow)
const { data } = await api.escrow.confirm('ESC-XXXXXXXX');

// 7. Leave a review
const { data } = await api.reviews.create({
  escrow_id: 'ESC-XXXXXXXX',
  rating: 5,
  comment: 'Exceptional session. Very clear explanations.'
});

// 8. Buy credits
const { data } = await api.wallet.buyCredits(120);

// 9. Start real-time sync
startSync(({ user, notifications }) => {
  document.getElementById('credits').textContent = user.credits;
  document.getElementById('notif-count').textContent = notifications.unread;
});

*/

window.api = api;
window.getDeviceFingerprint = getDeviceFingerprint;
window.startSync = startSync;
console.log('✓ SkillBridge API client loaded. Backend:', API_BASE);

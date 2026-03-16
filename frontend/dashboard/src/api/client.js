/**
 * src/api/client.js
 * Axios wrapper for all FastAPI calls.
 * proxy in package.json routes /api → http://localhost:8000
 */

import axios from 'axios';

const api = axios.create({ baseURL: '' });

// ── Tickers ──────────────────────────────────────────────────
export const fetchTickers = () =>
  api.get('/tickers').then(r => r.data.tickers);

// ── Events ───────────────────────────────────────────────────
export const fetchEvents = (ticker, limit = 50) =>
  api.get(`/events/${ticker}`, { params: { limit } }).then(r => r.data);

// ── Event window (day-level, for charts) ─────────────────────
export const fetchWindow = (ticker, eventId = null) => {
  const params = eventId ? { event_id: eventId } : {};
  return api.get(`/events/${ticker}/window`, { params }).then(r => r.data.rows);
};

// ── Cohorts ───────────────────────────────────────────────────
export const fetchCohorts = (capBucket = null) => {
  const params = capBucket ? { cap_bucket: capBucket } : {};
  return api.get('/cohorts', { params }).then(r => r.data.rows);
};

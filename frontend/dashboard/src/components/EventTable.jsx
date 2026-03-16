/**
 * src/components/EventTable.jsx
 * Table of individual earnings events with pre/post returns.
 */

const fmt = (v, suffix = '%') => {
  if (v == null) return '—';
  const n = Number(v) * 100;
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(2)}${suffix}`;
};

const fmtRatio = (v) => {
  if (v == null) return '—';
  return `${Number(v).toFixed(2)}x`;
};

const returnColor = (v) => {
  if (v == null) return '#94a3b8';
  return Number(v) > 0 ? '#4ade80' : Number(v) < 0 ? '#f87171' : '#94a3b8';
};

const CAP_COLORS = {
  LARGE: '#3b82f6',
  MID:   '#a78bfa',
  SMALL: '#34d399',
  OTHER: '#64748b',
};

export default function EventTable({ events, ticker, onSelectEvent, selectedEventId }) {
  if (!events || events.length === 0) {
    return (
      <div style={cardStyle}>
        <h3 style={titleStyle}>Earnings Event History</h3>
        <p style={{ color: '#64748b', fontSize: 13 }}>No events found.</p>
      </div>
    );
  }

  return (
    <div style={cardStyle}>
      <h3 style={titleStyle}>
        {ticker} — Earnings Event History
        <span style={{ fontWeight: 400, fontSize: 13, color: '#64748b', marginLeft: 8 }}>
          ({events.length} events)
        </span>
      </h3>

      <div style={{ overflowX: 'auto', marginTop: 12 }}>
        <table style={tableStyle}>
          <thead>
            <tr>
              {['Date', 'Quarter', 'Cap', 'Pre 5d', 'Day 0', 'Post 5d', 'Pre Vol', 'Post Vol'].map(h => (
                <th key={h} style={thStyle}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {events.map((ev, i) => {
              const isSelected = ev.event_id === selectedEventId;
              return (
                <tr
                  key={ev.event_id}
                  style={{
                    ...trStyle(i),
                    ...(isSelected ? trSelected : {}),
                    cursor: 'pointer',
                  }}
                  onClick={() => onSelectEvent && onSelectEvent(ev.event_id)}
                >
                  <td style={tdStyle}>{ev.event_date}</td>
                  <td style={tdStyle}>{ev.fiscal_quarter ?? '—'}</td>
                  <td style={tdStyle}>
                    <span style={{
                      padding: '2px 8px',
                      borderRadius: 999,
                      fontSize: 11,
                      fontWeight: 600,
                      background: CAP_COLORS[ev.cap_bucket] + '22',
                      color: CAP_COLORS[ev.cap_bucket] ?? '#64748b',
                    }}>
                      {ev.cap_bucket ?? '—'}
                    </span>
                  </td>
                  <td style={{ ...tdStyle, color: returnColor(ev.pre_return) }}>
                    {fmt(ev.pre_return)}
                  </td>
                  <td style={{ ...tdStyle, color: returnColor(ev.day0_return), fontWeight: 700 }}>
                    {fmt(ev.day0_return)}
                  </td>
                  <td style={{ ...tdStyle, color: returnColor(ev.post_return) }}>
                    {fmt(ev.post_return)}
                  </td>
                  <td style={{ ...tdStyle, color: '#94a3b8' }}>
                    {fmtRatio(ev.avg_pre_volume_ratio)}
                  </td>
                  <td style={{ ...tdStyle, color: '#94a3b8' }}>
                    {fmtRatio(ev.avg_post_volume_ratio)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const cardStyle = {
  background: '#1e293b',
  borderRadius: 12,
  padding: '20px 24px',
  border: '1px solid #334155',
};

const titleStyle = {
  fontSize: 15,
  fontWeight: 700,
  color: '#f1f5f9',
  marginBottom: 4,
};

const tableStyle = {
  width: '100%',
  borderCollapse: 'collapse',
  fontSize: 13,
};

const thStyle = {
  padding: '8px 12px',
  textAlign: 'left',
  color: '#64748b',
  fontWeight: 600,
  fontSize: 11,
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  borderBottom: '1px solid #334155',
};

const tdStyle = {
  padding: '9px 12px',
  color: '#e2e8f0',
  borderBottom: '1px solid #1e293b',
};

const trStyle = (i) => ({
  background: i % 2 === 0 ? 'transparent' : '#0f172a22',
});

const trSelected = {
  background: '#1d4ed820',
  outline: '1px solid #1d4ed8',
};

/**
 * src/components/EventChart.jsx
 * Cumulative return chart across the [-5, +5] event window.
 * Shows average cumulative return per relative_day,
 * aggregated across all events for the selected ticker.
 */

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ReferenceLine, ResponsiveContainer, Legend,
} from 'recharts';

// Aggregate window rows → one avg cum_return per relative_day
function aggregate(rows) {
  const byDay = {};
  for (const r of rows) {
    const d = r.relative_day;
    if (!byDay[d]) byDay[d] = { sum: 0, count: 0 };
    if (r.cum_return != null) {
      byDay[d].sum   += Number(r.cum_return);
      byDay[d].count += 1;
    }
  }
  return Object.entries(byDay)
    .map(([day, { sum, count }]) => ({
      day:        Number(day),
      avg_return: count > 0 ? +(sum / count * 100).toFixed(3) : null,
    }))
    .sort((a, b) => a.day - b.day);
}

const fmt = v => (v != null ? `${v > 0 ? '+' : ''}${v.toFixed(2)}%` : '—');

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: '#1e293b', border: '1px solid #334155',
      borderRadius: 8, padding: '10px 14px', fontSize: 13,
    }}>
      <p style={{ color: '#94a3b8', marginBottom: 4 }}>
        Day {label >= 0 ? `+${label}` : label}
      </p>
      <p style={{ color: '#60a5fa', fontWeight: 600 }}>
        Avg return: {fmt(payload[0]?.value)}
      </p>
    </div>
  );
};

export default function EventChart({ rows, ticker }) {
  if (!rows || rows.length === 0) {
    return (
      <div style={cardStyle}>
        <h3 style={titleStyle}>Avg Cumulative Return Around Earnings</h3>
        <p style={{ color: '#64748b', fontSize: 13 }}>No data available.</p>
      </div>
    );
  }

  const data = aggregate(rows);

  return (
    <div style={cardStyle}>
      <h3 style={titleStyle}>
        {ticker} — Avg Cumulative Return Around Earnings
      </h3>
      <p style={{ color: '#64748b', fontSize: 12, marginBottom: 16 }}>
        Averaged across {new Set(rows.map(r => r.event_id)).size} events · relative to event date
      </p>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="day"
            stroke="#475569"
            tick={{ fill: '#94a3b8', fontSize: 12 }}
            tickFormatter={d => d >= 0 ? `+${d}` : String(d)}
            label={{ value: 'Days relative to earnings', position: 'insideBottom', offset: -2, fill: '#475569', fontSize: 11 }}
          />
          <YAxis
            stroke="#475569"
            tick={{ fill: '#94a3b8', fontSize: 12 }}
            tickFormatter={v => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`}
          />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine x={0} stroke="#f59e0b" strokeDasharray="4 4" label={{ value: 'Earnings', fill: '#f59e0b', fontSize: 11 }} />
          <ReferenceLine y={0} stroke="#334155" />
          <Line
            type="monotone"
            dataKey="avg_return"
            stroke="#3b82f6"
            strokeWidth={2.5}
            dot={{ r: 3, fill: '#3b82f6' }}
            activeDot={{ r: 5 }}
            name="Avg Cum Return (%)"
          />
        </LineChart>
      </ResponsiveContainer>
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

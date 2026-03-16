/**
 * src/components/VolumeChart.jsx
 * Average volume ratio per relative_day.
 * volume_ratio > 1 means above-average volume for that event window.
 */

import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ReferenceLine, ResponsiveContainer, Cell,
} from 'recharts';

function aggregate(rows) {
  const byDay = {};
  for (const r of rows) {
    const d = r.relative_day;
    if (!byDay[d]) byDay[d] = { sum: 0, count: 0 };
    if (r.volume_ratio != null) {
      byDay[d].sum   += Number(r.volume_ratio);
      byDay[d].count += 1;
    }
  }
  return Object.entries(byDay)
    .map(([day, { sum, count }]) => ({
      day:          Number(day),
      volume_ratio: count > 0 ? +(sum / count).toFixed(3) : null,
    }))
    .sort((a, b) => a.day - b.day);
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  const val = payload[0]?.value;
  return (
    <div style={{
      background: '#1e293b', border: '1px solid #334155',
      borderRadius: 8, padding: '10px 14px', fontSize: 13,
    }}>
      <p style={{ color: '#94a3b8', marginBottom: 4 }}>
        Day {label >= 0 ? `+${label}` : label}
      </p>
      <p style={{ color: '#a78bfa', fontWeight: 600 }}>
        Volume ratio: {val != null ? `${val.toFixed(2)}x` : '—'}
      </p>
    </div>
  );
};

export default function VolumeChart({ rows, ticker }) {
  if (!rows || rows.length === 0) {
    return (
      <div style={cardStyle}>
        <h3 style={titleStyle}>Volume Ratio Around Earnings</h3>
        <p style={{ color: '#64748b', fontSize: 13 }}>No data available.</p>
      </div>
    );
  }

  const data = aggregate(rows);

  return (
    <div style={cardStyle}>
      <h3 style={titleStyle}>
        {ticker} — Volume Ratio Around Earnings
      </h3>
      <p style={{ color: '#64748b', fontSize: 12, marginBottom: 16 }}>
        1.0 = average window volume · above 1.0 = elevated activity
      </p>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={data} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
          <XAxis
            dataKey="day"
            stroke="#475569"
            tick={{ fill: '#94a3b8', fontSize: 12 }}
            tickFormatter={d => d >= 0 ? `+${d}` : String(d)}
          />
          <YAxis
            stroke="#475569"
            tick={{ fill: '#94a3b8', fontSize: 12 }}
            tickFormatter={v => `${v.toFixed(1)}x`}
          />
          <Tooltip content={<CustomTooltip />} />
          <ReferenceLine x={0} stroke="#f59e0b" strokeDasharray="4 4" />
          <ReferenceLine y={1} stroke="#334155" />
          <Bar dataKey="volume_ratio" radius={[4, 4, 0, 0]}>
            {data.map((entry) => (
              <Cell
                key={entry.day}
                fill={entry.day === 0 ? '#f59e0b' : '#7c3aed'}
                fillOpacity={entry.volume_ratio > 1 ? 1 : 0.5}
              />
            ))}
          </Bar>
        </BarChart>
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

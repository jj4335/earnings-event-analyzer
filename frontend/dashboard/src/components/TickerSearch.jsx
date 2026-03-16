/**
 * src/components/TickerSearch.jsx
 * Ticker input + autocomplete from /tickers endpoint.
 */

import { useState, useEffect } from 'react';
import { fetchTickers } from '../api/client';

const styles = {
  wrapper: {
    display: 'flex',
    flexDirection: 'column',
    gap: '12px',
    maxWidth: '480px',
  },
  label: {
    fontSize: '13px',
    color: '#94a3b8',
    fontWeight: 600,
    letterSpacing: '0.05em',
    textTransform: 'uppercase',
  },
  row: {
    display: 'flex',
    gap: '8px',
  },
  input: {
    flex: 1,
    padding: '10px 14px',
    background: '#1e293b',
    border: '1.5px solid #334155',
    borderRadius: '8px',
    color: '#f1f5f9',
    fontSize: '15px',
    outline: 'none',
  },
  button: {
    padding: '10px 20px',
    background: '#1d4ed8',
    border: 'none',
    borderRadius: '8px',
    color: '#fff',
    fontSize: '14px',
    fontWeight: 600,
    cursor: 'pointer',
  },
  chips: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: '6px',
  },
  chip: {
    padding: '4px 10px',
    background: '#1e293b',
    border: '1px solid #334155',
    borderRadius: '999px',
    fontSize: '12px',
    color: '#94a3b8',
    cursor: 'pointer',
  },
  chipActive: {
    background: '#1d4ed8',
    border: '1px solid #1d4ed8',
    color: '#fff',
  },
};

export default function TickerSearch({ onSearch, activeTicker }) {
  const [input, setInput]     = useState('');
  const [tickers, setTickers] = useState([]);

  useEffect(() => {
    fetchTickers()
      .then(setTickers)
      .catch(() => setTickers([]));
  }, []);

  const submit = (t) => {
    const val = (t || input).trim().toUpperCase();
    if (val) onSearch(val);
  };

  return (
    <div style={styles.wrapper}>
      <span style={styles.label}>Ticker Symbol</span>
      <div style={styles.row}>
        <input
          style={styles.input}
          value={input}
          placeholder="e.g. AAPL"
          onChange={e => setInput(e.target.value.toUpperCase())}
          onKeyDown={e => e.key === 'Enter' && submit()}
        />
        <button style={styles.button} onClick={() => submit()}>
          Analyze
        </button>
      </div>

      {/* Quick-select chips from /tickers */}
      {tickers.length > 0 && (
        <div style={styles.chips}>
          {tickers.slice(0, 12).map(t => (
            <span
              key={t.ticker}
              style={{
                ...styles.chip,
                ...(activeTicker === t.ticker ? styles.chipActive : {}),
              }}
              onClick={() => { setInput(t.ticker); submit(t.ticker); }}
            >
              {t.ticker}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

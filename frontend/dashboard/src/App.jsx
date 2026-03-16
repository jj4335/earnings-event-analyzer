/**
 * src/App.jsx
 * Root component. Orchestrates ticker search → data fetch → chart rendering.
 */

import { useState, useCallback } from 'react';
import TickerSearch from './components/TickerSearch';
import EventChart   from './components/EventChart';
import VolumeChart  from './components/VolumeChart';
import EventTable   from './components/EventTable';
import { fetchEvents, fetchWindow } from './api/client';

const styles = {
  app: {
    minHeight: '100vh',
    background: '#0f172a',
    color: '#e2e8f0',
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
  },
  header: {
    padding: '24px 32px',
    borderBottom: '1px solid #1e293b',
    display: 'flex',
    alignItems: 'center',
    gap: 16,
  },
  logo: {
    fontSize: 22,
    fontWeight: 800,
    color: '#f1f5f9',
    letterSpacing: '-0.02em',
  },
  subtitle: {
    fontSize: 13,
    color: '#475569',
  },
  main: {
    maxWidth: 1100,
    margin: '0 auto',
    padding: '32px 24px',
    display: 'flex',
    flexDirection: 'column',
    gap: 28,
  },
  metaBar: {
    display: 'flex',
    alignItems: 'center',
    gap: 12,
    padding: '14px 20px',
    background: '#1e293b',
    borderRadius: 10,
    border: '1px solid #334155',
  },
  metaLabel: { fontSize: 12, color: '#64748b', fontWeight: 600 },
  metaValue: { fontSize: 14, color: '#f1f5f9', fontWeight: 700 },
  chartsGrid: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: 20,
  },
  error: {
    padding: '16px 20px',
    background: '#7f1d1d22',
    border: '1px solid #991b1b',
    borderRadius: 10,
    color: '#fca5a5',
    fontSize: 14,
  },
  loading: {
    color: '#475569',
    fontSize: 14,
    padding: '40px 0',
    textAlign: 'center',
  },
};

export default function App() {
  const [ticker,          setTicker]          = useState(null);
  const [eventsData,      setEventsData]      = useState(null);
  const [windowRows,      setWindowRows]      = useState([]);
  const [selectedEventId, setSelectedEventId] = useState(null);
  const [loading,         setLoading]         = useState(false);
  const [error,           setError]           = useState(null);

  const handleSearch = useCallback(async (t) => {
    setTicker(t);
    setEventsData(null);
    setWindowRows([]);
    setSelectedEventId(null);
    setError(null);
    setLoading(true);

    try {
      const [evData, wRows] = await Promise.all([
        fetchEvents(t),
        fetchWindow(t),
      ]);
      setEventsData(evData);
      setWindowRows(wRows);
    } catch (err) {
      const msg = err.response?.data?.detail ?? err.message ?? 'Unknown error';
      setError(msg);
    } finally {
      setLoading(false);
    }
  }, []);

  // When a table row is clicked, filter charts to that event
  const handleSelectEvent = useCallback(async (eventId) => {
    if (selectedEventId === eventId) {
      // Deselect — show all events
      setSelectedEventId(null);
      const rows = await fetchWindow(ticker);
      setWindowRows(rows);
    } else {
      setSelectedEventId(eventId);
      const rows = await fetchWindow(ticker, eventId);
      setWindowRows(rows);
    }
  }, [ticker, selectedEventId]);

  const filteredRows = selectedEventId
    ? windowRows.filter(r => r.event_id === selectedEventId)
    : windowRows;

  return (
    <div style={styles.app}>
      {/* Header */}
      <header style={styles.header}>
        <div>
          <div style={styles.logo}>📈 Earnings Event Analyzer</div>
          <div style={styles.subtitle}>
            PySpark-powered event study · Enter a ticker to explore earnings reactions
          </div>
        </div>
      </header>

      <main style={styles.main}>
        {/* Search */}
        <TickerSearch onSearch={handleSearch} activeTicker={ticker} />

        {/* Loading */}
        {loading && <div style={styles.loading}>Loading {ticker}…</div>}

        {/* Error */}
        {error && <div style={styles.error}>⚠️ {error}</div>}

        {/* Results */}
        {eventsData && !loading && (
          <>
            {/* Meta bar */}
            <div style={styles.metaBar}>
              <span style={styles.metaLabel}>TICKER</span>
              <span style={{ ...styles.metaValue, color: '#60a5fa' }}>{eventsData.ticker}</span>
              <span style={{ color: '#334155' }}>·</span>
              <span style={styles.metaLabel}>CAP</span>
              <span style={styles.metaValue}>{eventsData.cap_bucket ?? '—'}</span>
              <span style={{ color: '#334155' }}>·</span>
              <span style={styles.metaLabel}>EVENTS</span>
              <span style={styles.metaValue}>{eventsData.event_count}</span>
              {selectedEventId && (
                <>
                  <span style={{ color: '#334155' }}>·</span>
                  <span style={{ fontSize: 12, color: '#f59e0b' }}>
                    Showing single event — click row again to deselect
                  </span>
                </>
              )}
            </div>

            {/* Charts */}
            <div style={styles.chartsGrid}>
              <EventChart  rows={filteredRows} ticker={eventsData.ticker} />
              <VolumeChart rows={filteredRows} ticker={eventsData.ticker} />
            </div>

            {/* Table */}
            <EventTable
              events={eventsData.events}
              ticker={eventsData.ticker}
              onSelectEvent={handleSelectEvent}
              selectedEventId={selectedEventId}
            />
          </>
        )}
      </main>
    </div>
  );
}

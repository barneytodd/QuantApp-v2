import React, { useState } from "react";
import styles from "./IngestionTab.module.css";
import { api } from "../../../api/axios";


interface IngestRequest {
  symbols: string[];
  start: string;
  end: string;
  interval: string;
  max_attempts: number;
  max_concurrent: number;
  coverage_threshold: number;
  dry_run: boolean;
}

interface SymbolResult {
  symbol: string;
  success: boolean;
  attempts: number;
  rows_inserted: number;
  elapsed_ms: number;
  error?: string;
}

export function IngestionTab() {
  const [symbols, setSymbols] = useState("AAPL,MSFT,GOOG");
  const [start, setStart] = useState("2023-01-01");
  const [end, setEnd] = useState("2023-12-31");
  const [interval, setInterval] = useState("1d");
  const [maxAttempts, setMaxAttempts] = useState(3);
  const [maxConcurrent, setMaxConcurrent] = useState(5);
  const [coverageThreshold, setCoverageThreshold] = useState(0.95);
  const [dryRun, setDryRun] = useState(false);

  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState<SymbolResult[]>([]);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResults([]);

    const payload: IngestRequest = {
      symbols: symbols.split(",").map((s) => s.trim().toUpperCase()),
      start,
      end,
      interval,
      max_attempts: maxAttempts,
      max_concurrent: maxConcurrent,
      coverage_threshold: coverageThreshold,
      dry_run: dryRun,
    };

    try {
      const resp = await api.post("/market-data/ingest", payload);
      setResults(resp.data.results);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className={styles.container}>
      <form className={styles.form} onSubmit={handleSubmit}>
        <label>
          Symbols (comma separated)
          <input value={symbols} onChange={(e) => setSymbols(e.target.value)} />
        </label>

        <label>
          Start Date
          <input type="date" value={start} onChange={(e) => setStart(e.target.value)} />
        </label>

        <label>
          End Date
          <input type="date" value={end} onChange={(e) => setEnd(e.target.value)} />
        </label>

        <label>
          Interval
          <select value={interval} onChange={(e) => setInterval(e.target.value)}>
            <option value="1d">1d</option>
            <option value="1wk">1wk</option>
            <option value="1mo">1mo</option>
          </select>
        </label>

        <label>
          Max Attempts
          <input
            type="number"
            value={maxAttempts}
            onChange={(e) => setMaxAttempts(parseInt(e.target.value))}
          />
        </label>

        <label>
          Max Concurrent
          <input
            type="number"
            value={maxConcurrent}
            onChange={(e) => setMaxConcurrent(parseInt(e.target.value))}
          />
        </label>

        <label>
          Coverage Threshold
          <input
            type="number"
            step="0.01"
            value={coverageThreshold}
            onChange={(e) => setCoverageThreshold(parseFloat(e.target.value))}
          />
        </label>

        <label>
          Dry Run
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
          />
        </label>

        <button type="submit" disabled={loading}>
          {loading ? "Ingesting..." : "Start Ingestion"}
        </button>
      </form>

      {error && <div className={styles.error}>Error: {error}</div>}

      {results.length > 0 && (
        <table className={styles.table}>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Success</th>
              <th>Attempts</th>
              <th>Rows Inserted</th>
              <th>Elapsed ms</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {results.map((r) => (
              <tr key={r.symbol}>
                <td>{r.symbol}</td>
                <td>{r.success ? "✅" : "❌"}</td>
                <td>{r.attempts}</td>
                <td>{r.rows_inserted}</td>
                <td>{r.elapsed_ms}</td>
                <td>{r.error || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

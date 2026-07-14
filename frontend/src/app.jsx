import React from "react";
import { createRoot } from "react-dom/client";

const COLS = [
  { key: "sym",  label: "Symbol",  group: "info",      frozen: true,  align: "left" },
  { key: "lot",  label: "Lot",     group: "info" },
  { key: "mgn",  label: "Margin",  group: "info" },
  { key: "chg",  label: "Charges", group: "info" },
  { key: "coc",  label: "Interest",group: "info" },
  { key: "nLtp", label: "Near",    group: "ltp",       cellClass: "ltp-cell" },
  { key: "xLtp", label: "Next",    group: "ltp",       cellClass: "ltp-cell" },
  { key: "fLtp", label: "Far",     group: "ltp",       cellClass: "ltp-cell" },
  { key: "lsNX", label: "LTP Spread", exp: "NearNext_LTP_Spread", group: "near-next", spread: true },
  { key: "sNX",  label: "Bid",        exp: "NearNext_Bid",        group: "near-next", spread: true },
  { key: "sXN",  label: "Ask",        exp: "NearNext_Ask",        group: "near-next", spread: true },
  { key: "rNX",  label: "R%",         exp: "NearNext_R%",         group: "near-next", pct: true },
  { key: "pNX",  label: "LTP %ile",   exp: "NearNext_LTP_%ile",   group: "near-next", pctile: true },
  { key: "lsXF", label: "LTP Spread", exp: "NextFar_LTP_Spread",  group: "next-far",  spread: true },
  { key: "sXF",  label: "Bid",        exp: "NextFar_Bid",         group: "next-far",  spread: true },
  { key: "sFX",  label: "Ask",        exp: "NextFar_Ask",         group: "next-far",  spread: true },
  { key: "rXF",  label: "R%",         exp: "NextFar_R%",          group: "next-far",  pct: true },
  { key: "pXF",  label: "LTP %ile",   exp: "NextFar_LTP_%ile",    group: "next-far",  pctile: true },
  { key: "lsNF", label: "LTP Spread", exp: "NearFar_LTP_Spread",  group: "near-far",  spread: true },
  { key: "sNF",  label: "Bid",        exp: "NearFar_Bid",         group: "near-far",  spread: true },
  { key: "sFN",  label: "Ask",        exp: "NearFar_Ask",         group: "near-far",  spread: true },
  { key: "rNF",  label: "R%",         exp: "NearFar_R%",          group: "near-far",  pct: true },
  { key: "pNF",  label: "LTP %ile",   exp: "NearFar_LTP_%ile",    group: "near-far",  pctile: true },
];

// return-% columns used by the "min spread %" filter
const SPREAD_PCT_KEYS = ["rNX", "rXF", "rNF"];

const SCROLL_COLS = COLS.filter(c => !c.frozen);

// Flip to true to show the spread-chart button column (left of Symbol).
const SHOW_CHART_COL = true;

const GROUP_META = {
  "info":      { label: "Info",      span: 4, cls: "col-group-info" },
  "ltp":       { label: "Last price",span: 3, cls: "col-group-ltp" },
  "near-next": { label: "Near ↔ Next", span: 5, cls: "col-group-near-next" },
  "next-far":  { label: "Next ↔ Far",  span: 5, cls: "col-group-next-far" },
  "near-far":  { label: "Near ↔ Far",  span: 5, cls: "col-group-near-far" },
};

// Ordered group list for the column-visibility menu (Symbol stays frozen/always on).
const GROUP_ORDER = ["info", "ltp", "near-next", "next-far", "near-far"];

function fmt(v) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  if (isNaN(n)) return v;
  if (Math.abs(n) >= 1000) return n.toLocaleString("en-IN", { maximumFractionDigits: 2 });
  return n.toFixed(2);
}

// Percentile cells render as a whole-number percent, e.g. 95 → "95%".
function fmtCell(col, v) {
  if (col.pctile) return v == null || v === "" ? "—" : `${Math.round(Number(v))}%`;
  return fmt(v);
}

// localStorage-backed state: same shape as useState, persisted under `key`.
function usePersisted(key, initial) {
  const [v, setV] = React.useState(() => {
    try { const s = localStorage.getItem(key); return s != null ? JSON.parse(s) : initial; }
    catch { return initial; }
  });
  React.useEffect(() => {
    try { localStorage.setItem(key, JSON.stringify(v)); } catch { /* quota / private mode */ }
  }, [key, v]);
  return [v, setV];
}

function cellClass(col, v) {
  if (col.cellClass) return col.cellClass;
  if (v == null || v === "") return "";
  const n = Number(v);
  if (isNaN(n)) return "";
  // Percentile: flag the extremes — wide (≥90) and narrow (≤10) — as trade signals.
  if (col.pctile) return "pctile-cell" + (n >= 90 ? " pctile-hi" : n <= 10 ? " pctile-lo" : "");
  if (col.spread) return n > 0 ? "spread-pos" : n < 0 ? "spread-neg" : "";
  if (col.pct) return n > 0 ? "pct-pos" : n < 0 ? "pct-neg" : "";
  return "";
}

function exportCSV(data) {
  const headers = COLS.map(c => c.exp || c.label).join(",");
  const rows = data.map(r => COLS.map(c => r[c.key] ?? "").join(","));
  const csv = [headers, ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `spreads_${new Date().toISOString().slice(0,19).replace(/[T:]/g,"-")}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function SearchIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>
    </svg>
  );
}

function ChartIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 3v18h18"/><path d="m7 14 4-4 3 3 5-6"/>
    </svg>
  );
}

function CloseIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <path d="M18 6 6 18M6 6l12 12"/>
    </svg>
  );
}

// ── DATE / TICK HELPERS ──────────────────────────────────────────────
const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
function parseDate(s) { return Date.parse(s + "T00:00:00Z"); }
function dayOfMonth(s) { return Number(s.slice(8, 10)); }               // "2025-07-15" → 15
function ym(s) { return s ? s.slice(0, 7) : null; }                     // "2025-07-15" → "2025-07"
function fmtFull(s) {
  if (!s) return "—";
  const d = new Date(parseDate(s));
  return `${String(d.getUTCDate()).padStart(2,"0")}-${MONTHS[d.getUTCMonth()]}-${d.getUTCFullYear()}`;
}
function monthLabel(ymStr) {                                            // "2025-07" → "Jul 2025"
  if (!ymStr) return "—";
  const [y, m] = ymStr.split("-");
  return `${MONTHS[Number(m) - 1]} ${y}`;
}
function monthChip(ymStr) {                                             // "2025-07" → "Jul '25"
  if (!ymStr) return "—";
  const [y, m] = ymStr.split("-");
  return `${MONTHS[Number(m) - 1]} '${y.slice(2)}`;
}
function niceNum(range, round) {
  const exp = Math.floor(Math.log10(range || 1));
  const f = range / Math.pow(10, exp);
  let nf;
  if (round) nf = f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10;
  else       nf = f <= 1 ? 1 : f <= 2 ? 2 : f <= 5 ? 5 : 10;
  return nf * Math.pow(10, exp);
}
function niceTicks(min, max, count) {
  if (min === max) { min -= 1; max += 1; }
  const step = niceNum(niceNum(max - min, false) / (count - 1), true);
  const lo = Math.floor(min / step) * step;
  const out = [];
  for (let v = lo; v <= max + step * 0.5; v += step) out.push(Number(v.toFixed(6)));
  return out;
}

const SPREAD_OPTS = [
  { key: "nx", label: "Near-Next" },
  { key: "xf", label: "Next-Far" },
  { key: "nf", label: "Near-Far" },
];
// How many of the most-recent expiries to display (Infinity = all).
const EXPIRY_OPTS = [
  { label: "Last 3", value: 3 },
  { label: "Last 6", value: 6 },
  { label: "Last 12", value: 12 },
  { label: "All", value: Infinity },
];
// High-contrast, dark-mode-friendly palette. Ordered so adjacent expiry months
// alternate warm/cool hues — never two similar shades side by side.
const CYCLE_COLORS = [
  "#38bdf8", // sky
  "#f97316", // orange
  "#22c55e", // green
  "#e879f9", // magenta
  "#facc15", // yellow
  "#60a5fa", // blue
  "#fb7185", // rose
  "#2dd4bf", // teal
  "#a78bfa", // violet
  "#a3e635", // lime
  "#f472b6", // pink
  "#34d399", // emerald
  "#fbbf24", // amber
  "#22d3ee", // cyan
  "#c084fc", // purple
  "#fca5a5", // salmon
];
const AVG_KEY = "__avg__";
const AVG_COLOR = "#e5e7eb";   // light gray — historical average line

// ── INTERACTIVE SPREAD CHART ─────────────────────────────────────────
// X-axis is normalized to day-of-month (1–31). Every expiry cycle is drawn as
// its own line, all overlaid on the same 1–31 axis so seasonal patterns line up.
const X_MIN = 1, X_MAX = 31;

function SpreadChart({ rows, spreadKey, expiryLimit = Infinity }) {
  const wrapRef = React.useRef(null);
  const svgRef = React.useRef(null);
  const dragRef = React.useRef(null);
  const clickTimer = React.useRef(null);
  const [width, setWidth] = React.useState(1000);
  const [vh, setVh] = React.useState(typeof window !== "undefined" ? window.innerHeight : 900);
  const [view, setView] = React.useState({ min: X_MIN, max: X_MAX });   // {min,max} in day units
  const [hover, setHover] = React.useState(null);
  const [hidden, setHidden] = React.useState(() => new Set());          // hidden series keys
  const [legendHi, setLegendHi] = React.useState(null);                 // key highlighted via legend

  React.useLayoutEffect(() => {
    const el = wrapRef.current; if (!el) return;
    const ro = new ResizeObserver(es => { for (const e of es) setWidth(e.contentRect.width); });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  React.useEffect(() => {
    const h = () => setVh(window.innerHeight);
    window.addEventListener("resize", h);
    return () => window.removeEventListener("resize", h);
  }, []);

  // Everything scales with `k` so the chart grows proportionally (no distortion).
  const BASE_W = 1000;
  const k = Math.max(0.85, Math.min(width / BASE_W, 1.7));
  const height = Math.round(Math.min(width * 0.46, Math.max(300, vh * 0.92 - 220 * k)));

  // One line per contract month, keyed by near expiry. Also computes per-cycle
  // stats (max/min/avg), tags its extreme points, assigns a stable color, and
  // stores a back-ref on each point so the tooltip/highlight can read it cheaply.
  const cycles = React.useMemo(() => {
    const map = new Map();
    for (const r of rows) {
      const v = r[spreadKey];
      if (v == null || !r.nearExp || !r.date) continue;
      if (ym(r.date) !== ym(r.nearExp)) continue;
      let c = map.get(r.nearExp) || { key: r.nearExp, month: ym(r.nearExp), pts: [] };
      c.pts.push({ x: dayOfMonth(r.date), v, row: r });
      map.set(r.nearExp, c);
    }
    const out = [...map.values()];
    out.sort((a, b) => a.key.localeCompare(b.key));       // chronological → stable colors
    out.forEach((c, i) => {
      c.pts.sort((a, b) => a.x - b.x);
      c.color = CYCLE_COLORS[i % CYCLE_COLORS.length];
      let mx = -Infinity, mn = Infinity, sum = 0, mxi = 0, mni = 0;
      c.pts.forEach((p, j) => {
        sum += p.v;
        if (p.v > mx) { mx = p.v; mxi = j; }
        if (p.v < mn) { mn = p.v; mni = j; }
        p.cycle = c;
      });
      c.max = mx; c.min = mn; c.avg = sum / c.pts.length;
      c.pts[mxi].isMax = true; c.pts[mni].isMin = true;
      c.maxPt = c.pts[mxi]; c.minPt = c.pts[mni];
    });
    return out;
  }, [rows, spreadKey]);

  // Restrict to the N most-recent expiries (newest = last, chronologically).
  // Colors were assigned on the full list above, so they stay stable per expiry.
  const limitedCycles = React.useMemo(
    () => (expiryLimit >= cycles.length ? cycles : cycles.slice(cycles.length - expiryLimit)),
    [cycles, expiryLimit]);

  // Historical average across the *displayed* expiries for each trading day (1–31).
  const avgSeries = React.useMemo(() => {
    const sum = new Array(32).fill(0), cnt = new Array(32).fill(0);
    for (const c of limitedCycles) for (const p of c.pts) { sum[p.x] += p.v; cnt[p.x]++; }
    const s = { key: AVG_KEY, month: "AVG", color: AVG_COLOR, isAvg: true, pts: [] };
    let mx = -Infinity, mn = Infinity, tot = 0;
    for (let d = 1; d <= 31; d++) if (cnt[d] > 0) {
      const v = sum[d] / cnt[d];
      const p = { x: d, v, cycle: s };
      s.pts.push(p);
      if (v > mx) mx = v; if (v < mn) mn = v; tot += v;
    }
    s.max = mx; s.min = mn; s.avg = s.pts.length ? tot / s.pts.length : 0;
    return s;
  }, [limitedCycles]);

  // "Today" = day-of-month of the most recent trading date in the data.
  const todayDay = React.useMemo(() => {
    let last = null;
    for (const r of rows) if (r.date && (!last || r.date > last)) last = r.date;
    return last ? dayOfMonth(last) : null;
  }, [rows]);

  // reset transient view/filter state whenever the symbol changes
  React.useEffect(() => {
    setView({ min: X_MIN, max: X_MAX });
    setHidden(new Set());
  }, [rows]);

  const isVisible = key => !hidden.has(key);
  const drawnCycles = limitedCycles.filter(c => isVisible(c.key));
  const drawAvg = isVisible(AVG_KEY) && avgSeries.pts.length > 0;
  const drawn = drawAvg ? [...drawnCycles, avgSeries] : drawnCycles;
  const allPts = React.useMemo(() => drawn.flatMap(s => s.pts), [drawn]);

  const yDomain = React.useMemo(() => {
    if (!allPts.length) return [-1, 1];
    let mn = Infinity, mx = -Infinity;
    for (const p of allPts) { if (p.v < mn) mn = p.v; if (p.v > mx) mx = p.v; }
    mn = Math.min(mn, 0); mx = Math.max(mx, 0);            // always include 0
    const pad = (mx - mn) * 0.08 || 1;
    return [mn - pad, mx + pad];
  }, [allPts]);

  const padL = 56 * k, padR = 20 * k, padT = 18 * k, padB = 44 * k;
  const plotW = Math.max(10, width - padL - padR);
  const plotH = height - padT - padB;
  const vmin = view.min, vmax = view.max;
  const xS = d => padL + (d - vmin) / ((vmax - vmin) || 1) * plotW;
  const yS = v => padT + (yDomain[1] - v) / ((yDomain[1] - yDomain[0]) || 1) * plotH;

  // which series is emphasised: legend hover wins, else the hovered point's series
  const activeKey = legendHi || (hover && hover.cycle.key) || null;

  // Native non-passive wheel listener so preventDefault works (React's onWheel is passive).
  const wheelState = React.useRef();
  wheelState.current = { vmin, vmax, plotW, padL, setView };
  React.useEffect(() => {
    const svg = svgRef.current; if (!svg) return;
    const handler = e => {
      e.preventDefault();
      const s = wheelState.current;
      const rect = svg.getBoundingClientRect();
      const t = s.vmin + (e.clientX - rect.left - s.padL) / s.plotW * (s.vmax - s.vmin);
      const factor = e.deltaY < 0 ? 0.82 : 1.22;
      let nmin = t - (t - s.vmin) * factor;
      let nmax = t + (s.vmax - t) * factor;
      if (nmin < X_MIN) nmin = X_MIN;
      if (nmax > X_MAX) nmax = X_MAX;
      if (nmax - nmin < 2) return;                        // don't zoom past ~2 days
      s.setView({ min: nmin, max: nmax });
    };
    svg.addEventListener("wheel", handler, { passive: false });
    return () => svg.removeEventListener("wheel", handler);
  }, []);

  const onPointerDown = e => {
    dragRef.current = { x: e.clientX, min: vmin, max: vmax };
    svgRef.current.setPointerCapture(e.pointerId);
    setHover(null);
  };
  const onPointerMove = e => {
    if (dragRef.current) {
      const d = dragRef.current;
      const dt = (e.clientX - d.x) / plotW * (d.max - d.min);
      const span = d.max - d.min;
      let nmin = d.min - dt, nmax = d.max - dt;
      if (nmin < X_MIN) { nmin = X_MIN; nmax = nmin + span; }
      if (nmax > X_MAX) { nmax = X_MAX; nmin = nmax - span; }
      setView({ min: nmin, max: nmax });
      return;
    }
    // nearest point in 2-D so overlapping cycles can be inspected individually
    const rect = svgRef.current.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    if (mx < padL - 2 || mx > padL + plotW + 2) { setHover(null); return; }
    let best = null, bd = Infinity;
    for (const p of allPts) {
      const px = xS(p.x); if (px < padL - 1 || px > padL + plotW + 1) continue;
      const py = yS(p.v);
      const d = (px - mx) * (px - mx) + (py - my) * (py - my);
      if (d < bd) { bd = d; best = p; }
    }
    setHover(best && bd < (32 * k) * (32 * k) ? best : null);
  };
  const onPointerUp = () => { dragRef.current = null; };

  // ── legend interactions ──
  const toggleKey = key => setHidden(h => {
    const n = new Set(h); n.has(key) ? n.delete(key) : n.add(key); return n;
  });
  const isolateKey = key => setHidden(() => {
    const keys = limitedCycles.map(c => c.key);          // only the displayed expiries
    const onlyThis = keys.every(kk => kk === key || hidden.has(kk)) && !hidden.has(key);
    if (onlyThis) return new Set();                       // already isolated → restore all
    return new Set(keys.filter(kk => kk !== key));        // hide other months (avg stays)
  });
  const onLegendClick = key => {
    if (clickTimer.current) return;                       // 2nd click of a dbl-click: ignore
    clickTimer.current = setTimeout(() => { clickTimer.current = null; toggleKey(key); }, 220);
  };
  const onLegendDbl = key => {
    if (clickTimer.current) { clearTimeout(clickTimer.current); clickTimer.current = null; }
    isolateKey(key);
  };

  const yTicks = niceTicks(yDomain[0], yDomain[1], 6).filter(t => t >= yDomain[0] && t <= yDomain[1]);
  const xStep = Math.max(1, Math.ceil((vmax - vmin) / 12));
  const xTicks = [];
  for (let d = Math.ceil(vmin); d <= Math.floor(vmax); d++) if ((d - 1) % xStep === 0) xTicks.push(d);
  const zeroY = yS(0);
  const showZero = 0 >= yDomain[0] && 0 <= yDomain[1];
  const mk = 4.5 * k;                                      // extreme-marker half-size

  const hasData = cycles.length > 0;

  const legendItem = (key, month, color, isAvg) => (
    <span key={key}
          className={"legend-item" + (hidden.has(key) ? " off" : "") + (activeKey === key ? " hi" : "")}
          onClick={() => onLegendClick(key)}
          onDoubleClick={() => onLegendDbl(key)}
          onMouseEnter={() => setLegendHi(key)}
          onMouseLeave={() => setLegendHi(null)}
          title="Click to toggle · double-click to isolate">
      <i className={isAvg ? "sw-avg" : ""}
         style={isAvg ? { width: 14 * k, borderTopColor: color, borderTopWidth: 2 * k }
                      : { background: color, width: 11 * k, height: 3 * k }} />
      {isAvg ? "Historical Avg" : monthChip(month)}
    </span>
  );

  return (
    <div className="chart-wrap" ref={wrapRef}>
      {/* legend: avg first, then displayed contract months (click=toggle, dbl-click=isolate) */}
      <div className="chart-legend" style={{ fontSize: 10.5 * k }}>
        {legendItem(AVG_KEY, "AVG", AVG_COLOR, true)}
        {limitedCycles.map(c => legendItem(c.key, c.month, c.color, false))}
      </div>

      {!hasData ? (
        <div className="chart-empty">No data available for this spread.</div>
      ) : (
      <svg
        ref={svgRef} width={width} height={height}
        style={{ touchAction: "none", cursor: dragRef.current ? "grabbing" : "crosshair" }}
        onPointerDown={onPointerDown} onPointerMove={onPointerMove}
        onPointerUp={onPointerUp} onPointerLeave={() => { onPointerUp(); setHover(null); }}
      >
        <defs>
          <clipPath id="plotclip"><rect x={padL} y={padT} width={plotW} height={plotH} /></clipPath>
        </defs>

        {/* y grid + labels */}
        {yTicks.map((t, i) => (
          <g key={"y" + i}>
            <line x1={padL} y1={yS(t)} x2={padL + plotW} y2={yS(t)} className="grid-line" />
            <text x={padL - 8 * k} y={yS(t) + 3 * k} className="axis-label" fontSize={10 * k} textAnchor="end">{t.toFixed(2)}</text>
          </g>
        ))}
        {/* x grid + labels (day of month) */}
        {xTicks.map(d => (
          <g key={"x" + d}>
            <line x1={xS(d)} y1={padT} x2={xS(d)} y2={padT + plotH} className="grid-line" />
            <text x={xS(d)} y={padT + plotH + 16 * k} className="axis-label" fontSize={10 * k} textAnchor="middle">{d}</text>
          </g>
        ))}
        <text x={padL + plotW / 2} y={height - 6 * k} className="axis-title" fontSize={10 * k} textAnchor="middle">Trading day of month</text>

        {/* clipped plot content */}
        <g clipPath="url(#plotclip)">
          {showZero && <line x1={padL} y1={zeroY} x2={padL + plotW} y2={zeroY} className="zero-line" strokeWidth={1.4 * k} />}

          {/* Today marker */}
          {todayDay != null && todayDay >= vmin && todayDay <= vmax && (
            <g>
              <line x1={xS(todayDay)} y1={padT} x2={xS(todayDay)} y2={padT + plotH} className="today-line" strokeWidth={1.3 * k} />
              <rect x={xS(todayDay) - 20 * k} y={padT} width={40 * k} height={14 * k} rx={3 * k} className="today-tag" />
              <text x={xS(todayDay)} y={padT + 10 * k} className="today-label" fontSize={9 * k} textAnchor="middle">Today</text>
            </g>
          )}

          {/* expiry lines */}
          {drawnCycles.map(c => {
            const faded = activeKey && activeKey !== c.key;
            const active = activeKey === c.key;
            const pts = c.pts.map(p => `${xS(p.x).toFixed(1)},${yS(p.v).toFixed(1)}`).join(" ");
            return (
              <polyline key={c.key} points={pts} fill="none" stroke={c.color}
                        strokeWidth={(active ? 2.6 : 1.6) * k} strokeOpacity={faded ? 0.22 : 1}
                        strokeLinejoin="round" strokeLinecap="round" />
            );
          })}

          {/* historical-average line (thick, light gray, dashed) */}
          {drawAvg && (() => {
            const faded = activeKey && activeKey !== AVG_KEY;
            const active = activeKey === AVG_KEY;
            const pts = avgSeries.pts.map(p => `${xS(p.x).toFixed(1)},${yS(p.v).toFixed(1)}`).join(" ");
            return (
              <polyline points={pts} fill="none" stroke={AVG_COLOR}
                        strokeWidth={(active ? 3.4 : 2.6) * k} strokeOpacity={faded ? 0.28 : 0.95}
                        strokeDasharray={`${7 * k} ${5 * k}`} strokeLinejoin="round" strokeLinecap="round" />
            );
          })()}

          {/* high / low markers per expiry (subtle triangles) */}
          {drawnCycles.map(c => {
            if (activeKey && activeKey !== c.key) return null;   // only show for active / all-normal
            const hi = c.maxPt, lo = c.minPt;
            const up = `${xS(hi.x)},${yS(hi.v) - mk - 2 * k} ${xS(hi.x) - mk},${yS(hi.v) - 2 * k} ${xS(hi.x) + mk},${yS(hi.v) - 2 * k}`;
            const dn = `${xS(lo.x)},${yS(lo.v) + mk + 2 * k} ${xS(lo.x) - mk},${yS(lo.v) + 2 * k} ${xS(lo.x) + mk},${yS(lo.v) + 2 * k}`;
            return (
              <g key={"m" + c.key} opacity={activeKey ? 1 : 0.7}>
                <polygon points={up} fill={c.color} />
                <polygon points={dn} fill={c.color} />
              </g>
            );
          })}

          {hover && <line x1={xS(hover.x)} y1={padT} x2={xS(hover.x)} y2={padT + plotH} className="hover-line" strokeWidth={k} />}
          {hover && <circle cx={xS(hover.x)} cy={yS(hover.v)} r={4 * k} fill={hover.cycle.color} className="hover-dot" strokeWidth={1.5 * k} />}
        </g>

        {showZero && <text x={padL + plotW} y={zeroY - 4 * k} className="zero-label" fontSize={10 * k} textAnchor="end">0</text>}
      </svg>
      )}

      {hover && (() => {
        const px = xS(hover.x), py = yS(hover.v);
        const tipW = 210 * k;
        const left = px > padL + plotW * 0.55 ? px - tipW - 8 * k : px + 16 * k;
        const top = Math.max(padT, Math.min(py - 20 * k, height - 176 * k));
        const c = hover.cycle;
        const badge = hover.isMax ? "▲ Highest Spread" : hover.isMin ? "▼ Lowest Spread" : null;
        return (
          <div className="chart-tip" style={{ left, top, fontSize: 11 * k, minWidth: 186 * k }}>
            <div className="tip-head">
              <span className="tip-swatch" style={{ background: c.color }} />
              {c.isAvg ? "Historical Average" : monthLabel(ym(c.key))}
            </div>
            {badge && <div className={"tip-badge " + (hover.isMax ? "bg-hi" : "bg-lo")}>{badge}</div>}
            {!c.isAvg && <div className="tip-row"><span>Trade Date</span><b>{fmtFull(hover.row.date)}</b></div>}
            <div className="tip-row"><span>Trading Day</span><b>{hover.x}</b></div>
            <div className="tip-row tip-spread">
              <span>Spread</span>
              <b className={hover.v > 0 ? "spread-pos" : hover.v < 0 ? "spread-neg" : ""}>{fmt(hover.v)}</b>
            </div>
            <div className="tip-row"><span>Highest</span><b className="spread-pos">{fmt(c.max)}</b></div>
            <div className="tip-row"><span>Lowest</span><b className="spread-neg">{fmt(c.min)}</b></div>
            <div className="tip-row"><span>Average</span><b>{fmt(c.avg)}</b></div>
          </div>
        );
      })()}
    </div>
  );
}

function ChartModal({ symbol, onClose }) {
  const [rows, setRows] = React.useState(null);
  const [err, setErr] = React.useState(null);
  const [spreadKey, setSpreadKey] = usePersisted("f2f.modal.spreadKey", "nx");
  const [expiryLimit, setExpiryLimit] = usePersisted("f2f.modal.expiryLimit", 6);   // how many recent expiries to show
  const [resetNonce, setResetNonce] = React.useState(0);   // bump → chart remounts → zoom resets

  React.useEffect(() => {
    let active = true;
    setRows(null); setErr(null);
    fetch(`/api/history/${encodeURIComponent(symbol)}`)
      .then(r => r.json())
      .then(j => { if (active) setRows(j.rows || []); })
      .catch(e => { if (active) setErr(String(e)); });
    return () => { active = false; };
  }, [symbol]);

  React.useEffect(() => {
    const h = e => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [onClose]);

  return (
    <div className="modal-backdrop" onMouseDown={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal">
        <div className="modal-head">
          <div className="modal-title">
            <ChartIcon />
            <span className="modal-sym">{symbol}</span>
            <span className="modal-sub">Futures spread history</span>
          </div>
          <div className="modal-controls">
            <div className="seg" title="Spread type">
              {SPREAD_OPTS.map(o => (
                <button key={o.key}
                        className={"seg-btn" + (spreadKey === o.key ? " active" : "")}
                        onClick={() => setSpreadKey(o.key)}>{o.label}</button>
              ))}
            </div>
            <div className="seg" title="Expiries shown">
              {EXPIRY_OPTS.map(o => (
                <button key={o.label}
                        className={"seg-btn" + (expiryLimit === o.value ? " active" : "")}
                        onClick={() => setExpiryLimit(o.value)}>{o.label}</button>
              ))}
            </div>
            <button className="btn-ghost" onClick={() => setResetNonce(n => n + 1)}>Reset zoom</button>
            <button className="btn-icon" onClick={onClose} title="Close (Esc)"><CloseIcon /></button>
          </div>
        </div>
        <div className="modal-body">
          {err && <div className="chart-empty">Failed to load: {err}</div>}
          {!err && rows === null && <div className="chart-empty">Loading…</div>}
          {!err && rows !== null && <SpreadChart key={resetNonce} rows={rows} spreadKey={spreadKey} expiryLimit={expiryLimit} />}
        </div>
        <div className="modal-foot">
          <span>Scroll to zoom · drag to pan · hover for details</span>
          <span>{rows ? rows.length : 0} trading days</span>
        </div>
      </div>
    </div>
  );
}

// Dropdown to show/hide whole column groups. `hiddenGroups` is the persisted
// list of group keys currently hidden; `onToggle(key)` flips one.
function ColumnsMenu({ hiddenGroups, onToggle }) {
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  React.useEffect(() => {
    if (!open) return;
    const h = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    window.addEventListener("mousedown", h);
    return () => window.removeEventListener("mousedown", h);
  }, [open]);
  const hiddenCount = hiddenGroups.length;
  return (
    <div className="cols-menu" ref={ref}>
      <button className={"btn-export" + (open ? " active" : "")} onClick={() => setOpen(o => !o)}>
        ▦ Columns{hiddenCount ? ` (${hiddenCount} hidden)` : ""}
      </button>
      {open && (
        <div className="cols-dropdown">
          {GROUP_ORDER.map(g => (
            <label key={g} className="cols-item">
              <input type="checkbox"
                     checked={!hiddenGroups.includes(g)}
                     onChange={() => onToggle(g)} />
              <span>{GROUP_META[g].label}</span>
            </label>
          ))}
        </div>
      )}
    </div>
  );
}

function App() {
  const [data, setData] = React.useState([]);
  const [meta, setMeta] = React.useState({ ts: "", live: 0 });
  const [search, setSearch] = usePersisted("f2f.search", "");
  const [sortKey, setSortKey] = usePersisted("f2f.sortKey", null);
  const [sortDir, setSortDir] = usePersisted("f2f.sortDir", 1);
  const [chartSym, setChartSym] = React.useState(null);
  // watchlist + filters + column visibility (all persisted)
  const [watchlist, setWatchlist] = usePersisted("f2f.watchlist", []);
  const [watchOnly, setWatchOnly] = usePersisted("f2f.watchOnly", false);
  const [minSpread, setMinSpread] = usePersisted("f2f.minSpread", "");
  const [hiddenGroups, setHiddenGroups] = usePersisted("f2f.hiddenGroups", []);
  const scrollRef = React.useRef(null);
  const frozenBodyRef = React.useRef(null);

  const watchSet = React.useMemo(() => new Set(watchlist), [watchlist]);
  const toggleWatch = (sym) =>
    setWatchlist(w => (w.includes(sym) ? w.filter(s => s !== sym) : [...w, sym]));
  const toggleGroup = (g) =>
    setHiddenGroups(h => (h.includes(g) ? h.filter(x => x !== g) : [...h, g]));

  // Columns / groups actually rendered in the scroll area (Symbol stays frozen).
  const visibleScrollCols = React.useMemo(
    () => SCROLL_COLS.filter(c => !hiddenGroups.includes(c.group)), [hiddenGroups]);

  // Frozen column is wider only when the chart button column is shown.
  React.useLayoutEffect(() => {
    document.documentElement.style.setProperty("--frozen-width", SHOW_CHART_COL ? "184px" : "150px");
  }, []);

  // Fetch data
  React.useEffect(() => {
    let active = true;
    const poll = async () => {
      try {
        const res = await fetch("/api/spreads");
        const j = await res.json();
        if (active) {
          setData(j.data || []);
          setMeta({ ts: j.ts, live: j.live });
        }
      } catch (e) { console.error("Fetch error:", e); }
    };
    poll();
    const id = setInterval(poll, 500);
    return () => { active = false; clearInterval(id); };
  }, []);

  // Sync frozen column scroll
  React.useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const handler = () => {
      if (frozenBodyRef.current) {
        frozenBodyRef.current.style.transform = `translateY(-${el.scrollTop}px)`;
      }
    };
    el.addEventListener("scroll", handler, { passive: true });
    return () => el.removeEventListener("scroll", handler);
  }, []);

  // Keyboard shortcut
  React.useEffect(() => {
    const handler = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") { e.preventDefault(); document.querySelector(".search-input")?.focus(); }
      if ((e.metaKey || e.ctrlKey) && e.key === "e") { e.preventDefault(); exportCSV(filtered); }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [data, search, sortKey, sortDir]);

  // Filter + sort
  const filtered = React.useMemo(() => {
    let d = data;
    if (search) {
      const q = search.toUpperCase();
      d = d.filter(r => r.sym && r.sym.toUpperCase().includes(q));
    }
    if (watchOnly) d = d.filter(r => watchSet.has(r.sym));
    const t = parseFloat(minSpread);
    if (!isNaN(t)) {
      const thr = Math.abs(t);
      d = d.filter(r => SPREAD_PCT_KEYS.some(k => r[k] != null && Math.abs(r[k]) >= thr));
    }
    if (sortKey) {
      d = [...d].sort((a, b) => {
        let av = a[sortKey], bv = b[sortKey];
        if (av == null) return 1; if (bv == null) return -1;
        if (typeof av === "string") return av.localeCompare(bv) * sortDir;
        return (av - bv) * sortDir;
      });
    }
    return d;
  }, [data, search, sortKey, sortDir, watchOnly, watchSet, minSpread]);

  const handleSort = (key) => {
    if (sortKey === key) { setSortDir(d => d * -1); }
    else { setSortKey(key); setSortDir(1); }
  };

  const sortArrow = (key) => {
    if (sortKey !== key) return null;
    return <span className="sort-arrow">{sortDir === 1 ? "▲" : "▼"}</span>;
  };

  // Build group header row (for scrollable area)
  const scrollGroups = [];
  let lastGroup = null;
  for (const col of visibleScrollCols) {
    if (col.group !== lastGroup) {
      scrollGroups.push({ ...GROUP_META[col.group], group: col.group, span: 0 });
      lastGroup = col.group;
    }
    scrollGroups[scrollGroups.length - 1].span++;
  }

  return (
    <>
      {/* ── TOP BAR ── */}
      <div className="topbar">
        <div className="topbar-dot" />
        <div className="topbar-brand">F2F Spread Terminal</div>
        <div className="topbar-stats">
          <div><span>{meta.live}</span> instruments</div>
          <div><span>{filtered.length}</span> / {data.length} symbols</div>
          <div>Updated <span>{meta.ts || "—"}</span></div>
        </div>
        <div className="topbar-right">
          <button
            className={"btn-export" + (watchOnly ? " active" : "")}
            title="Show only watchlisted symbols"
            onClick={() => setWatchOnly(v => !v)}>
            ★ Watchlist{watchlist.length ? ` (${watchlist.length})` : ""}
          </button>
          <div className="minspread-wrap" title="Only rows where any spread % is at least this">
            <span>|%|≥</span>
            <input
              className="minspread-input"
              type="number" step="0.1" min="0" placeholder="0"
              value={minSpread}
              onChange={e => setMinSpread(e.target.value)}
            />
          </div>
          <ColumnsMenu hiddenGroups={hiddenGroups} onToggle={toggleGroup} />
          <div className="search-wrap">
            <SearchIcon />
            <input
              className="search-input"
              placeholder="Search symbol..."
              value={search}
              onChange={e => setSearch(e.target.value)}
            />
          </div>
          <button className="btn-export" onClick={() => exportCSV(filtered)}>
            ↓ Export CSV
          </button>
        </div>
      </div>

      {/* ── TABLE ── */}
      <div className="table-viewport">
        {/* Frozen Chart + Symbol columns */}
        <div className="frozen-col">
          <table>
            <thead>
              <tr className="col-groups"><th className="col-group-info" colSpan={SHOW_CHART_COL ? 2 : 1}>&nbsp;</th></tr>
              <tr>
                {SHOW_CHART_COL && <th className="chart-col-head">&nbsp;</th>}
                <th onClick={() => handleSort("sym")} className={sortKey === "sym" ? "sorted" : ""}>
                  Symbol{sortArrow("sym")}
                </th>
              </tr>
            </thead>
            <tbody ref={frozenBodyRef}>
              {filtered.map((r, i) => (
                <tr key={r.sym || i}>
                  {SHOW_CHART_COL && (
                    <td className="chart-cell">
                      <button className="chart-btn" title={`Spread chart · ${r.sym}`}
                              onClick={() => setChartSym(r.sym)}>
                        <ChartIcon />
                      </button>
                    </td>
                  )}
                  <td className="sym-cell">
                    <button
                      className={"star-btn" + (watchSet.has(r.sym) ? " on" : "")}
                      title={watchSet.has(r.sym) ? "Remove from watchlist" : "Add to watchlist"}
                      onClick={() => toggleWatch(r.sym)}>★</button>
                    <span className="sym-text">{r.sym || "—"}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Scrollable area */}
        <div className="table-scroll" ref={scrollRef}>
          <table>
            <thead>
              <tr className="col-groups">
                {scrollGroups.map((g, i) => (
                  <th key={i} colSpan={g.span} className={g.cls}>{g.label}</th>
                ))}
              </tr>
              <tr>
                {visibleScrollCols.map(col => (
                  <th key={col.key} onClick={() => handleSort(col.key)} className={sortKey === col.key ? "sorted" : ""}>
                    {col.label}{sortArrow(col.key)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={r.sym || i}>
                  {visibleScrollCols.map(col => (
                    <td key={col.key} className={cellClass(col, r[col.key])} style={col.align ? {textAlign: col.align} : undefined}>
                      {fmtCell(col, r[col.key])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* ── BOTTOM BAR ── */}
      <div className="bottombar">
        <div>
          <kbd>Ctrl</kbd>+<kbd>K</kbd> Search&ensp;
          <kbd>Ctrl</kbd>+<kbd>E</kbd> Export&ensp;
          Click headers to sort
        </div>
        <div>500ms refresh • {data.length} rows</div>
      </div>

      {chartSym && <ChartModal symbol={chartSym} onClose={() => setChartSym(null)} />}
    </>
  );
}

createRoot(document.getElementById("root")).render(<App />);

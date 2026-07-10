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
  { key: "sNX",  label: "N→X",     group: "near-next", spread: true },
  { key: "sNXp", label: "%",       group: "near-next", pct: true },
  { key: "sXN",  label: "X→N",     group: "near-next", spread: true },
  { key: "sXNp", label: "%",       group: "near-next", pct: true },
  { key: "sXF",  label: "X→F",     group: "next-far",  spread: true },
  { key: "sXFp", label: "%",       group: "next-far",  pct: true },
  { key: "sFX",  label: "F→X",     group: "next-far",  spread: true },
  { key: "sFXp", label: "%",       group: "next-far",  pct: true },
  { key: "sNF",  label: "N→F",     group: "near-far",  spread: true },
  { key: "sNFp", label: "%",       group: "near-far",  pct: true },
  { key: "sFN",  label: "F→N",     group: "near-far",  spread: true },
  { key: "sFNp", label: "%",       group: "near-far",  pct: true },
];

const SCROLL_COLS = COLS.filter(c => !c.frozen);

// Flip to true to show the spread-chart button column (left of Symbol).
const SHOW_CHART_COL = true;

const GROUP_META = {
  "info":      { label: "Info",      span: 4, cls: "col-group-info" },
  "ltp":       { label: "Last price",span: 3, cls: "col-group-ltp" },
  "near-next": { label: "Near ↔ Next", span: 4, cls: "col-group-near-next" },
  "next-far":  { label: "Next ↔ Far",  span: 4, cls: "col-group-next-far" },
  "near-far":  { label: "Near ↔ Far",  span: 4, cls: "col-group-near-far" },
};

function fmt(v) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  if (isNaN(n)) return v;
  if (Math.abs(n) >= 1000) return n.toLocaleString("en-IN", { maximumFractionDigits: 2 });
  return n.toFixed(2);
}

function cellClass(col, v) {
  if (col.cellClass) return col.cellClass;
  if (v == null || v === "") return "";
  const n = Number(v);
  if (isNaN(n)) return "";
  if (col.spread) return n > 0 ? "spread-pos" : n < 0 ? "spread-neg" : "";
  if (col.pct) return n > 0 ? "pct-pos" : n < 0 ? "pct-neg" : "";
  return "";
}

function exportCSV(data) {
  const headers = COLS.map(c => c.label).join(",");
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
// distinct color per monthly cycle line
const CYCLE_COLORS = ["#38bdf8","#a855f7","#22c55e","#fbbf24","#f472b6","#22d3ee",
                      "#f97316","#818cf8","#4ade80","#e879f9","#facc15","#2dd4bf"];

// ── INTERACTIVE SPREAD CHART ─────────────────────────────────────────
// X-axis is normalized to day-of-month (1–31). Every expiry cycle is drawn as
// its own line, all overlaid on the same 1–31 axis so seasonal patterns line up.
const X_MIN = 1, X_MAX = 31;

function SpreadChart({ rows, spreadKey }) {
  const wrapRef = React.useRef(null);
  const svgRef = React.useRef(null);
  const dragRef = React.useRef(null);
  const [width, setWidth] = React.useState(880);
  const height = 430;
  const [view, setView] = React.useState({ min: X_MIN, max: X_MAX });   // {min,max} in day units
  const [hover, setHover] = React.useState(null);

  React.useLayoutEffect(() => {
    const el = wrapRef.current; if (!el) return;
    const ro = new ResizeObserver(es => { for (const e of es) setWidth(e.contentRect.width); });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // One line per contract month. A cycle = the near contract's own expiry month:
  // keep only trade dates whose calendar month equals the near-expiry month
  // (drops the 1–2 post-roll days that would otherwise wrap the line back).
  const cycles = React.useMemo(() => {
    const map = new Map();                       // "YYYY-MM" → { key, pts }
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
    out.forEach(c => c.pts.sort((a, b) => a.x - b.x));
    return out;
  }, [rows, spreadKey]);

  const allPts = React.useMemo(() => cycles.flatMap(c => c.pts), [cycles]);

  const yDomain = React.useMemo(() => {
    if (!allPts.length) return [-1, 1];
    let mn = Infinity, mx = -Infinity;
    for (const p of allPts) { if (p.v < mn) mn = p.v; if (p.v > mx) mx = p.v; }
    mn = Math.min(mn, 0); mx = Math.max(mx, 0);            // always include 0
    const pad = (mx - mn) * 0.08 || 1;
    return [mn - pad, mx + pad];
  }, [allPts]);

  // reset view to full 1–31 whenever the symbol (rows) changes
  React.useEffect(() => { setView({ min: X_MIN, max: X_MAX }); }, [rows]);

  const padL = 56, padR = 20, padT = 18, padB = 44;
  const plotW = Math.max(10, width - padL - padR);
  const plotH = height - padT - padB;
  const vmin = view.min, vmax = view.max;
  const xS = d => padL + (d - vmin) / ((vmax - vmin) || 1) * plotW;
  const yS = v => padT + (yDomain[1] - v) / ((yDomain[1] - yDomain[0]) || 1) * plotH;

  // Reset zoom remounts this component (ChartModal bumps its key), so no local reset state.

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
    setHover(best && bd < 32 * 32 ? best : null);
  };
  const onPointerUp = () => { dragRef.current = null; };

  const yTicks = niceTicks(yDomain[0], yDomain[1], 6).filter(t => t >= yDomain[0] && t <= yDomain[1]);
  const xStep = Math.max(1, Math.ceil((vmax - vmin) / 12));
  const xTicks = [];
  for (let d = Math.ceil(vmin); d <= Math.floor(vmax); d++) if ((d - 1) % xStep === 0) xTicks.push(d);
  const zeroY = yS(0);
  const showZero = 0 >= yDomain[0] && 0 <= yDomain[1];

  if (!allPts.length) {
    return <div className="chart-empty">No data available for this spread.</div>;
  }

  return (
    <div className="chart-wrap" ref={wrapRef}>
      {/* cycle legend (contract months) */}
      <div className="chart-legend">
        {cycles.map((c, ci) => (
          <span key={c.key} className="legend-item">
            <i style={{ background: CYCLE_COLORS[ci % CYCLE_COLORS.length] }} />
            {monthChip(c.month)}
          </span>
        ))}
      </div>

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
            <text x={padL - 8} y={yS(t) + 3} className="axis-label" textAnchor="end">{t.toFixed(2)}</text>
          </g>
        ))}
        {/* x grid + labels (day of month) */}
        {xTicks.map(d => (
          <g key={"x" + d}>
            <line x1={xS(d)} y1={padT} x2={xS(d)} y2={padT + plotH} className="grid-line" />
            <text x={xS(d)} y={padT + plotH + 16} className="axis-label" textAnchor="middle">{d}</text>
          </g>
        ))}
        <text x={padL + plotW / 2} y={height - 6} className="axis-title" textAnchor="middle">Trading day of month</text>

        {/* clipped plot content */}
        <g clipPath="url(#plotclip)">
          {showZero && <line x1={padL} y1={zeroY} x2={padL + plotW} y2={zeroY} className="zero-line" />}
          {cycles.map((c, ci) => {
            const color = CYCLE_COLORS[ci % CYCLE_COLORS.length];
            const dim = hover && hover.row.nearExp !== c.key;
            const pts = c.pts.map(p => `${xS(p.x).toFixed(1)},${yS(p.v).toFixed(1)}`).join(" ");
            return (
              <polyline key={c.key} points={pts} fill="none" stroke={color}
                        strokeWidth={dim ? 1 : 1.6} strokeOpacity={dim ? 0.35 : 1}
                        strokeLinejoin="round" strokeLinecap="round" />
            );
          })}
          {hover && <line x1={xS(hover.x)} y1={padT} x2={xS(hover.x)} y2={padT + plotH} className="hover-line" />}
          {hover && <circle cx={xS(hover.x)} cy={yS(hover.v)} r="4" className="hover-dot" />}
        </g>

        {showZero && <text x={padL + plotW} y={zeroY - 4} className="zero-label" textAnchor="end">0</text>}
      </svg>

      {hover && (() => {
        const px = xS(hover.x), py = yS(hover.v);
        const left = px > padL + plotW * 0.6 ? px - 200 : px + 16;
        const top = Math.max(padT, Math.min(py - 20, height - 168));
        const r = hover.row;
        return (
          <div className="chart-tip" style={{ left, top }}>
            <div className="tip-date">{fmtFull(r.date)}</div>
            <div className="tip-row"><span>Contract Month</span><b>{monthLabel(ym(r.nearExp))}</b></div>
            <div className="tip-row"><span>Near Expiry</span><b>{fmtFull(r.nearExp)}</b></div>
            <div className="tip-row"><span>Next Expiry</span><b>{fmtFull(r.nextExp)}</b></div>
            <div className="tip-row"><span>Near Close</span><b>{fmt(r.nearClose)}</b></div>
            <div className="tip-row"><span>Next Close</span><b>{fmt(r.nextClose)}</b></div>
            <div className="tip-row tip-spread">
              <span>Spread</span>
              <b className={hover.v > 0 ? "spread-pos" : hover.v < 0 ? "spread-neg" : ""}>{fmt(hover.v)}</b>
            </div>
          </div>
        );
      })()}
    </div>
  );
}

function ChartModal({ symbol, onClose }) {
  const [rows, setRows] = React.useState(null);
  const [err, setErr] = React.useState(null);
  const [spreadKey, setSpreadKey] = React.useState("nx");
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
            <div className="seg">
              {SPREAD_OPTS.map(o => (
                <button key={o.key}
                        className={"seg-btn" + (spreadKey === o.key ? " active" : "")}
                        onClick={() => setSpreadKey(o.key)}>{o.label}</button>
              ))}
            </div>
            <button className="btn-ghost" onClick={() => setResetNonce(n => n + 1)}>Reset zoom</button>
            <button className="btn-icon" onClick={onClose} title="Close (Esc)"><CloseIcon /></button>
          </div>
        </div>
        <div className="modal-body">
          {err && <div className="chart-empty">Failed to load: {err}</div>}
          {!err && rows === null && <div className="chart-empty">Loading…</div>}
          {!err && rows !== null && <SpreadChart key={resetNonce} rows={rows} spreadKey={spreadKey} />}
        </div>
        <div className="modal-foot">
          <span>Scroll to zoom · drag to pan · hover for details</span>
          <span>{rows ? rows.length : 0} trading days</span>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [data, setData] = React.useState([]);
  const [meta, setMeta] = React.useState({ ts: "", live: 0 });
  const [search, setSearch] = React.useState("");
  const [sortKey, setSortKey] = React.useState(null);
  const [sortDir, setSortDir] = React.useState(1);
  const [chartSym, setChartSym] = React.useState(null);
  const scrollRef = React.useRef(null);
  const frozenBodyRef = React.useRef(null);

  // Frozen column is wider only when the chart button column is shown.
  React.useLayoutEffect(() => {
    document.documentElement.style.setProperty("--frozen-width", SHOW_CHART_COL ? "168px" : "130px");
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
    if (sortKey) {
      d = [...d].sort((a, b) => {
        let av = a[sortKey], bv = b[sortKey];
        if (av == null) return 1; if (bv == null) return -1;
        if (typeof av === "string") return av.localeCompare(bv) * sortDir;
        return (av - bv) * sortDir;
      });
    }
    return d;
  }, [data, search, sortKey, sortDir]);

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
  for (const col of SCROLL_COLS) {
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
                  <td className="sym-cell">{r.sym || "—"}</td>
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
                {SCROLL_COLS.map(col => (
                  <th key={col.key} onClick={() => handleSort(col.key)} className={sortKey === col.key ? "sorted" : ""}>
                    {col.label}{sortArrow(col.key)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={r.sym || i}>
                  {SCROLL_COLS.map(col => (
                    <td key={col.key} className={cellClass(col, r[col.key])} style={col.align ? {textAlign: col.align} : undefined}>
                      {fmt(r[col.key])}
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

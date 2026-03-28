import { useState, useMemo, useRef, useEffect } from "react";

const COLS = [
  { key: "sym", label: "Symbol", group: "info", frozen: true, align: "left" },
  { key: "lot", label: "Lot", group: "info" },
  { key: "mgn", label: "Margin", group: "info" },
  { key: "chg", label: "Charges", group: "info" },
  { key: "coc", label: "Interest", group: "info" },
  { key: "nLtp", label: "Near", group: "ltp", cellClass: "ltp" },
  { key: "xLtp", label: "Next", group: "ltp", cellClass: "ltp" },
  { key: "fLtp", label: "Far", group: "ltp", cellClass: "ltp" },
  { key: "sNX", label: "N→X", group: "near-next", spread: true },
  { key: "sNXp", label: "%", group: "near-next", pct: true },
  { key: "sXN", label: "X→N", group: "near-next", spread: true },
  { key: "sXNp", label: "%", group: "near-next", pct: true },
  { key: "sXF", label: "X→F", group: "next-far", spread: true },
  { key: "sXFp", label: "%", group: "next-far", pct: true },
  { key: "sFX", label: "F→X", group: "next-far", spread: true },
  { key: "sFXp", label: "%", group: "next-far", pct: true },
  { key: "sNF", label: "N→F", group: "near-far", spread: true },
  { key: "sNFp", label: "%", group: "near-far", pct: true },
  { key: "sFN", label: "F→N", group: "near-far", spread: true },
  { key: "sFNp", label: "%", group: "near-far", pct: true },
];

const SCROLL_COLS = COLS.filter((c) => !c.frozen);

const GROUP_META = {
  info: { label: "Info", cls: "bg-slate-900/80" },
  ltp: { label: "Last price", cls: "bg-sky-500/5" },
  "near-next": { label: "Near ↔ Next", cls: "bg-purple-500/5" },
  "next-far": { label: "Next ↔ Far", cls: "bg-amber-500/5" },
  "near-far": { label: "Near ↔ Far", cls: "bg-cyan-500/5" },
};

const MOCK = [
  { sym: "RELIANCE", lot: 250, mgn: 85420, chg: 42.5, coc: 18.3, nLtp: 2503.6, xLtp: 2517.9, fLtp: 2530.8, sNX: 14.3, sNXp: 0.57, sXN: -14.3, sXNp: -0.57, sXF: 12.9, sXFp: 0.52, sFX: -12.9, sFXp: -0.52, sNF: 27.2, sNFp: 1.09, sFN: -27.2, sFNp: -1.09 },
  { sym: "TCS", lot: 175, mgn: 62300, chg: 38.2, coc: 15.1, nLtp: 3842.5, xLtp: 3868.0, fLtp: 3891.5, sNX: 25.5, sNXp: 0.66, sXN: -25.5, sXNp: -0.66, sXF: 23.5, sXFp: 0.61, sFX: -23.5, sFXp: -0.61, sNF: 49.0, sNFp: 1.28, sFN: -49.0, sFNp: -1.28 },
  { sym: "INFY", lot: 400, mgn: 54200, chg: 28.4, coc: 12.6, nLtp: 1586.3, xLtp: 1594.8, fLtp: 1603.1, sNX: 8.5, sNXp: 0.54, sXN: -8.5, sXNp: -0.54, sXF: 8.3, sXFp: 0.52, sFX: -8.3, sFXp: -0.52, sNF: 16.8, sNFp: 1.06, sFN: -16.8, sFNp: -1.06 },
  { sym: "HDFCBANK", lot: 550, mgn: 78900, chg: 35.6, coc: 16.8, nLtp: 1712.4, xLtp: 1724.6, fLtp: 1738.2, sNX: 12.2, sNXp: 0.71, sXN: -12.2, sXNp: -0.71, sXF: 13.6, sXFp: 0.79, sFX: -13.6, sFXp: -0.79, sNF: 25.8, sNFp: 1.51, sFN: -25.8, sFNp: -1.51 },
  { sym: "ICICIBANK", lot: 700, mgn: 67100, chg: 31.2, coc: 14.5, nLtp: 1245.8, xLtp: 1254.2, fLtp: 1261.9, sNX: 8.4, sNXp: 0.67, sXN: -8.4, sXNp: -0.67, sXF: 7.7, sXFp: 0.62, sFX: -7.7, sFXp: -0.62, sNF: 16.1, sNFp: 1.29, sFN: -16.1, sFNp: -1.29 },
  { sym: "SBIN", lot: 1500, mgn: 52400, chg: 24.8, coc: 11.2, nLtp: 810.5, xLtp: 815.3, fLtp: 820.8, sNX: 4.8, sNXp: 0.59, sXN: -4.8, sXNp: -0.59, sXF: 5.5, sXFp: 0.68, sFX: -5.5, sFXp: -0.68, sNF: 10.3, sNFp: 1.27, sFN: -10.3, sFNp: -1.27 },
  { sym: "TATAMOTORS", lot: 1400, mgn: 48600, chg: 22.1, coc: 10.4, nLtp: 682.3, xLtp: 686.9, fLtp: 691.4, sNX: 4.6, sNXp: 0.67, sXN: -4.6, sXNp: -0.67, sXF: 4.5, sXFp: 0.66, sFX: -4.5, sFXp: -0.66, sNF: 9.1, sNFp: 1.33, sFN: -9.1, sFNp: -1.33 },
  { sym: "WIPRO", lot: 1500, mgn: 41200, chg: 19.6, coc: 8.8, nLtp: 452.1, xLtp: 455.8, fLtp: 459.2, sNX: 3.7, sNXp: 0.82, sXN: -3.7, sXNp: -0.82, sXF: 3.4, sXFp: 0.75, sFX: -3.4, sFXp: -0.75, sNF: 7.1, sNFp: 1.57, sFN: -7.1, sFNp: -1.57 },
  { sym: "ADANIENT", lot: 500, mgn: 91200, chg: 48.3, coc: 21.5, nLtp: 2506.0, xLtp: 2518.4, fLtp: 2531.2, sNX: 12.4, sNXp: 0.49, sXN: -12.4, sXNp: -0.49, sXF: 12.8, sXFp: 0.51, sFX: -12.8, sFXp: -0.51, sNF: 25.2, sNFp: 1.01, sFN: -25.2, sFNp: -1.01 },
  { sym: "BAJFINANCE", lot: 125, mgn: 112400, chg: 56.8, coc: 24.2, nLtp: 9245.0, xLtp: 9302.5, fLtp: 9358.0, sNX: 57.5, sNXp: 0.62, sXN: -57.5, sXNp: -0.62, sXF: 55.5, sXFp: 0.60, sFX: -55.5, sFXp: -0.60, sNF: 113.0, sNFp: 1.22, sFN: -113.0, sFNp: -1.22 },
  { sym: "KOTAKBANK", lot: 400, mgn: 72100, chg: 33.4, coc: 15.9, nLtp: 1932.6, xLtp: 1945.8, fLtp: 1958.1, sNX: 13.2, sNXp: 0.68, sXN: -13.2, sXNp: -0.68, sXF: 12.3, sXFp: 0.64, sFX: -12.3, sFXp: -0.64, sNF: 25.5, sNFp: 1.32, sFN: -25.5, sFNp: -1.32 },
  { sym: "MARUTI", lot: 100, mgn: 98500, chg: 52.1, coc: 22.8, nLtp: 11842.0, xLtp: 11918.0, fLtp: 11990.0, sNX: 76.0, sNXp: 0.64, sXN: -76.0, sXNp: -0.64, sXF: 72.0, sXFp: 0.61, sFX: -72.0, sFXp: -0.61, sNF: 148.0, sNFp: 1.25, sFN: -148.0, sFNp: -1.25 },
];

function fmt(v) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  if (isNaN(n)) return v;
  if (Math.abs(n) >= 1000) return n.toLocaleString("en-IN", { maximumFractionDigits: 2 });
  return n.toFixed(2);
}

function cClass(col, v) {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (isNaN(n)) return "";
  if (col.spread) return n > 0 ? "bg-emerald-950/80 text-emerald-400" : n < 0 ? "bg-red-950/60 text-red-400" : "";
  if (col.pct) return n > 0 ? "text-emerald-400" : n < 0 ? "text-red-400" : "";
  if (col.cellClass === "ltp") return "text-amber-400";
  return "";
}

export default function App() {
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState(null);
  const [sortDir, setSortDir] = useState(1);
  const scrollRef = useRef(null);
  const frozenRef = useRef(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const h = () => { if (frozenRef.current) frozenRef.current.style.transform = `translateY(-${el.scrollTop}px)`; };
    el.addEventListener("scroll", h, { passive: true });
    return () => el.removeEventListener("scroll", h);
  }, []);

  const filtered = useMemo(() => {
    let d = MOCK;
    if (search) { const q = search.toUpperCase(); d = d.filter((r) => r.sym.includes(q)); }
    if (sortKey) d = [...d].sort((a, b) => { let av = a[sortKey], bv = b[sortKey]; if (av == null) return 1; if (bv == null) return -1; return (typeof av === "string" ? av.localeCompare(bv) : av - bv) * sortDir; });
    return d;
  }, [search, sortKey, sortDir]);

  const handleSort = (k) => { if (sortKey === k) setSortDir((d) => d * -1); else { setSortKey(k); setSortDir(1); } };
  const arrow = (k) => sortKey === k ? <span className="text-[9px] ml-0.5 opacity-80">{sortDir === 1 ? "▲" : "▼"}</span> : null;

  const scrollGroups = [];
  let last = null;
  for (const c of SCROLL_COLS) { if (c.group !== last) { scrollGroups.push({ ...GROUP_META[c.group], span: 0 }); last = c.group; } scrollGroups[scrollGroups.length - 1].span++; }

  return (
    <div className="h-screen flex flex-col" style={{ background: "#0a0e17", fontFamily: "'JetBrains Mono', monospace", fontSize: 12, color: "#e2e8f0", WebkitFontSmoothing: "antialiased" }}>

      {/* TOP BAR */}
      <div className="flex items-center gap-4 px-5 py-3 border-b" style={{ background: "#111827", borderColor: "#162032" }}>
        <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse flex-shrink-0" />
        <div className="text-[15px] font-semibold tracking-tight text-sky-400 whitespace-nowrap" style={{ fontFamily: "'DM Sans', sans-serif" }}>F2F Spread Terminal</div>
        <div className="flex gap-5 text-[11px]" style={{ color: "#64748b" }}>
          <div><span className="text-slate-400 font-medium">620</span> instruments</div>
          <div><span className="text-slate-400 font-medium">{filtered.length}</span> / {MOCK.length} symbols</div>
          <div>Updated <span className="text-slate-400 font-medium">14:32:08</span></div>
        </div>
        <div className="ml-auto flex items-center gap-2.5">
          <div className="relative w-52">
            <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#64748b" strokeWidth="2"><circle cx="11" cy="11" r="8" /><path d="m21 21-4.3-4.3" /></svg>
            <input className="w-full py-1.5 pl-8 pr-3 rounded-md text-xs outline-none" style={{ background: "#1a2236", border: "1px solid #162032", color: "#e2e8f0", fontFamily: "inherit" }} placeholder="Search symbol..." value={search} onChange={(e) => setSearch(e.target.value)} />
          </div>
          <button className="px-3 py-1.5 rounded-md text-[11px] cursor-pointer whitespace-nowrap transition-colors" style={{ background: "#1a2236", border: "1px solid #162032", color: "#94a3b8", fontFamily: "inherit" }} onMouseEnter={e => { e.target.style.background = "#243044"; e.target.style.color = "#e2e8f0"; }} onMouseLeave={e => { e.target.style.background = "#1a2236"; e.target.style.color = "#94a3b8"; }}>↓ Export CSV</button>
        </div>
      </div>

      {/* TABLE */}
      <div className="flex-1 relative overflow-hidden">
        {/* Frozen */}
        <div className="absolute left-0 top-0 z-30 overflow-hidden pointer-events-none" style={{ width: 130, background: "#0a0e17", borderRight: "1px solid #1e3a5f" }}>
          <table className="w-full" style={{ borderCollapse: "separate", borderSpacing: 0 }}>
            <thead className="sticky top-0 z-20">
              <tr><th className="h-[26px] text-[9px] tracking-widest font-medium" style={{ background: "#111827", color: "#64748b", borderBottom: "1px solid #162032", borderRight: "1px solid #1e3a5f" }}>&nbsp;</th></tr>
              <tr><th className="h-10 px-2.5 text-[10px] uppercase tracking-wider font-medium cursor-pointer text-left" style={{ background: "#111827", color: sortKey === "sym" ? "#38bdf8" : "#64748b", borderBottom: "2px solid #1e3a5f", borderRight: "1px solid #1e3a5f" }} onClick={() => handleSort("sym")}>Symbol{arrow("sym")}</th></tr>
            </thead>
            <tbody ref={frozenRef}>
              {filtered.map((r, i) => (
                <tr key={r.sym} className="h-9" style={{ background: i % 2 === 1 ? "rgba(255,255,255,0.01)" : "transparent" }}>
                  <td className="px-2.5 text-left font-semibold" style={{ color: "#e2e8f0", borderBottom: "1px solid #162032", borderRight: "1px solid #1e3a5f" }}>{r.sym}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Scrollable */}
        <div ref={scrollRef} className="h-full overflow-auto" style={{ paddingLeft: 130, scrollbarWidth: "thin", scrollbarColor: "#1e3a5f #0a0e17" }}>
          <table style={{ borderCollapse: "separate", borderSpacing: 0, width: "max-content", minWidth: "100%" }}>
            <thead className="sticky top-0 z-20">
              <tr>{scrollGroups.map((g, i) => <th key={i} colSpan={g.span} className={`h-[26px] text-[9px] tracking-widest font-medium ${g.cls}`} style={{ color: "#64748b", borderBottom: "1px solid #162032" }}>{g.label}</th>)}</tr>
              <tr>{SCROLL_COLS.map((c) => <th key={c.key} className="h-10 px-2.5 text-[10px] uppercase tracking-wider font-medium cursor-pointer whitespace-nowrap select-none transition-colors" style={{ background: "#111827", color: sortKey === c.key ? "#38bdf8" : "#64748b", borderBottom: "2px solid #1e3a5f" }} onClick={() => handleSort(c.key)}>{c.label}{arrow(c.key)}</th>)}</tr>
            </thead>
            <tbody>
              {filtered.map((r, i) => (
                <tr key={r.sym} className="h-9 transition-colors" style={{ background: i % 2 === 1 ? "rgba(255,255,255,0.01)" : "transparent" }} onMouseEnter={e => e.currentTarget.style.background = "#243044"} onMouseLeave={e => e.currentTarget.style.background = i % 2 === 1 ? "rgba(255,255,255,0.01)" : "transparent"}>
                  {SCROLL_COLS.map((c) => <td key={c.key} className={`px-2.5 text-right whitespace-nowrap tabular-nums ${cClass(c, r[c.key])}`} style={{ borderBottom: "1px solid #162032", color: cClass(c, r[c.key]) ? undefined : "#94a3b8" }}>{fmt(r[c.key])}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* BOTTOM BAR */}
      <div className="flex items-center justify-between px-5 py-1.5 text-[11px] border-t" style={{ background: "#111827", borderColor: "#162032", color: "#64748b" }}>
        <div>
          <span className="inline-block px-1 py-px rounded text-[10px] mx-0.5" style={{ background: "#1a2236", border: "1px solid #162032", color: "#94a3b8" }}>Ctrl</span>+<span className="inline-block px-1 py-px rounded text-[10px] mx-0.5" style={{ background: "#1a2236", border: "1px solid #162032", color: "#94a3b8" }}>K</span> Search&ensp;
          <span className="inline-block px-1 py-px rounded text-[10px] mx-0.5" style={{ background: "#1a2236", border: "1px solid #162032", color: "#94a3b8" }}>Ctrl</span>+<span className="inline-block px-1 py-py rounded text-[10px] mx-0.5" style={{ background: "#1a2236", border: "1px solid #162032", color: "#94a3b8" }}>E</span> Export&ensp;
          Click headers to sort
        </div>
        <div>500ms refresh • {MOCK.length} rows</div>
      </div>
    </div>
  );
}

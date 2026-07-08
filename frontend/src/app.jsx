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

function App() {
  const [data, setData] = React.useState([]);
  const [meta, setMeta] = React.useState({ ts: "", live: 0 });
  const [search, setSearch] = React.useState("");
  const [sortKey, setSortKey] = React.useState(null);
  const [sortDir, setSortDir] = React.useState(1);
  const scrollRef = React.useRef(null);
  const frozenBodyRef = React.useRef(null);

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
        {/* Frozen Symbol column */}
        <div className="frozen-col">
          <table>
            <thead>
              <tr className="col-groups"><th className="col-group-info">&nbsp;</th></tr>
              <tr><th onClick={() => handleSort("sym")} className={sortKey === "sym" ? "sorted" : ""}>
                Symbol{sortArrow("sym")}
              </th></tr>
            </thead>
            <tbody ref={frozenBodyRef}>
              {filtered.map((r, i) => (
                <tr key={r.sym || i}><td className="sym-cell">{r.sym || "—"}</td></tr>
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
    </>
  );
}

createRoot(document.getElementById("root")).render(<App />);

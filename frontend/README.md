# F2F Spread Terminal — Frontend

Self-contained React frontend for the F2F Spread dashboard. Bundled with **esbuild**
into `../static/app.js` — no CDN, no in-browser Babel, works fully offline.

## Source
- `src/app.jsx` — the entire React app (edit here, never edit `static/app.js` directly).

## Build
```bash
cd frontend
npm install        # once
npm run build      # → ../static/app.js  (minified bundle, React inlined)
npm run watch      # rebuild on save during development
```

## What the backend serves
`f2fspread.py` (Flask) serves:
- `/`                       → `static/index.html`
- `/static/app.js`         → the bundle (React + app)
- `/static/fonts/*.woff2`  → self-hosted JetBrains Mono + DM Sans (latin, variable)
- `/api/spreads`           → live spread JSON (polled every 500 ms)

After changing `src/app.jsx`, run `npm run build` and reload the page.

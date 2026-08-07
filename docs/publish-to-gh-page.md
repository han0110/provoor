# Publishing Results to GitHub Pages

The benchmarkoor frontend runs as a static results viewer when its runtime `config.json` omits the `api` section. In that mode every view reads plain files under `dataSource`, so a GitHub Pages site needs only the built UI bundle and a results directory. No benchmarkoor or provoor code changes are required.

## Requirements

- A results directory produced by `benchmarkoor run` (the `results_dir` of the run config), containing `runs/` and `suites/`.
- A benchmarkoor checkout, Go for its CLI, and Node.js for the UI build.
- A GitHub repository whose Pages site is served from the domain root, meaning a user or organization site repository named `<owner>.github.io` or any repository with a custom domain configured in its Pages settings. A project site under `https://<owner>.github.io/<repo>/` does not work because the UI hardcodes root absolute paths such as `/config.json`, `/results`, and `/img/...` and sets `base: '/'` in `ui/vite.config.ts`.

## Steps

Set the three paths used below.

```bash
BENCHMARKOOR=/path/to/benchmarkoor
RESULTS=/path/to/results
SITE=/path/to/site-staging
```

### 1. Regenerate derived files

The index and suite stats must cover exactly the runs being published. Both commands rewrite them from the raw per-run files.

```bash
cd "$BENCHMARKOOR"
go run ./cmd/benchmarkoor generate-index-file --method local --results-dir "$RESULTS"
go run ./cmd/benchmarkoor generate-suite-stats-file --method local --results-dir "$RESULTS"
```

### 2. Build the UI

```bash
cd "$BENCHMARKOOR/ui"
npm ci
npx vite build --outDir "$SITE" --emptyOutDir
```

### 3. Stage a pruned results copy

Vite copies `ui/public/` into the output and follows the `public/results` symlink, so the staged `results` directory starts as a copy of whatever that symlink points to. Replace it with a pruned copy of the published results.

```bash
rm -rf "$SITE/results"
mkdir "$SITE/results"
tar -C "$RESULTS" -cf - \
  --exclude=container.log \
  --exclude=benchmarkoor.log \
  --exclude='*.request' \
  . | tar -C "$SITE/results" -xf -
```

The excluded files carry no metrics. The two log files are raw runner and client output. The `.request` files under `suites/` hold full JSON-RPC request bodies, including the complete stateless input hex, and dominate the size of the tree. Every metric view reads only the remaining files.

### 4. Add the static-site files

```bash
cat > "$SITE/config.json" <<'EOF'
{ "dataSource": "/results", "title": "zkVM Benchmarks" }
EOF
cp "$SITE/index.html" "$SITE/404.html"
touch "$SITE/.nojekyll"
```

- Omitting the `api` section from `config.json` puts the UI in static viewer mode, so it never attempts API or websocket requests. The shipped default points at `http://localhost:9090` and would show a permanent API-unreachable banner.
- GitHub Pages answers unknown paths with `404.html`, so a copy of `index.html` there makes deep links such as `/runs/<run_id>` boot the app, which then routes client side.
- The `.nojekyll` file disables Jekyll processing, which would otherwise drop dot directories such as `suites/*/.eest-meta` and can mishandle test paths containing `::` and `[]`.

### 5. Preview locally

```bash
python3 -m http.server 8080 -d "$SITE"
```

Open http://localhost:8080/ and click through the runs, run detail, suites, and compare views. Plain file servers lack the `404.html` fallback, so reloading a deep link shows the server's own 404 page. Navigation starting from the root exercises everything else.

### 6. Push and enable Pages

```bash
cd "$SITE"
git init -b gh-pages
git add -A
git commit -m "docs: publish benchmark results"
git remote add origin git@github.com:<owner>/<repo>.git
git push -f origin gh-pages
```

In the repository settings under Pages, choose "Deploy from a branch" with branch `gh-pages` and the root folder. Force pushing a single fresh commit each publish keeps the branch free of stale result history.

Publishing more runs later means running more benchmarks into the same `$RESULTS` directory and repeating these steps.

## Limits

- The live run view, log streaming, the query builder, and the admin pages require the API server and do not appear on the static site. All completed-run metric views work.
- An expanded execution row shows a perpetually loading request size chip because request payloads are pruned. The response viewer and all metrics render normally.
- GitHub Pages enforces a 1 GB site size, 100 MB per file, and a soft 100 GB monthly bandwidth quota. Pruned result trees sit far below these.
- Test directory names contain `::`, `[`, and `]`. GitHub Pages and Linux handle them, but checking the published branch out on Windows fails.

# Publishing Results to GitHub Pages

The benchmarkoor UI runs as a static results viewer when its runtime
`config.json` carries no `api` section. Every view then reads plain files under
`dataSource`, so a Pages site needs only the built bundle and a results
directory.

Publishing is automated in the `provoor-runs` submodule. Its `scripts/build.sh`
regenerates the derived index and suite stats, builds the UI against the Pages
base path, and stages `results/` and `config.json` into `site/`. The `deploy`
workflow runs it on every push to `main`, uploads `site/` with
`actions/upload-pages-artifact`, and publishes it with `actions/deploy-pages`.
Passing `--serve` hosts the result on port 3002 under the same base path the
deployment uses, so a preview matches production.

Results reach that checkout through `scripts/sync.sh` and
`scripts/desensitize.sh`, which the [README](../README.md#scripts) describes.

## Constraints worth knowing

- The UI resolves assets, routes, and `dataSource` against Vite's `--base`, so
  a project site under `https://<owner>.github.io/<repo>/` works as long as the
  base path matches the repository name.
- The shipped `config.json` default points at `http://localhost:9090`, so
  omitting the `api` section is what keeps a published site from showing a
  permanent unreachable banner.
- GitHub Pages answers unknown paths with `404.html`, so a copy of `index.html`
  there lets deep links such as `/provoor-runs/runs/<run_id>` boot the app and
  route client side.

## Limits

- The live run view, log streaming, the query builder, and the admin pages need
  the API server and do not appear on a static site. Every completed-run metric
  view works.
- An expanded execution row shows a perpetually loading request size chip
  because request payloads are pruned. Metrics render normally.
- GitHub Pages enforces a 1 GB published site and a soft 100 GB monthly
  bandwidth quota, and its deployment step times out after ten minutes. Pruned
  result trees sit far below the size limit, so a growing tree reaches the
  deployment timeout first.
- Test directory names contain `::`, `[`, and `]`. GitHub Pages and Linux
  handle them, but checking the published site out on Windows fails.

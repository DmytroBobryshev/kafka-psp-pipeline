# UI e2e smoke scripts

Plain Playwright scripts (no test runner) against the deployed UI at http://localhost.
Requires a global `playwright` install with cached Chromium.

```
NODE_PATH=$(npm root -g) node e2e/layout-shift.cjs    # CLS across all 6 pages must be 0
NODE_PATH=$(npm root -g) node e2e/nav-stability.cjs   # header geometry sampled every 50ms during navigation
```

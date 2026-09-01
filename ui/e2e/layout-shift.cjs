const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });
  await page.goto('http://localhost/payments', { waitUntil: 'networkidle' });
  await page.evaluate(() => {
    window.__cls = 0;
    new PerformanceObserver((list) => {
      for (const e of list.getEntries()) if (!e.hadRecentInput) window.__cls += e.value;
    }).observe({ type: 'layout-shift' });
  });
  const tabs = ['Dashboard', 'Simulator', 'Merchants', 'DLQ', 'Cluster', 'Transactions'];
  const offsets = { payments: await page.evaluate(() => document.querySelector('main').getBoundingClientRect().left) };
  for (const t of tabs) {
    await page.click(`nav >> text="${t}"`);
    await page.waitForTimeout(800);
    offsets[t] = await page.evaluate(() => document.querySelector('main')?.getBoundingClientRect().left);
  }
  const cls = await page.evaluate(() => window.__cls);
  console.log('main-container left offset per page:', JSON.stringify(offsets));
  const values = Object.values(offsets);
  console.log('all offsets identical:', values.every((v) => v === values[0]));
  console.log('cumulative layout shift across 6 navigations:', cls.toFixed(4));
  await browser.close();
})().catch((e) => { console.error('FAIL', e.message); process.exit(1); });

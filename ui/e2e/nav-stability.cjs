const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });
  await page.goto('http://localhost/payments', { waitUntil: 'networkidle' });
  const sample = () =>
    page.evaluate(() => {
      const b = document.querySelector('nav button:last-of-type').getBoundingClientRect();
      const n = document.querySelector('nav').getBoundingClientRect();
      return [Math.round(b.x), Math.round(b.width), Math.round(n.x), Math.round(n.width)].join(',');
    });
  let issues = [];
  for (const t of ['DLQ', 'Cluster', 'Dashboard', 'Merchants', 'Simulator', 'Transactions']) {
    const base = await sample();
    await page.click(`nav >> text="${t}"`);
    for (let i = 0; i < 24; i++) {           // 24 samples x 50ms = 1.2s of navigation settling
      const s = await sample();
      if (s !== base) issues.push(`${t}@${i * 50}ms: ${base} -> ${s}`);
      await page.waitForTimeout(50);
    }
  }
  console.log(issues.length === 0 ? 'STABLE: 144 samples across 6 navigations, zero geometry changes' : issues.slice(0, 8).join('\n'));
  await browser.close();
})().catch((e) => { console.error('FAIL', e.message); process.exit(1); });

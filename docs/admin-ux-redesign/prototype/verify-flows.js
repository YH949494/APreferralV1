// Scripted check of the prototype: runs every flow, counts interactions, triggers each error state, asserts no network request and no page error.
// Usage: npm i playwright (or use an existing install) && node verify-flows.js
const { chromium } = require('playwright');
const path = require('path');
const FILE = require('url').pathToFileURL(require('path').join(__dirname, 'index.html')).href;
const SHOT = require('os').tmpdir();
(async () => {
  const browser = await chromium.launch();
  const results = [];
  const netAttempts = [], errors = [];
  async function fresh(viewport) {
    const ctx = await browser.newContext({ viewport: viewport || { width: 1280, height: 900 } });
    await ctx.route('**/*', r => { const u = r.request().url(); if (u.startsWith('file:') || u.startsWith('data:')) r.continue(); else { netAttempts.push(u); r.abort(); } });
    const page = await ctx.newPage();
    page.on('pageerror', e => errors.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
    await page.goto(FILE); await page.waitForSelector('#main h1');
    return page;
  }
  let n;
  const C = { click: async (p, sel, opts) => { n++; await p.click(sel, opts); }, fill: async (p, sel, v) => { n++; await p.fill(sel, v); }, sel: async (p, sel, v) => { n++; await p.selectOption(sel, v); } };
  async function flow(name, expected, fn, vp) {
    const page = await fresh(vp); n = 0; let ok = true, note = '';
    try { await fn(page); } catch (e) { ok = false; note = e.message.split('\n')[0]; }
    if (ok && expected && n !== expected) { ok = false; note = `interaction count ${n} != expected ${expected}`; }
    results.push({ name, count: n, expected, ok, note }); await page.context().close();
  }
  const bar = (page, label) => C.click(page, `#quick button:has-text("${label}")`);
  const codes10 = Array.from({ length: 8 }, (_, i) => 'AFF10-9' + i).join('\n');
  await flow('create drop', 5, async p => {
    await bar(p, 'Create voucher drop'); await C.fill(p, '#dn', 'Test Drop'); await C.fill(p, '#cb-codes', 'T-1\nT-2\nT-3\nT-4\nT-5\nT-6');
    await C.click(p, '#n1'); await C.click(p, '#create'); await p.waitForSelector('text=was created');
  });
  await flow('upload $10 batch', 6, async p => {
    await bar(p, 'Upload voucher codes'); await C.click(p, '#dchips .chip:has-text("Affiliate $10")'); await C.click(p, '#mchips .chip >> nth=1');
    await C.fill(p, '#cb-text', 'AFF10-0101\nAFF10-0102\nAFF10-0101\nAFF 10 BAD\nAFF10-0001'); await p.waitForSelector('.pill:has-text("1 already in the system")').catch(() => {});
    await C.click(p, '#u-go'); await C.click(p, '#m-ok'); await p.waitForSelector('text=uploaded');
  });
  await flow('shortage fix (tile)', 6, async p => {
    await C.click(p, '.tile:has-text("waiting for $10")'); await C.fill(p, '#cb-text', codes10); await C.click(p, '#u-go'); await C.click(p, '#m-ok'); await p.waitForSelector('text=uploaded');
    await C.click(p, 'button:has-text("Release the")'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Released")');
  });
  await flow('approve one', 3, async p => {
    await C.click(p, '.tile:has-text("need your review")'); await C.click(p, 'tbody tr:first-child button:has-text("Approve")'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Approved")');
  });
  await flow('reject one', 4, async p => {
    await C.click(p, '.tile:has-text("need your review")'); await C.click(p, 'tbody tr:first-child button:has-text("Reject")'); await C.sel(p, '#m-reason', { index: 1 }); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Rejected")');
  });
  await flow('bulk approve safe', 4, async p => {
    await C.click(p, '.tile:has-text("need your review")'); await C.click(p, '#appr-safe'); const label = await p.textContent('#m-phrase >> xpath=preceding::b[1]'); await C.fill(p, '#m-phrase', label.trim()); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Approved 10")');
  });
  await flow('bulk reject', 6, async p => {
    await C.click(p, '.tile:has-text("need your review")'); await C.click(p, '#selall'); await C.click(p, '#rej-sel'); await C.sel(p, '#m-reason', { index: 1 }); await C.fill(p, '#m-phrase', 'REJECT 12'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Rejected 12")');
  });
  await flow('inspect affiliate', 2, async p => {
    await C.fill(p, '#gsearch', '@arif_k'); await p.press('#gsearch', 'Enter'); n++; await p.waitForSelector('text=Two counts differ'); await p.waitForSelector('text=Linked to multiple accounts');
  });
  await flow('inspect + approve', 4, async p => {
    await C.fill(p, '#gsearch', '@mei_lin'); await p.press('#gsearch', 'Enter'); n++; await p.waitForSelector('#led button:has-text("Approve")'); await C.click(p, '#led button:has-text("Approve")'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Approved")');
  });
  await flow('xp', 5, async p => {
    await bar(p, 'Add or remove XP'); await C.fill(p, '#xq', '@sam_tan'); await p.waitForSelector('text=Current XP'); await C.fill(p, '#xa', '100'); await C.click(p, '#xgo'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("now has 410")');
  });
  await flow('registration live', 5, async p => {
    await bar(p, 'Create campaign'); await C.click(p, '.pick:has-text("Registration")'); await C.fill(p, '#cn', 'Oct Draw'); await C.click(p, '#nx'); await C.click(p, '#pub'); await p.waitForSelector('text=Published');
  });
  await flow('tournament live', 8, async p => {
    await bar(p, 'Create campaign'); await C.click(p, '.pick:has-text("Tournament")'); await C.fill(p, '#cn', 'Cup'); await C.sel(p, '#cp', 'arena'); await C.fill(p, '#cpath', '/cup'); await C.click(p, '#crw'); await C.click(p, '#nx'); await C.click(p, '#pub'); await p.waitForSelector('text=Published');
  });
  await flow('tournament blocked by inactive partner', 0, async p => {
    await bar(p, 'Create campaign'); await p.click('.pick:has-text("Tournament")'); await p.fill('#cn', 'Cup'); await p.selectOption('#cp', 'spin'); await p.fill('#cpath', '/cup'); await p.click('#crw'); await p.click('#nx');
    if (!(await p.isDisabled('#pub'))) throw new Error('publish should be disabled'); await p.waitForSelector('text=The partner is active');
  });
  await flow('mission live', 7, async p => {
    await bar(p, 'Create campaign'); await C.click(p, '.pick:has-text("Mission")'); await C.fill(p, '#cn', 'Poll'); await C.fill(p, '#mp', 'Which?'); await C.fill(p, '#mo', 'a | A\nb | B'); await C.click(p, '#nx'); await C.click(p, '#pub'); await p.waitForSelector('text=Published');
  });
  await flow('voucher campaign', 7, async p => {
    await bar(p, 'Create campaign'); await C.click(p, '.pick:has-text("Voucher campaign")'); await C.fill(p, '#cn', 'VC'); await C.fill(p, '#cb-codes', 'V-1\nV-2\nV-3\nV-4\nV-5\nV-6'); await C.click(p, '#nx');
    await p.waitForSelector('#lc'); await C.fill(p, '#lc', 'LAUNCH'); await C.click(p, '#go'); await p.waitForSelector('text=Launched');
  });
  await flow('pause live drop', 3, async p => { await C.click(p, 'tbody tr:first-child .menu button'); await C.click(p, '.menu-pop button:has-text("Pause")'); await p.waitForSelector('text=sets a new 24-hour window'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Paused")'); });
  await flow('end live drop (claimed => typed)', 4, async p => { await C.click(p, 'tbody tr:first-child .menu button'); await C.click(p, '.menu-pop button:has-text("End now")'); await C.fill(p, '#m-phrase', 'END'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("Ended")'); });
  await flow('start scheduled drop', 3, async p => { await C.click(p, 'tbody tr:has-text("VIP October") .menu button'); await C.click(p, '.menu-pop button:has-text("Start now")'); await p.waitForSelector('text=end time is reset'); await C.click(p, '#m-ok'); await p.waitForSelector('.toast:has-text("is live")'); });
  await flow('add codes from row', 4, async p => { await C.click(p, 'tbody tr:first-child button:has-text("Add codes")'); await C.fill(p, '#cb-text', 'N-1\nN-2'); await C.click(p, '#u-go'); await C.click(p, '#m-ok'); await p.waitForSelector('text=uploaded'); });
  // error states
  await flow('error: network fail on create', 0, async p => {
    await p.click('#quick button:has-text("Create voucher drop")'); await p.fill('#dn', 'X'); await p.fill('#cb-codes', 'Q-1\nQ-2'); await p.click('#n1'); await p.check('#demo-btn ~ * #d-fail, #d-fail', { force: true }).catch(async () => { await p.click('#demo-btn'); await p.check('#d-fail'); });
    await p.click('#create'); await p.waitForSelector('text=may or may not have gone through'); await p.waitForSelector('text=open Drops and look for'); await p.waitForSelector('#create:not([disabled])');
  });
  await flow('error: duplicate code blocks drop create', 0, async p => {
    await p.click('#quick button:has-text("Create voucher drop")'); await p.fill('#dn', 'X'); await p.fill('#cb-codes', 'WKND-0007\nQ-2'); await p.waitForSelector('text=already used in another drop'); await p.click('#n1'); await p.waitForSelector('.toast:has-text("already used")');
  });
  await flow('error: colleague handled reward', 0, async p => {
    await p.click('.tile:has-text("need your review")'); await p.click('#demo-btn'); await p.check('#d-other'); await p.click('tbody tr:first-child button:has-text("Approve")'); await p.click('#m-ok'); await p.waitForSelector('#m-err:has-text("colleague already handled")');
  });
  await flow('error: session expired + sign in', 0, async p => {
    await p.click('#demo-btn'); await p.check('#d-sess'); await p.click('.tile:has-text("need your review")'); await p.click('tbody tr:first-child button:has-text("Approve")'); await p.click('#m-ok'); await p.waitForSelector('#m-err:has-text("session expired")'); await p.click('#m-sign'); await p.waitForSelector('#m-err:has-text("Signed back in")');
  });
  await flow('error: xp cooldown', 0, async p => {
    await p.click('#demo-btn'); await p.check('#d-cool'); await p.click('#quick button:has-text("Add or remove XP")'); await p.fill('#xq', '@sam_tan'); await p.waitForSelector('text=Current XP'); await p.fill('#xa', '50'); await p.click('#xgo'); await p.click('#m-ok'); await p.waitForSelector('#m-err:has-text("42 seconds")');
  });
  await flow('error: xp large => typed', 0, async p => {
    await p.click('#quick button:has-text("Add or remove XP")'); await p.fill('#xq', '@sam_tan'); await p.waitForSelector('text=Current XP'); await p.fill('#xa', '5000'); await p.click('#xgo'); await p.waitForSelector('#m-phrase'); if (!(await p.isDisabled('#m-ok'))) throw new Error('should require phrase');
  });
  await flow('error: not found search', 0, async p => { await p.fill('#gsearch', '@nobody'); await p.press('#gsearch', 'Enter'); await p.waitForSelector('text=No player found'); });
  await flow('error: insufficient stock on tournament approve', 0, async p => { await p.click('nav >> text=Campaigns'); await p.click('[data-sub="results"]'); await p.waitForSelector('text=Need 25, have 30'); await p.click('tbody tr:nth-child(1) button:has-text("Approve")'); await p.click('#m-ok'); await p.waitForSelector('.toast:has-text("Approved")'); await p.waitForSelector('text=Need 25, have 20'); await p.click('tbody tr:nth-child(1) button:has-text("Approve")'); await p.click('#m-ok'); await p.waitForSelector('#m-err:has-text("Not enough vouchers")'); });
  // every view loads + mobile overflow + screenshots
  const views = [['home','main'],['vouchers','upload'],['vouchers','drops'],['vouchers','create'],['vouchers','stock'],['affiliates','find'],['affiliates','review'],['affiliates','stock'],['campaigns','all'],['campaigns','create'],['campaigns','results'],['players','find'],['players','xp'],['content','main'],['reports','main'],['tools','main']];
  for (const vp of [{ width: 1280, height: 900 }, { width: 390, height: 800 }]) {
    const page = await fresh(vp);
    for (const [v, s] of views) {
      await page.evaluate(([v, s]) => go(v, s), [v, s]); await page.waitForTimeout(80);
      const w = await page.evaluate(() => ({ sw: document.documentElement.scrollWidth, iw: window.innerWidth }));
      const h1 = await page.textContent('#main h1').catch(() => null);
      if (!h1) results.push({ name: `view ${v}:${s} @${vp.width}`, count: 0, expected: 0, ok: false, note: 'no h1' });
      if (w.sw > w.iw + 1) results.push({ name: `overflow ${v}:${s} @${vp.width}`, count: w.sw, expected: w.iw, ok: false, note: 'horizontal scroll' });
    }
    if (vp.width === 1280) {
      await page.evaluate(() => go('home', 'main')); await page.waitForTimeout(100); await page.screenshot({ path: SHOT + '/home.png', fullPage: false });
      await page.evaluate(() => go('vouchers', 'upload', { dest: 'AFFILIATE_10' })); await page.click('#mchips .chip >> nth=1'); await page.fill('#cb-text', 'AFF10-0101\nAFF10-0102\nAFF10-0101\nAFF 10 BAD\nAFF10-0001'); await page.waitForTimeout(150); await page.screenshot({ path: SHOT + '/upload.png' });
      await page.fill('#gsearch', '@arif_k'); await page.press('#gsearch', 'Enter'); await page.waitForSelector('text=Two counts differ'); await page.waitForTimeout(200); await page.screenshot({ path: SHOT + '/aff360.png', fullPage: true });
    } else { await page.evaluate(() => go('home', 'main')); await page.waitForTimeout(100); await page.screenshot({ path: SHOT + '/mobile-home.png' }); }
    await page.context().close();
  }
  // dark mode screenshot
  const dctx = await browser.newContext({ viewport: { width: 1280, height: 800 }, colorScheme: 'dark' }); const dp = await dctx.newPage(); await dp.goto(FILE); await dp.waitForSelector('#main h1'); await dp.screenshot({ path: SHOT + '/dark-home.png' }); await dctx.close();
  console.log(results.map(r => `${r.ok ? 'PASS' : 'FAIL'}  ${r.name}  count=${r.count}${r.expected ? ' expected=' + r.expected : ''}${r.note ? '  ' + r.note : ''}`).join('\n'));
  console.log('network attempts:', netAttempts.length, netAttempts.slice(0, 3));
  console.log('page errors:', errors.length, errors.slice(0, 5));
  const failed = results.filter(r => !r.ok).length;
  if (failed || netAttempts.length || errors.length) { console.error(`FAILED: ${failed} flow check(s), ${netAttempts.length} network request(s), ${errors.length} page error(s)`); process.exitCode = 1; }
  await browser.close();
})();

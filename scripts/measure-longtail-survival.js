#!/usr/bin/env node
'use strict';
// Mesure ponctuelle (lecture seule, pas déployée) : quelle fraction des
// tokens qui atteignent la cascade étendue (T+20s) survit jusqu'à la
// longue traîne actuelle (T+120s+), et parmi ceux-là, combien ressortent
// de la longue traîne SANS que le RPC ait observé la curve terminée —
// c'est exactement la population qui bénéficierait d'une phase de
// confirmation. Sert à chiffrer précisément le coût RPC de cette phase
// au lieu de deviner une fourchette.

const { createClient } = require('@supabase/supabase-js');
function requireEnv(n) { const v = process.env[n]; if (!v) throw new Error(`env manquant: ${n}`); return v; }
const PAGE_SIZE = 1000;
async function fetchAllRows(supabase, table, select, orderColumn, applyFilter) {
  const rows = [];
  let from = 0;
  for (;;) {
    let q = supabase.from(table).select(select).order(orderColumn, { ascending: true }).range(from, from + PAGE_SIZE - 1);
    if (applyFilter) q = applyFilter(q);
    const { data, error } = await q;
    if (error) throw new Error(`${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows;
}
const MINT_BATCH_SIZE = 150;
const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  const allTokens = await fetchAllRows(supabase, 'tokens', 'mint, created_at, raw_new_token_event', 'mint');
  const tokens = allTokens.filter((t) => t.raw_new_token_event && t.created_at && new Date(t.created_at) >= ANALYSIS_SINCE);
  console.log(`Tokens dans la fenêtre (créés depuis ${ANALYSIS_SINCE.toISOString()}) : ${tokens.length}`);

  const mints = tokens.map((t) => t.mint);
  const snapshots = [];
  for (let i = 0; i < mints.length; i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const rows = await fetchAllRows(supabase, 'token_snapshots', 'mint, age_seconds, nominal_delay_s, virtual_sol_reserves, raw_event', 'id', (q) => q.in('mint', batch));
    snapshots.push(...rows);
  }
  console.log(`Snapshots récupérés : ${snapshots.length}`);

  const byMint = new Map();
  for (const s of snapshots) {
    if (!byMint.has(s.mint)) byMint.set(s.mint, []);
    byMint.get(s.mint).push(s);
  }

  function isCompleted(s) {
    return !!(s.raw_event && s.raw_event.complete === true) || s.virtual_sol_reserves === 0;
  }

  let reachedExtended = 0; // a une lecture au premier point étendu (20s)
  let reachedLongTail = 0; // a AUSSI une lecture en longue traîne (>=120s)
  let unresolvedAfterLongTail = 0; // a atteint la longue traîne SANS qu'aucune lecture (gate/étendue/longue traîne) ne montre complete
  let resolvedSomewhereInLongTailWindow = 0;

  for (const mint of mints) {
    const rows = byMint.get(mint) || [];
    const hasExtended = rows.some((s) => s.nominal_delay_s === 20);
    if (!hasExtended) continue;
    reachedExtended += 1;

    const hasLongTail = rows.some((s) => s.nominal_delay_s >= 120);
    if (!hasLongTail) continue;
    reachedLongTail += 1;

    const anyCompleted = rows.some(isCompleted);
    if (anyCompleted) resolvedSomewhereInLongTailWindow += 1;
    else unresolvedAfterLongTail += 1;
  }

  console.log(`\nAtteint l'étendue (T+20s)                          : ${reachedExtended}`);
  console.log(`... et atteint la longue traîne (T+120s+)          : ${reachedLongTail}  (f2 = ${reachedExtended ? ((reachedLongTail / reachedExtended) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`    ... résolu (RPC) quelque part dans la fenêtre  : ${resolvedSomewhereInLongTailWindow}`);
  console.log(`    ... jamais résolu (candidat confirmation)      : ${unresolvedAfterLongTail}  (f3 = ${reachedLongTail ? ((unresolvedAfterLongTail / reachedLongTail) * 100).toFixed(1) : 'n/a'}%)`);

  const windowHours = (Date.now() - ANALYSIS_SINCE.getTime()) / 3_600_000;
  const perDay = (n) => (n / windowHours) * 24;
  console.log(`\nFenêtre observée : ${windowHours.toFixed(2)}h`);
  console.log(`Extrapolation /jour — atteint étendue : ${perDay(reachedExtended).toFixed(0)}, atteint longue traîne : ${perDay(reachedLongTail).toFixed(0)}, candidats confirmation : ${perDay(unresolvedAfterLongTail).toFixed(0)}`);
}

main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

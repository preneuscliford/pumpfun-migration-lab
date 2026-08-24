#!/usr/bin/env node
'use strict';

// Mesure ponctuelle (lecture seule) demandée le 2026-08-24 : quantifier
// précisément l'engorgement de la file RPC partagée (bonding curve +
// holders) à partir des données déjà collectées, sans toucher au
// listener. Sert de base chiffrée à la proposition file prioritaire
// bonding curve / file séparée holders.
//
// Limite connue, annoncée explicitement dans la sortie : captureCascadeRead
// (src/listener.js) calcule age_seconds AVANT d'attendre la file RPC
// (rpcThrottle), donc age_seconds/nominal_delay_s ne capturent PAS le
// temps d'attente en file — seulement la gigue du timer Node. Le seul
// signal disponible pour le délai réel bout-en-bout est captured_at
// (horodatage par défaut de Postgres, posé à l'insertion — donc APRÈS
// l'appel RPC complet). Ce script mesure ce délai en le nommant
// honnêtement "délai total mesurable (attente file + appel RPC + insert
// DB confondus)", pas "délai de file" seul — voir la proposition
// d'instrumentation scheduled_at/queued_at/started_at/completed_at pour
// une mesure propre à l'avenir.
//
// Usage : node scripts/measure-rpc-queue-delay.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(n) {
  const v = process.env[n];
  if (!v) throw new Error(`env manquant: ${n}`);
  return v;
}

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

function mean(a) { return a.length ? a.reduce((s, v) => s + v, 0) / a.length : null; }
function median(a) { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; }
function percentile(a, p) { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const idx = (p / 100) * (s.length - 1); const lo = Math.floor(idx), hi = Math.ceil(idx); return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo); }
function fmt(v, d = 2) { return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v.toFixed(d); }
function stats(name, values) {
  const clean = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!clean.length) { console.log(`  ${name.padEnd(46)} n=0`); return; }
  console.log(`  ${name.padEnd(46)} n=${String(clean.length).padStart(5)}  P10=${fmt(percentile(clean, 10))} P25=${fmt(percentile(clean, 25))} P50=${fmt(percentile(clean, 50))} P75=${fmt(percentile(clean, 75))} P90=${fmt(percentile(clean, 90))} P99=${fmt(percentile(clean, 99))} max=${fmt(Math.max(...clean))}`);
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  const allTokens = await fetchAllRows(supabase, 'tokens', 'mint, created_at, raw_new_token_event', 'mint');
  const tokens = allTokens.filter((t) => t.raw_new_token_event && t.created_at && new Date(t.created_at) >= ANALYSIS_SINCE);
  console.log(`Tokens dans la fenêtre (créés depuis ${ANALYSIS_SINCE.toISOString()}) : ${tokens.length}`);
  const createdAtByMint = new Map(tokens.map((t) => [t.mint, new Date(t.created_at).getTime()]));

  const mints = tokens.map((t) => t.mint);
  const snapshots = [];
  for (let i = 0; i < mints.length; i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const rows = await fetchAllRows(
      supabase,
      'token_snapshots',
      'mint, captured_at, age_seconds, nominal_delay_s, virtual_sol_reserves, total_supply, raw_event',
      'id',
      (q) => q.in('mint', batch)
    );
    snapshots.push(...rows);
  }
  console.log(`Snapshots récupérés : ${snapshots.length}`);

  const windowHours = (Date.now() - ANALYSIS_SINCE.getTime()) / 3_600_000;
  const perDay = (n) => (n / windowHours) * 24;

  // --- 1. Volume RPC réel mesuré, par type (pas théorique) ---
  const gate = snapshots.filter((s) => [2, 5, 10].includes(s.nominal_delay_s));
  const extended = snapshots.filter((s) => [20, 30, 45, 60].includes(s.nominal_delay_s));
  const longTail = snapshots.filter((s) => s.nominal_delay_s >= 120);
  const holders = snapshots.filter((s) => s.total_supply !== null && s.total_supply !== undefined);
  console.log('\n' + '='.repeat(72));
  console.log('VOLUME RPC RÉEL MESURÉ (pas théorique) — extrapolé /jour');
  console.log('='.repeat(72));
  console.log(`  Gate (T+2/5/10s)            : ${gate.length} lectures  -> ${perDay(gate.length).toFixed(0)}/j  (x1 requête RPC chacune)`);
  console.log(`  Étendue (T+20/30/45/60s)    : ${extended.length} lectures  -> ${perDay(extended.length).toFixed(0)}/j`);
  console.log(`  Longue traîne (T+120s+)     : ${longTail.length} lectures  -> ${perDay(longTail.length).toFixed(0)}/j`);
  console.log(`  Holders (~20 requêtes/capture): ${holders.length} captures -> ${perDay(holders.length).toFixed(0)}/j  soit ~${(perDay(holders.length) * 20).toFixed(0)} requêtes/j`);
  const totalReqPerDay = perDay(gate.length + extended.length + longTail.length) + perDay(holders.length) * 20;
  console.log(`  Total requêtes RPC estimé   : ~${totalReqPerDay.toFixed(0)}/j  (capacité soutenable ~288 000/j à 300ms d'écart)`);

  // --- 2. Délai total mesurable (captured_at - created_at - nominal_delay_s) ---
  // ATTENTION (voir en-tête) : inclut attente file + appel RPC + insert DB,
  // PAS uniquement l'attente en file. C'est la seule mesure disponible
  // sans nouvelle instrumentation.
  function totalDelaySeconds(s) {
    const createdAtMs = createdAtByMint.get(s.mint);
    if (createdAtMs === undefined || !s.captured_at) return null;
    const capturedAtMs = new Date(s.captured_at).getTime();
    return (capturedAtMs - createdAtMs) / 1000 - s.nominal_delay_s;
  }
  console.log('\n' + '='.repeat(72));
  console.log('DÉLAI TOTAL MESURABLE (captured_at - created_at - nominal_delay_s)');
  console.log('  Inclut attente file RPC + appel RPC lui-même + insertion DB —');
  console.log('  pas décomposable finement sans nouvelle instrumentation (voir plus bas).');
  console.log('='.repeat(72));
  stats('Gate (T+2/5/10s)', gate.map(totalDelaySeconds));
  stats('Étendue (T+20/30/45/60s)', extended.map(totalDelaySeconds));
  stats('Longue traîne (T+120s+)', longTail.map(totalDelaySeconds));
  stats('Holders (première lecture étendue)', holders.map(totalDelaySeconds));

  // --- 3. Corrélation lectures bonding-curve retardées <-> holders en cours ---
  // Pour chaque lecture bonding-curve-only (total_supply null) avec un délai
  // total > 5s (indice net de mise en attente), regarde s'il existe une
  // capture holders (n'importe quel mint) dont captured_at tombe dans les
  // 30s précédentes — fenêtre large car une capture holders dure ~10-25s et
  // bloque la file pendant tout ce temps (voir découverte du 2026-08-24).
  const bcOnly = snapshots.filter((s) => (s.total_supply === null || s.total_supply === undefined) && s.captured_at);
  const holdersCapturedAtMs = holders.filter((h) => h.captured_at).map((h) => new Date(h.captured_at).getTime()).sort((a, b) => a - b);
  function holdersActiveJustBefore(capturedAtMs, windowMs = 30_000) {
    // recherche binaire du plus grand holdersCapturedAtMs <= capturedAtMs
    let lo = 0, hi = holdersCapturedAtMs.length - 1, idx = -1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (holdersCapturedAtMs[mid] <= capturedAtMs) { idx = mid; lo = mid + 1; } else hi = mid - 1;
    }
    if (idx === -1) return false;
    return capturedAtMs - holdersCapturedAtMs[idx] <= windowMs;
  }
  const delayed = bcOnly.filter((s) => {
    const d = totalDelaySeconds(s);
    return d !== null && d > 5;
  });
  const delayedNearHolders = delayed.filter((s) => holdersActiveJustBefore(new Date(s.captured_at).getTime()));
  console.log('\n' + '='.repeat(72));
  console.log('CORRÉLATION — lectures bonding-curve retardées (>5s) vs holders en cours');
  console.log('  "en cours" = une capture holders (n\'importe quel mint) terminée dans');
  console.log('  les 30s précédant cette lecture retardée — corrélation temporelle,');
  console.log('  PAS une preuve de causalité stricte requête par requête.');
  console.log('='.repeat(72));
  console.log(`  Lectures bonding-curve-only (hors holders) : ${bcOnly.length}`);
  console.log(`  ... dont retardées (délai total > 5s)      : ${delayed.length}  (${bcOnly.length ? ((delayed.length / bcOnly.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`  ... dont retardées ET holders actif juste avant : ${delayedNearHolders.length}  (${delayed.length ? ((delayedNearHolders.length / delayed.length) * 100).toFixed(1) : 'n/a'}% des retardées)`);
  stats('délai total des lectures retardées (s)', delayed.map(totalDelaySeconds));

  // --- 4. Longue traîne : population candidate à la confirmation ---
  const snapshotsByMint = new Map();
  for (const s of snapshots) {
    if (!snapshotsByMint.has(s.mint)) snapshotsByMint.set(s.mint, []);
    snapshotsByMint.get(s.mint).push(s);
  }
  function isCompleted(s) { return !!(s.raw_event && s.raw_event.complete === true) || s.virtual_sol_reserves === 0; }
  let reachedExtendedTokens = 0, reachedLongTailTokens = 0, unresolvedAfterLongTail = 0;
  for (const mint of mints) {
    const rows = snapshotsByMint.get(mint) || [];
    if (!rows.some((s) => s.nominal_delay_s === 20)) continue;
    reachedExtendedTokens += 1;
    if (!rows.some((s) => s.nominal_delay_s >= 120)) continue;
    reachedLongTailTokens += 1;
    if (!rows.some(isCompleted)) unresolvedAfterLongTail += 1;
  }
  console.log('\n' + '='.repeat(72));
  console.log('POPULATION LONGUE TRAÎNE / CANDIDATS CONFIRMATION');
  console.log('='.repeat(72));
  console.log(`  Tokens ayant une lecture à T+20s (étendue atteinte) : ${reachedExtendedTokens}  -> ${perDay(reachedExtendedTokens).toFixed(0)}/j`);
  console.log(`  Tokens ayant AUSSI une lecture à T+120s+ (longue traîne atteinte) : ${reachedLongTailTokens}  -> ${perDay(reachedLongTailTokens).toFixed(0)}/j`);
  console.log(`  ... dont jamais résolus par RPC (candidats confirmation) : ${unresolvedAfterLongTail}  -> ${perDay(unresolvedAfterLongTail).toFixed(0)}/j`);

  console.log('\n' + '='.repeat(72));
  console.log('Limites de cette mesure (pas de nouvelle instrumentation) :');
  console.log('  - Le "délai total" mélange attente file + appel RPC + insert DB —');
  console.log('    impossible de les séparer avec les colonnes actuelles.');
  console.log('  - La corrélation "holders actif juste avant" est temporelle, pas une');
  console.log('    preuve stricte que CE holders a retardé CETTE lecture précise.');
  console.log('  - reachedExtendedTokens (ci-dessus) est bien inférieur à ce que le taux');
  console.log('    d\'activation au gate laisserait attendre (~35% mesuré ailleurs) — signe');
  console.log('    que des lectures étendues SONT programmées mais n\'aboutissent jamais');
  console.log('    (perdues en file, ou perdues au relais du process toutes les ~6h).');
  console.log('='.repeat(72));
}

main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

#!/usr/bin/env node
'use strict';

// Analyse en lecture seule (2026-08-24) : vérifier, sur les données
// réellement collectées depuis le déploiement du throttle adaptatif
// bonding curve (commit e9001df, createAdaptiveBondingCurveThrottle),
// si les chiffres tiennent la promesse faite avant codage — comparaison
// directe avec les valeurs mesurées à l'espacement fixe (300ms) le
// 2026-08-24 (scripts/analyze-v3-instrumentation.js) :
//   Gate       P90=47690ms P99=121047ms max=144561ms
//   Étendue    P90=47915ms P99=109841ms max=145585ms
//   Longue traîne P90=39251ms P99=103321ms max=144087ms
//
// Ne construit aucun score, aucun filtre, aucune logique A/B/C. Lecture
// seule.
//
// Usage : node scripts/analyze-adaptive-throttle.js

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

// Déploiement du CORRECTIF du throttle adaptatif (commit 8d5c872,
// 2026-08-24) — le premier déploiement (e9001df, 14:59:36Z) s'est révélé
// engorgé à 99% (voir le commit 8d5c872 pour le post-mortem) ; ce script
// mesure désormais depuis le correctif, pas depuis le premier déploiement.
const DEPLOY_AT = new Date(process.env.DEPLOY_AT || '2026-08-24T19:43:27Z');
// Valeur par défaut du garde-fou de délai (BC_DEADLINE_MS) — sert à
// estimer combien de lectures ont dû passer par lui.
const BC_DEADLINE_MS = Number(process.env.BC_DEADLINE_MS) || 18_000;

function percentile(a, p) { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const idx = (p / 100) * (s.length - 1); const lo = Math.floor(idx), hi = Math.ceil(idx); return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo); }
function fmt(v, d = 1) { return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v.toFixed(d); }
function stats(name, values) {
  const clean = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!clean.length) { console.log(`  ${name.padEnd(24)} n=0`); return; }
  console.log(`  ${name.padEnd(24)} n=${String(clean.length).padStart(6)}  P50=${fmt(percentile(clean, 50))} P90=${fmt(percentile(clean, 90))} P99=${fmt(percentile(clean, 99))} max=${fmt(Math.max(...clean))}`);
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  const snapshots = await fetchAllRows(
    supabase,
    'token_snapshots',
    'nominal_delay_s, queue_wait_ms, rpc_call_ms, scheduled_at, started_at',
    'id',
    (q) => q.gte('captured_at', DEPLOY_AT.toISOString())
  );
  console.log(`Snapshots depuis le déploiement du throttle adaptatif (${DEPLOY_AT.toISOString()}) : ${snapshots.length}`);

  const gate = snapshots.filter((s) => [2, 5, 10].includes(s.nominal_delay_s));
  const extended = snapshots.filter((s) => [20, 30, 45, 60].includes(s.nominal_delay_s));
  const longTail = snapshots.filter((s) => s.nominal_delay_s >= 120);

  console.log('\n' + '='.repeat(78));
  console.log('queue_wait_ms — AVANT (300ms fixe, mesuré le 2026-08-24 matin) :');
  console.log('  Gate       P90=47690  P99=121047  max=144561');
  console.log('  Étendue    P90=47915  P99=109841  max=145585');
  console.log('  Longue traîne P90=39251 P99=103321  max=144087');
  console.log('queue_wait_ms — APRÈS (throttle adaptatif) :');
  console.log('='.repeat(78));
  stats('Gate (T+2/5/10s)', gate.map((s) => s.queue_wait_ms));
  stats('Étendue (T+20-60s)', extended.map((s) => s.queue_wait_ms));
  stats('Longue traîne (T+120s+)', longTail.map((s) => s.queue_wait_ms));

  console.log('\n' + '='.repeat(78));
  console.log('rpc_call_ms (durée de l\'appel lui-même, retries internes inclus)');
  console.log('='.repeat(78));
  stats('Gate', gate.map((s) => s.rpc_call_ms));
  stats('Étendue', extended.map((s) => s.rpc_call_ms));
  stats('Longue traîne', longTail.map((s) => s.rpc_call_ms));

  // Mesure précise "prévu vs exécuté" via les horodatages absolus
  // (scheduled_at/started_at), plus fiable que l'ancienne approximation
  // captured_at - created_at - nominal_delay_s (qui incluait aussi
  // l'insertion DB).
  function precisionGapS(s) {
    if (!s.scheduled_at || !s.started_at) return null;
    return (new Date(s.started_at).getTime() - new Date(s.scheduled_at).getTime()) / 1000;
  }
  console.log('\n' + '='.repeat(78));
  console.log('ÉCART started_at - scheduled_at (secondes) — mesure précise "T+5s visé vs exécuté"');
  console.log('  (nouveau, impossible avant l\'ajout des horodatages absolus)');
  console.log('='.repeat(78));
  stats('Gate', gate.map(precisionGapS));
  stats('Étendue', extended.map(precisionGapS));
  stats('Longue traîne', longTail.map(precisionGapS));

  const allWait = snapshots.map((s) => s.queue_wait_ms).filter((v) => v !== null && v !== undefined);
  const nearDeadline = allWait.filter((v) => v >= BC_DEADLINE_MS * 0.9).length;
  console.log('\n' + '='.repeat(78));
  console.log(`GARDE-FOU (BC_DEADLINE_MS=${BC_DEADLINE_MS}ms) — lectures avec queue_wait_ms >= 90% du seuil`);
  console.log('='.repeat(78));
  console.log(`  ${nearDeadline} / ${allWait.length}  (${allWait.length ? ((nearDeadline / allWait.length) * 100).toFixed(2) : 'n/a'}%)`);
  console.log(`  max observé : ${allWait.length ? Math.max(...allWait) : 'n/a'}ms`);
  console.log('  Le pire cas ne devrait plus dépasser sensiblement le seuil, quelle que');
  console.log('  soit l\'intensité de la rafale — c\'est ce garde-fou qui doit l\'assurer.');
}

main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

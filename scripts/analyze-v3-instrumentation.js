#!/usr/bin/env node
'use strict';

// Analyse en lecture seule demandée le 2026-08-24, après ~9h de collecte
// sous la nouvelle architecture (commit 59fd5d2 : files RPC séparées
// bonding curve/holders, curve_completed_at posé par le RPC lui-même,
// instrumentation queue_wait_ms/rpc_call_ms). Objectif : répondre avec des
// chiffres réels (pas devinés) à deux questions laissées en suspens :
//   1. Une lecture visée à T+5s est-elle vraiment exécutée à T+5s
//      maintenant que bonding curve a sa file dédiée ?
//   2. curve_completed_at capture-t-il enfin la complétion pour une part
//      significative des tokens (plus seulement ~0.8% comme dans la
//      preuve de concept avant ce correctif) ?
//
// Ne construit aucun score, aucun filtre, aucun groupe A/B/C. Lecture
// seule, aucune écriture.
//
// Usage : node scripts/analyze-v3-instrumentation.js

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

// Déploiement du nouveau code : 05:23:15Z. Le cache de schéma PostgREST
// n'a été rechargé qu'à ~05:25:49Z (voir conversation) : les inserts de
// snapshots avec queue_wait_ms/rpc_call_ms entre ces deux instants ont
// échoué et sont donc ABSENTS (pas corrompus) de token_snapshots. On
// s'aligne sur le rechargement du cache pour la partie instrumentation.
const DEPLOY_AT = new Date(process.env.DEPLOY_AT || '2026-08-24T05:23:15Z');
const SCHEMA_FIXED_AT = new Date(process.env.SCHEMA_FIXED_AT || '2026-08-24T05:26:00Z');

function median(a) { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; }
function percentile(a, p) { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const idx = (p / 100) * (s.length - 1); const lo = Math.floor(idx), hi = Math.ceil(idx); return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo); }
function fmt(v, d = 1) { return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v.toFixed(d); }
function stats(name, values) {
  const clean = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!clean.length) { console.log(`  ${name.padEnd(38)} n=0`); return; }
  console.log(`  ${name.padEnd(38)} n=${String(clean.length).padStart(6)}  P10=${fmt(percentile(clean, 10))} P50=${fmt(percentile(clean, 50))} P90=${fmt(percentile(clean, 90))} P99=${fmt(percentile(clean, 99))} max=${fmt(Math.max(...clean))}`);
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  const allTokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, created_at, curve_completed_at, migrated_at, curve_completion_lag_seconds',
    'mint'
  );
  const tokens = allTokens.filter((t) => t.created_at && new Date(t.created_at) >= DEPLOY_AT);
  console.log(`Tokens créés depuis le déploiement (${DEPLOY_AT.toISOString()}) : ${tokens.length}`);

  const mints = tokens.map((t) => t.mint);
  const snapshots = [];
  for (let i = 0; i < mints.length; i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const rows = await fetchAllRows(
      supabase,
      'token_snapshots',
      'mint, captured_at, nominal_delay_s, queue_wait_ms, rpc_call_ms, holders_queue_wait_ms, holders_rpc_call_ms, holders_error, total_supply',
      'id',
      (q) => q.in('mint', batch).gte('captured_at', SCHEMA_FIXED_AT.toISOString())
    );
    snapshots.push(...rows);
  }
  console.log(`Snapshots récupérés depuis le rechargement du cache (${SCHEMA_FIXED_AT.toISOString()}) : ${snapshots.length}`);

  // --- 1. queue_wait_ms / rpc_call_ms réels, par type de lecture ---
  const gate = snapshots.filter((s) => [2, 5, 10].includes(s.nominal_delay_s));
  const extended = snapshots.filter((s) => [20, 30, 45, 60].includes(s.nominal_delay_s));
  const longTail = snapshots.filter((s) => s.nominal_delay_s >= 120);

  console.log('\n' + '='.repeat(78));
  console.log('DÉLAI RÉEL DE FILE BONDING CURVE (queue_wait_ms) — était-ce vraiment T+5s ?');
  console.log('='.repeat(78));
  stats('Gate (T+2/5/10s)', gate.map((s) => s.queue_wait_ms));
  stats('Étendue (T+20/30/45/60s)', extended.map((s) => s.queue_wait_ms));
  stats('Longue traîne (T+120s+)', longTail.map((s) => s.queue_wait_ms));

  console.log('\n' + '='.repeat(78));
  console.log('DURÉE DE L\'APPEL RPC LUI-MÊME (rpc_call_ms, retries internes inclus)');
  console.log('='.repeat(78));
  stats('Gate (T+2/5/10s)', gate.map((s) => s.rpc_call_ms));
  stats('Étendue (T+20/30/45/60s)', extended.map((s) => s.rpc_call_ms));
  stats('Longue traîne (T+120s+)', longTail.map((s) => s.rpc_call_ms));

  // --- 2. Holders : file séparée, taux de succès réel depuis le correctif ---
  const holdersAttempts = snapshots.filter((s) => s.holders_queue_wait_ms !== null || s.holders_error !== null);
  const holdersSuccess = snapshots.filter((s) => s.holders_queue_wait_ms !== null);
  const holdersFail = snapshots.filter((s) => s.holders_error !== null);
  console.log('\n' + '='.repeat(78));
  console.log('HOLDERS — file dédiée, taux de succès réel');
  console.log('='.repeat(78));
  console.log(`  Tentatives : ${holdersAttempts.length}   Succès : ${holdersSuccess.length}   Échecs (429 ou autre) : ${holdersFail.length}`);
  console.log(`  Taux de succès : ${holdersAttempts.length ? ((holdersSuccess.length / holdersAttempts.length) * 100).toFixed(1) : 'n/a'}%`);
  if (holdersFail.length) {
    const sampleErrors = [...new Set(holdersFail.map((s) => s.holders_error))].slice(0, 5);
    console.log(`  Exemples d'erreur : ${sampleErrors.join(' | ')}`);
  }
  stats('holders_queue_wait_ms (succès)', holdersSuccess.map((s) => s.holders_queue_wait_ms));
  stats('holders_rpc_call_ms (succès)', holdersSuccess.map((s) => s.holders_rpc_call_ms));
  console.log('  NOTE : la file bonding curve est séparée par construction (deux');
  console.log('  createRpcThrottle indépendants) — un queue_wait_ms bonding curve élevé');
  console.log('  ne peut structurellement plus venir d\'une attente holders. Les stats');
  console.log('  ci-dessus vérifient seulement que holders n\'a pas régressé de son côté.');

  // --- 3. curve_completed_at : couverture réelle depuis l'arrêt-sur-RPC ---
  const withCompleted = tokens.filter((t) => t.curve_completed_at);
  const withMigrated = tokens.filter((t) => t.migrated_at);
  const withBoth = tokens.filter((t) => t.curve_completed_at && t.migrated_at);
  const completedNoMigration = tokens.filter((t) => t.curve_completed_at && !t.migrated_at);
  console.log('\n' + '='.repeat(78));
  console.log('COUVERTURE curve_completed_at (horloge RPC) DEPUIS LE CORRECTIF');
  console.log('='.repeat(78));
  console.log(`  Tokens dans la fenêtre                         : ${tokens.length}`);
  console.log(`  ... avec curve_completed_at (RPC a vu la fin)  : ${withCompleted.length}  (${tokens.length ? ((withCompleted.length / tokens.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`  ... avec migrated_at (PumpPortal a notifié)    : ${withMigrated.length}  (${tokens.length ? ((withMigrated.length / tokens.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`  ... curve_completed_at posé, migrated_at ABSENT ENCORE : ${completedNoMigration.length}`);
  console.log(`      (normal si récent : médiane de retard PumpPortal mesurée ~414s ailleurs)`);

  function ageAtCompletion(t) {
    if (!t.curve_completed_at || !t.created_at) return null;
    return (new Date(t.curve_completed_at).getTime() - new Date(t.created_at).getTime()) / 1000;
  }
  console.log('\n' + '='.repeat(78));
  console.log('DÉLAI created_at -> curve_completed_at (secondes) — quand le RPC voit la fin');
  console.log('='.repeat(78));
  stats('Tous les curve_completed_at posés', withCompleted.map(ageAtCompletion));

  console.log('\n' + '='.repeat(78));
  console.log('ÉCART curve_completion_lag_seconds = migrated_at - curve_completed_at');
  console.log('  (positif => le RPC a vu la fin AVANT que PumpPortal ne notifie)');
  console.log('='.repeat(78));
  stats('Tokens avec les deux horloges posées', withBoth.map((t) => t.curve_completion_lag_seconds));
  const negativeLag = withBoth.filter((t) => t.curve_completion_lag_seconds < 0).length;
  console.log(`  n=${withBoth.length}, dont lag négatif (PumpPortal EN AVANCE sur le RPC) : ${negativeLag}`);

  console.log('\n' + '='.repeat(78));
  console.log('Limites de cette mesure :');
  console.log('  - Fenêtre encore courte (~9h de collecte sous le nouveau code) : les');
  console.log('    tokens créés en toute fin de fenêtre n\'ont pas eu le temps d\'atteindre');
  console.log('    leur cascade longue traîne (jusqu\'à 30min) — la couverture réelle finale');
  console.log('    de curve_completed_at pour cette cohorte est probablement sous-estimée ici.');
  console.log('  - queue_wait_ms/rpc_call_ms mesurent la file/l\'appel, PAS le temps réseau');
  console.log('    de la requête HTTP elle-même avant d\'entrer dans le throttle.');
  console.log('='.repeat(78));
}

main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

#!/usr/bin/env node
'use strict';

// Diagnostic (lecture seule) avant d'implémenter un filtre de sélection de
// tokens pour la simulation d'achat passif, demandé le 2026-08-26 :
//   - Créateur : solAmount >= 1 SOL, initialBuy >= 50M tokens
//   - Bonding curve à T+2s : variation vSol >= 25%, vToken >= 10%
//   - Persistance : mouvement encore >= 20% à T+10s
//   - Concentration premiers holders : aucun wallet > 6% de la supply
//
// Objectif de ce script : vérifier deux hypothèses avant de coder le
// filtre en dur, plutôt que de deviner les unités ou l'existence des
// données :
//   1. Unités réelles de raw_new_token_event.solAmount / .initialBuy
//      (SOL entiers ? lamports ? unités brutes token à 6 décimales ?).
//   2. Taux de remplissage réel de top_holders_pct_of_supply /
//      holders_error sur token_snapshots (HANDOFF.md indique un taux
//      d'échec ~100% sur la capture holders — à confirmer avec des
//      chiffres avant de bâtir un critère dessus).
//
// Ne modifie rien. Usage : node scripts/inspect-filter-criteria-availability.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

const PAGE_SIZE = 1000;
async function fetchAllRows(supabase, table, select, orderColumn, applyFilter) {
  const rows = [];
  let from = 0;
  for (;;) {
    let query = supabase.from(table).select(select).order(orderColumn, { ascending: true }).range(from, from + PAGE_SIZE - 1);
    if (applyFilter) query = applyFilter(query);
    const { data, error } = await query;
    if (error) throw new Error(`lecture ${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows;
}

function median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function fmt(v) { return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v; }

const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');
const SAMPLE_SIZE = 500;

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  console.log('='.repeat(78));
  console.log(`Diagnostic disponibilité critères de filtre — ${new Date().toISOString()}`);
  console.log('='.repeat(78));

  const tokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, created_at, initial_virtual_sol_reserves, raw_new_token_event',
    'mint',
    (q) => q.gte('created_at', ANALYSIS_SINCE.toISOString()).limit(SAMPLE_SIZE)
  );
  console.log(`\nTokens échantillonnés : ${tokens.length}`);

  const withRaw = tokens.filter((t) => t.raw_new_token_event);
  const solAmounts = withRaw.map((t) => Number(t.raw_new_token_event.solAmount)).filter((v) => Number.isFinite(v));
  const initialBuys = withRaw.map((t) => Number(t.raw_new_token_event.initialBuy)).filter((v) => Number.isFinite(v));

  console.log('\n-- 1. Unités de raw_new_token_event.solAmount / initialBuy --');
  console.log(`  Exemples bruts (5 premiers tokens avec raw_new_token_event) :`);
  for (const t of withRaw.slice(0, 5)) {
    console.log(`    mint=${t.mint.slice(0, 8)}...  solAmount=${fmt(t.raw_new_token_event.solAmount)}  initialBuy=${fmt(t.raw_new_token_event.initialBuy)}  initial_virtual_sol_reserves(DB)=${fmt(t.initial_virtual_sol_reserves)}`);
  }
  console.log(`  solAmount   : n=${solAmounts.length}  min=${fmt(Math.min(...solAmounts))}  median=${fmt(median(solAmounts))}  max=${fmt(Math.max(...solAmounts))}`);
  console.log(`  initialBuy  : n=${initialBuys.length}  min=${fmt(Math.min(...initialBuys))}  median=${fmt(median(initialBuys))}  max=${fmt(Math.max(...initialBuys))}`);
  console.log(`  Repère : initial_virtual_sol_reserves ≈ 30 + solAmount (établi précédemment) -> si median(solAmount) est de l'ordre de 0.1-10, solAmount est déjà en SOL entiers.`);
  console.log(`  Repère : si initialBuy est de l'ordre de 1e12-1e15, c'est en unités brutes token (6 décimales, diviser par 1e6). Si de l'ordre de 1e6-1e9, c'est probablement déjà en unités "tokens".`);

  console.log('\n-- 2. Disponibilité des données holders (top_holders_pct_of_supply) --');
  const mints = tokens.map((t) => t.mint);
  const MINT_BATCH_SIZE = 150;
  let snapshotsWithHolderCols = [];
  for (let i = 0; i < mints.length; i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const rows = await fetchAllRows(
      supabase,
      'token_snapshots',
      'mint, top_holders_count, top_holders_pct_of_supply, holders_error',
      'id',
      (q) => q.in('mint', batch).not('holders_error', 'is', null).limit(1).order('id')
    );
    snapshotsWithHolderCols.push(...rows);
  }
  // Requête large : combien de lignes ont une valeur non-nulle de
  // top_holders_pct_of_supply vs combien ont un holders_error rempli,
  // sur un échantillon de snapshots (pas seulement 1 par mint).
  let sampleSnapshots = [];
  for (let i = 0; i < Math.min(mints.length, 100); i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const rows = await fetchAllRows(
      supabase,
      'token_snapshots',
      'mint, top_holders_count, top_holders_pct_of_supply, holders_error',
      'id',
      (q) => q.in('mint', batch)
    );
    sampleSnapshots.push(...rows);
  }
  const totalSnap = sampleSnapshots.length;
  const withPct = sampleSnapshots.filter((s) => s.top_holders_pct_of_supply !== null && s.top_holders_pct_of_supply !== undefined).length;
  const withError = sampleSnapshots.filter((s) => s.holders_error !== null && s.holders_error !== undefined).length;
  console.log(`  Snapshots inspectés (premiers ${Math.min(mints.length, 100)} mints) : ${totalSnap}`);
  console.log(`  ... avec top_holders_pct_of_supply rempli : ${withPct} (${totalSnap ? ((withPct / totalSnap) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`  ... avec holders_error rempli (échec)     : ${withError} (${totalSnap ? ((withError / totalSnap) * 100).toFixed(1) : 'n/a'}%)`);
  if (withError > 0) {
    const sampleErrors = [...new Set(sampleSnapshots.filter((s) => s.holders_error).map((s) => s.holders_error))].slice(0, 3);
    console.log(`  Exemples de holders_error : ${JSON.stringify(sampleErrors)}`);
  }

  console.log('\n' + '='.repeat(78));
  console.log('Ce diagnostic ne construit aucun filtre — sert uniquement à calibrer les');
  console.log('unités et vérifier la disponibilité réelle des données avant de coder le');
  console.log('filtre demandé par l\'utilisateur.');
  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

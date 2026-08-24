#!/usr/bin/env node
'use strict';

// Preuve de concept — LECTURE SEULE, ne modifie ni le listener ni le
// schéma, ne remplace ni ne supprime aucune donnée PumpPortal.
//
// Contexte (2026-08-24, voir la section CALIBRATION de src/report.js) :
// l'événement subscribeMigration de PumpPortal peut arriver plusieurs
// minutes après l'état réel on-chain (médiane ~414s, jusqu'à ~35min sur
// l'échantillon observé). migrated_at/time_to_migration_seconds ne sont
// donc PAS une horloge fiable pour savoir, à un âge donné, si un token
// avait déjà fini sa bonding curve.
//
// Ce script construit une notion parallèle, curve_completed_at_observed,
// dérivée uniquement de l'état RPC déjà collecté dans token_snapshots :
// la première lecture montrant complete=true (dans raw_event) ou des
// réserves vidées (virtual_sol_reserves=0). Cette valeur est un
// MAJORANT, pas l'instant exact : la vraie complétion a eu lieu quelque
// part entre la dernière lecture "vivante" et cette première lecture
// "terminée" — la largeur de cet intervalle est affichée, pas cachée.
//
// Question posée : avec cette horloge RPC plutôt que la notification
// PumpPortal, la bonding curve montre-t-elle une différence entre les
// futurs B et les C AVANT la complétion ? Aucun score, aucun seuil de
// décision, aucun filtre construit ici — uniquement une comparaison de
// distributions, comme le reste de src/report.js.
//
// Usage : node scripts/curve-completion-analysis.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

// Même limite/pagination que src/report.js (PostgREST plafonne à 1000
// lignes par requête).
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

// .in('mint', [...]) sur des milliers de mints dépasse la longueur d'URL
// acceptée par PostgREST (voir src/report.js, repéré le 2026-08-23) —
// même découpage en lots.
const MINT_BATCH_SIZE = 150;

async function fetchSnapshotsForMints(supabase, mints) {
  const rows = [];
  for (let i = 0; i < mints.length; i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const batchRows = await fetchAllRows(supabase, 'token_snapshots', 'mint, age_seconds, nominal_delay_s, virtual_sol_reserves, raw_event', 'id', (q) =>
      q.in('mint', batch)
    );
    rows.push(...batchRows);
  }
  return rows;
}

function mean(arr) {
  return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : null;
}

function median(arr) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function percentile(arr, p) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function fmt(v, digits = 4) {
  return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v.toFixed(digits);
}

function printProfileStats(name, values) {
  const clean = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!clean.length) {
    console.log(`      ${name.padEnd(42)} n=0`);
    return;
  }
  const [p10, p25, p50, p75, p90] = [10, 25, 50, 75, 90].map((p) => fmt(percentile(clean, p), 4));
  console.log(`      ${name.padEnd(42)} n=${String(clean.length).padStart(4)}  P10=${p10} P25=${p25} P50=${p50} P75=${p75} P90=${p90}`);
}

// Même seuil que src/listener.js (ACTIVITY_REL_DEV_THRESHOLD), calibré le
// 2026-08-23 sur données réelles — voir scripts/calibrate-activity-threshold.js.
const ACTIVITY_REL_DEV_THRESHOLD = Number(process.env.ACTIVITY_REL_DEV_THRESHOLD) || 1e-4;
const CHECKPOINTS_S = [5, 10, 20, 30];

// Même fenêtre que la section CASCADE V2 de src/report.js : avant le
// hotfix du 2026-08-23 21:21 UTC, une partie des lectures T+2s/T+5s a pu
// échouer (course upsert/cascade), faussant la disponibilité des
// snapshots les plus précoces.
const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');

function isCompletedSnapshot(s) {
  const completeFlag = !!(s.raw_event && s.raw_event.complete === true);
  const drained = s.virtual_sol_reserves === 0;
  return completeFlag || drained;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  console.log('='.repeat(72));
  console.log(`Preuve de concept — curve_completed_at_observed (RPC) — ${new Date().toISOString()}`);
  console.log('='.repeat(72));

  const allTokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, created_at, migrated, migrated_at, time_to_migration_seconds, initial_virtual_sol_reserves, raw_new_token_event',
    'mint'
  );
  const tokens = allTokens.filter(
    (t) => t.raw_new_token_event && t.created_at && new Date(t.created_at) >= ANALYSIS_SINCE && t.initial_virtual_sol_reserves
  );
  console.log(`\nTokens analysés (création observée, créés depuis ${ANALYSIS_SINCE.toISOString()}) : ${tokens.length}`);

  const mints = tokens.map((t) => t.mint);
  const snapshots = await fetchSnapshotsForMints(supabase, mints);
  console.log(`Snapshots RPC récupérés : ${snapshots.length}`);

  const snapshotsByMint = new Map();
  for (const s of snapshots) {
    if (!snapshotsByMint.has(s.mint)) snapshotsByMint.set(s.mint, []);
    snapshotsByMint.get(s.mint).push(s);
  }
  for (const arr of snapshotsByMint.values()) arr.sort((a, b) => a.age_seconds - b.age_seconds);

  // Pour chaque token : 1re lecture "terminée" (complete=true ou réserves
  // vidées) et dernière lecture "vivante" strictement avant elle — c'est
  // l'intervalle d'incertitude, pas un instant exact.
  const perToken = new Map();
  for (const t of tokens) {
    const rows = snapshotsByMint.get(t.mint) || [];
    let firstCompletedAge = null;
    let lastAliveAge = null;
    for (const s of rows) {
      if (isCompletedSnapshot(s)) {
        firstCompletedAge = s.age_seconds;
        break;
      }
      lastAliveAge = s.age_seconds;
    }
    perToken.set(t.mint, { firstCompletedAge, lastAliveAge, snapshotCount: rows.length });
  }

  // Nouveau classement, basé UNIQUEMENT sur l'état RPC observé.
  const groupOf = new Map();
  for (const t of tokens) {
    const info = perToken.get(t.mint);
    if (info.firstCompletedAge === null) groupOf.set(t.mint, 'C');
    else if (info.firstCompletedAge <= 10) groupOf.set(t.mint, 'A');
    else groupOf.set(t.mint, 'B');
  }
  const countA = [...groupOf.values()].filter((g) => g === 'A').length;
  const countB = [...groupOf.values()].filter((g) => g === 'B').length;
  const countC = [...groupOf.values()].filter((g) => g === 'C').length;

  console.log('\nNouveau classement (curve_completed_at_observed, RPC — PAS pumpportal_migration_at) :');
  console.log(`  A. terminé ≤10s (RPC)      : ${countA}`);
  console.log(`  B. terminé >10s (RPC)      : ${countB}`);
  console.log(`  C. jamais vu terminé (RPC) : ${countC}`);
  console.log('  ATTENTION : "C" veut dire "aucune lecture retenue ne montre la curve');
  console.log('  terminée", PAS "n\'a jamais migré" — voir limites en bas de ce rapport.');

  // Écart avec le classement PumpPortal existant : combien de tokens
  // changent de catégorie, et surtout combien de "C" PumpPortal (pas
  // encore migré selon l'événement WS) ont en réalité déjà une curve
  // terminée côté RPC — c'est exactement le décalage quantifié dans
  // src/report.js (section CALIBRATION), vu ici token par token.
  const detail = [];
  for (const t of tokens) {
    const rpcGroup = groupOf.get(t.mint);
    const pumpportalGroup = !t.migrated ? 'C' : t.time_to_migration_seconds !== null && t.time_to_migration_seconds !== undefined && t.time_to_migration_seconds <= 10 ? 'A' : 'B';
    if (rpcGroup !== pumpportalGroup) {
      detail.push({ mint: t.mint, rpcGroup, pumpportalGroup, firstCompletedAge: perToken.get(t.mint).firstCompletedAge, ttm: t.time_to_migration_seconds });
    }
  }
  const pumpportalCbutRpcDone = detail.filter((d) => d.pumpportalGroup === 'C' && d.rpcGroup !== 'C');
  console.log(`\nTokens dont le classement change entre PumpPortal et RPC observé : ${detail.length}/${tokens.length}`);
  console.log(`  dont PumpPortal="C" (pas encore migré) mais RPC a déjà vu la curve terminée : ${pumpportalCbutRpcDone.length}`);
  if (pumpportalCbutRpcDone.length) {
    printProfileStats('âge de la 1re lecture "terminée" pour ces cas (s)', pumpportalCbutRpcDone.map((d) => d.firstCompletedAge));
  }

  // Largeur de la fenêtre d'incertitude sur l'instant réel de complétion.
  const withCompletion = tokens.filter((t) => perToken.get(t.mint).firstCompletedAge !== null);
  const withBothBounds = withCompletion.filter((t) => perToken.get(t.mint).lastAliveAge !== null);
  console.log('\nIncertitude sur l\'instant de complétion :');
  console.log(`  tokens avec une lecture "terminée" observée              : ${withCompletion.length}`);
  console.log(`  ... dont avec une lecture "vivante" juste avant (encadré) : ${withBothBounds.length}`);
  console.log(`  ... dont SANS lecture vivante avant (déjà terminé à la 1re lecture reçue, borne inférieure inconnue) : ${withCompletion.length - withBothBounds.length}`);
  if (withBothBounds.length) {
    const windowSizes = withBothBounds.map((t) => {
      const i = perToken.get(t.mint);
      return i.firstCompletedAge - i.lastAliveAge;
    });
    printProfileStats('largeur de la fenêtre d\'incertitude (s)', windowSizes);
  }

  // Comparaison B (pré-completion uniquement) vs C, aux âges comparables.
  console.log('\n' + '='.repeat(72));
  console.log('COMPARAISON B vs C — horloge RPC (curve_completed_at_observed)');
  console.log(`  B = curve terminée >10s après création (n=${countB}) — lectures POST-`);
  console.log('  completion exclues de cette section. C = curve jamais vue terminée');
  console.log(`  dans les lectures retenues (n=${countC}).`);
  console.log('='.repeat(72));

  const bMints = [...groupOf.entries()].filter(([, g]) => g === 'B').map(([m]) => m);
  const cMints = [...groupOf.entries()].filter(([, g]) => g === 'C').map(([m]) => m);
  const initialByMint = new Map(tokens.map((t) => [t.mint, t.initial_virtual_sol_reserves]));

  for (const d of CHECKPOINTS_S) {
    console.log(`\n  --- T+${d}s ---`);
    for (const [label, mintList, isB] of [
      ['B (pré-completion)', bMints, true],
      ['C (jamais vu terminé)', cMints, false],
    ]) {
      const rows = [];
      for (const mint of mintList) {
        const snaps = snapshotsByMint.get(mint) || [];
        const snap = snaps.find((s) => s.nominal_delay_s === d);
        if (!snap) continue;
        const info = perToken.get(mint);
        if (info.firstCompletedAge !== null && snap.age_seconds >= info.firstCompletedAge) continue; // post-completion, exclu
        const initial = initialByMint.get(mint);
        const ratio = initial ? snap.virtual_sol_reserves / initial : null;
        const relDev = initial ? Math.abs(snap.virtual_sol_reserves - initial) / initial : null;
        rows.push({
          vSol: snap.virtual_sol_reserves,
          ratio,
          relDev,
          gapToCompletion: info.firstCompletedAge !== null ? info.firstCompletedAge - snap.age_seconds : null,
        });
      }
      console.log(`    ${label.padEnd(24)} observés pré-completion à ce point : ${rows.length}/${mintList.length}`);
      if (rows.length) {
        printProfileStats('vSol', rows.map((r) => r.vSol));
        printProfileStats('vSol / vSol_initial', rows.map((r) => r.ratio));
        printProfileStats('écart relatif |Δ|/initial', rows.map((r) => r.relDev));
        const active = rows.filter((r) => r.relDev !== null && r.relDev > ACTIVITY_REL_DEV_THRESHOLD);
        console.log(`      actifs (écart relatif > ${ACTIVITY_REL_DEV_THRESHOLD})           n=${rows.length}  ${active.length}/${rows.length} (${((active.length / rows.length) * 100).toFixed(1)}%)`);
        if (isB) printProfileStats('délai jusqu\'à la complétion observée (s)', rows.map((r) => r.gapToCompletion));
      }
    }
  }

  console.log('\n' + '='.repeat(72));
  console.log('Limites connues de cette preuve de concept :');
  console.log('  - "C" veut dire "aucune lecture retenue ne montre la curve terminée",');
  console.log('    jamais "n\'a jamais migré" : certains ont pu migrer après notre');
  console.log('    dernière lecture, ou entre deux lectures sans qu\'aucune n\'ait');
  console.log('    capturé l\'état terminé (cascade arrêtée si jugée inactive au gate).');
  console.log('  - curve_completed_at_observed est un MAJORANT de l\'instant réel, pas');
  console.log('    une seconde exacte — voir la largeur de fenêtre d\'incertitude ci-dessus.');
  console.log('  - Échantillon B encore petit (voir comptages ci-dessus) — repère,');
  console.log('    pas une conclusion statistique.');
  console.log('  - pumpportal_migration_at (colonne migrated_at) n\'est ni modifiée ni');
  console.log('    supprimée par ce script : tout ce qui précède est une notion');
  console.log('    parallèle, calculée à la volée à partir de token_snapshots.');
  console.log('='.repeat(72));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

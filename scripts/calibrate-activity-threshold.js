#!/usr/bin/env node
'use strict';

// Diagnostic read-only : calibre le seuil d'écart relatif (vs
// initial_virtual_sol_reserves) qui distingue le bruit numérique d'un
// vrai mouvement de trading, sur les snapshots déjà collectés en
// production — avant d'implémenter le gate à deux niveaux (V2).
//
// Ne touche à rien, n'écrit rien. Lit token_snapshots + tokens.
//
// Usage : node scripts/calibrate-activity-threshold.js

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

function percentile(arr, p) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

// Bornes log — le bruit flottant pur se compte en 1e-9 à 1e-7, un vrai
// trade même minuscule (quelques centimes de SOL contre ~30 SOL de
// réserve) déplace le ratio d'au moins 1e-4 à 1e-3. On cherche le "trou"
// entre les deux dans les données réelles plutôt que de le supposer.
const REL_DEV_BINS = [0, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1, 1];

function bucketLabel(i) {
  if (i === 0) return '=0 (identique)';
  if (i === REL_DEV_BINS.length) return `>${REL_DEV_BINS[REL_DEV_BINS.length - 1]}`;
  return `${REL_DEV_BINS[i - 1]}-${REL_DEV_BINS[i]}`;
}

function histogram(values) {
  const counts = new Array(REL_DEV_BINS.length + 1).fill(0);
  for (const v of values) {
    if (v === 0) {
      counts[0] += 1;
      continue;
    }
    let i = REL_DEV_BINS.findIndex((b) => v <= b);
    if (i === -1) i = REL_DEV_BINS.length;
    if (i === 0) i = 1; // v>0 mais <= prembattrait 0 (impossible ici, garde-fou)
    counts[i] += 1;
  }
  return counts;
}

function printHistogram(counts, total) {
  const maxCount = Math.max(...counts, 1);
  const barWidth = 40;
  for (let i = 0; i < counts.length; i++) {
    const bar = '#'.repeat(Math.round((counts[i] / maxCount) * barWidth));
    const pct = total ? ((counts[i] / total) * 100).toFixed(2) : '0.00';
    console.log(`    ${bucketLabel(i).padEnd(18)} ${bar.padEnd(barWidth)} ${String(counts[i]).padStart(7)} (${pct}%)`);
  }
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  console.log('Lecture des tokens (initial_virtual_sol_reserves, groupe)...');
  const tokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, initial_virtual_sol_reserves, migrated, time_to_migration_seconds, raw_new_token_event',
    'mint',
    (q) => q.not('raw_new_token_event', 'is', null)
  );
  const tokenByMint = new Map(tokens.map((t) => [t.mint, t]));
  console.log(`  ${tokens.length} tokens avec création observée.`);

  console.log('Lecture de token_snapshots...');
  const snapshots = await fetchAllRows(supabase, 'token_snapshots', 'mint, age_seconds, virtual_sol_reserves', 'id');
  console.log(`  ${snapshots.length} snapshots au total.`);

  const zeroed = [];
  const live = []; // { relDev, group, ageSeconds }

  for (const s of snapshots) {
    const t = tokenByMint.get(s.mint);
    if (!t || t.initial_virtual_sol_reserves === null || t.initial_virtual_sol_reserves === undefined) continue;
    const initial = Number(t.initial_virtual_sol_reserves);
    const observed = Number(s.virtual_sol_reserves);
    if (!initial || Number.isNaN(observed)) continue;
    if (observed === 0) {
      zeroed.push(s);
      continue;
    }
    const relDev = Math.abs(observed - initial) / initial;
    const group = !t.migrated ? 'C' : t.time_to_migration_seconds <= 10 ? 'A' : 'B';
    live.push({ relDev, group, ageSeconds: s.age_seconds });
  }

  console.log('\n' + '='.repeat(72));
  console.log(`Snapshots à 0 (déjà migrés/vidés au moment du relevé) : ${zeroed.length} (${((zeroed.length / snapshots.length) * 100).toFixed(1)}%)`);
  console.log(`Snapshots "vivants" analysés (réserve > 0) : ${live.length}`);
  console.log('='.repeat(72));

  const allRelDev = live.map((l) => l.relDev);
  console.log('\nDistribution de l\'écart relatif |observé - initial| / initial, TOUS groupes confondus :');
  printHistogram(histogram(allRelDev), live.length);
  console.log(
    `\n  percentiles : P50=${percentile(allRelDev, 50)?.toExponential(2)} | P75=${percentile(allRelDev, 75)?.toExponential(2)} | ` +
      `P90=${percentile(allRelDev, 90)?.toExponential(2)} | P95=${percentile(allRelDev, 95)?.toExponential(2)} | P99=${percentile(allRelDev, 99)?.toExponential(2)}`
  );

  for (const group of ['A', 'B', 'C']) {
    const vals = live.filter((l) => l.group === group).map((l) => l.relDev);
    console.log(`\n--- Groupe ${group} (n=${vals.length}) ---`);
    printHistogram(histogram(vals), vals.length);
  }

  console.log('\n' + '='.repeat(72));
  console.log('Fraction de snapshots "vivants" classés actifs selon divers seuils candidats :');
  console.log('='.repeat(72));
  const CANDIDATE_THRESHOLDS = [1e-6, 1e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2];
  for (const th of CANDIDATE_THRESHOLDS) {
    const activeAll = allRelDev.filter((v) => v > th).length;
    const activeB = live.filter((l) => l.group === 'B' && l.relDev > th).length;
    const totalB = live.filter((l) => l.group === 'B').length;
    const activeC = live.filter((l) => l.group === 'C' && l.relDev > th).length;
    const totalC = live.filter((l) => l.group === 'C').length;
    console.log(
      `  seuil > ${th.toExponential(0)} : ${((activeAll / live.length) * 100).toFixed(2)}% actifs (tous) | ` +
        `B: ${totalB ? ((activeB / totalB) * 100).toFixed(1) : 'n/a'}% | C: ${totalC ? ((activeC / totalC) * 100).toFixed(1) : 'n/a'}%`
    );
  }

  console.log('\n' + '='.repeat(72));
  console.log('Répartition par âge (30/60/180/300s), seuil candidat 1e-4 :');
  console.log('='.repeat(72));
  const NOMINAL = [30, 60, 180, 300];
  for (const d of NOMINAL) {
    const pts = live.filter((l) => Math.abs(l.ageSeconds - d) < 15);
    const active = pts.filter((l) => l.relDev > 1e-4).length;
    const activeB = pts.filter((l) => l.group === 'B' && l.relDev > 1e-4).length;
    const totalB = pts.filter((l) => l.group === 'B').length;
    const activeC = pts.filter((l) => l.group === 'C' && l.relDev > 1e-4).length;
    const totalC = pts.filter((l) => l.group === 'C').length;
    console.log(
      `  ~${d}s : n=${pts.length}, actifs=${active} (${pts.length ? ((active / pts.length) * 100).toFixed(1) : 'n/a'}%)` +
        ` — B: ${totalB ? ((activeB / totalB) * 100).toFixed(1) : 'n/a'}% (n=${totalB}) | C: ${totalC ? ((activeC / totalC) * 100).toFixed(1) : 'n/a'}% (n=${totalC})`
    );
  }
  console.log('='.repeat(72));
}

main().catch((err) => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});

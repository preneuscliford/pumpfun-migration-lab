#!/usr/bin/env node
'use strict';

// Analyse ciblée (lecture seule), demandée le 2026-08-25 après la
// validation vSol/vToken (scripts/validate-bc-trajectory-gap.js, écart
// confirmé, résiste aux biais de timing/mesure) : à quel âge du token la
// séparation B/C devient-elle déjà nette ? Même méthodologie de sélection
// que la validation (âge RÉEL via started_at, tolérance symétrique B/C),
// étendue à T+2s. Pour chaque checkpoint : couverture réelle, P25/P50/
// P75/P90 de |Δ|/initial (vSol et vToken), % de tokens dépassant 10%/25%/
// 50% de variation (repère descriptif, PAS un seuil de décision/trading),
// âge réel médian obtenu.
//
// Aucun score, seuil de décision ou stratégie construit ici — uniquement
// des statistiques descriptives, comme le reste de cette série d'analyses.
// Ne touche ni ne modifie le collecteur ni le schéma.
//
// Usage : node scripts/analyze-bc-early-detection.js

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

const MINT_BATCH_SIZE = 150;
async function fetchSnapshotsForMints(supabase, mints, select) {
  const rows = [];
  for (let i = 0; i < mints.length; i += MINT_BATCH_SIZE) {
    const batch = mints.slice(i, i + MINT_BATCH_SIZE);
    const batchRows = await fetchAllRows(supabase, 'token_snapshots', select, 'id', (q) => q.in('mint', batch));
    rows.push(...batchRows);
  }
  return rows;
}

function median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}
function percentile(arr, p) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const idx = (p / 100) * (s.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return lo === hi ? s[lo] : s[lo] + (s[hi] - s[lo]) * (idx - lo);
}
function fmt(v, d = 4) { return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v.toFixed(d); }
function pctStats(name, values) {
  const c = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!c.length) { console.log(`      ${name.padEnd(24)} n=0`); return; }
  console.log(`      ${name.padEnd(24)} n=${String(c.length).padStart(6)}  P25=${fmt(percentile(c,25))} P50=${fmt(median(c))} P75=${fmt(percentile(c,75))} P90=${fmt(percentile(c,90))}`);
}
function crossingRates(values) {
  const c = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!c.length) return { n: 0, p10: null, p25: null, p50: null };
  return {
    n: c.length,
    p10: (c.filter((v) => v > 0.10).length / c.length) * 100,
    p25: (c.filter((v) => v > 0.25).length / c.length) * 100,
    p50: (c.filter((v) => v > 0.50).length / c.length) * 100,
  };
}

const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');
const CHECKPOINTS_S = [2, 5, 10, 20, 30];
// Tolérance d'âge réel — même principe que validate-bc-trajectory-gap.js,
// ±1s pour T+2s (gate le plus serré), reste identique aux checkpoints
// déjà utilisés.
const AGE_TOLERANCE_S = { 2: 1, 5: 2, 10: 3, 20: 5, 30: 7 };

function isCompletedSnapshot(s) {
  return !!(s.raw_event && s.raw_event.complete === true) || s.virtual_sol_reserves === 0;
}
function realAgeSeconds(s, createdAtMs) {
  if (s.started_at) return (new Date(s.started_at).getTime() - createdAtMs) / 1000;
  return s.age_seconds;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  console.log('='.repeat(78));
  console.log(`Apparition temporelle du signal B vs C — ${new Date().toISOString()}`);
  console.log('='.repeat(78));

  const allTokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, created_at, initial_virtual_sol_reserves, initial_virtual_token_reserves, raw_new_token_event',
    'mint'
  );
  const tokens = allTokens.filter((t) => t.raw_new_token_event && t.created_at && new Date(t.created_at) >= ANALYSIS_SINCE && t.initial_virtual_sol_reserves && t.initial_virtual_token_reserves);
  console.log(`\nTokens dans la fenêtre : ${tokens.length}`);
  const createdAtByMint = new Map(tokens.map((t) => [t.mint, Date.parse(t.created_at)]));
  const initialSolByMint = new Map(tokens.map((t) => [t.mint, t.initial_virtual_sol_reserves]));
  const initialTokenByMint = new Map(tokens.map((t) => [t.mint, Number(t.initial_virtual_token_reserves)]));

  const mints = tokens.map((t) => t.mint);
  const snapshots = await fetchSnapshotsForMints(
    supabase,
    mints,
    'mint, age_seconds, nominal_delay_s, virtual_sol_reserves, virtual_token_reserves, raw_event, started_at'
  );
  console.log(`Snapshots récupérés : ${snapshots.length}`);

  const snapshotsByMint = new Map();
  for (const s of snapshots) {
    if (!snapshotsByMint.has(s.mint)) snapshotsByMint.set(s.mint, []);
    snapshotsByMint.get(s.mint).push(s);
  }
  for (const [mint, arr] of snapshotsByMint) {
    const createdAtMs = createdAtByMint.get(mint);
    for (const s of arr) s._realAge = realAgeSeconds(s, createdAtMs);
    arr.sort((a, b) => a._realAge - b._realAge);
  }

  // Classement A/B/C — identique aux analyses précédentes.
  const perToken = new Map();
  for (const t of tokens) {
    const rows = snapshotsByMint.get(t.mint) || [];
    let firstCompletedAge = null;
    for (const s of rows) {
      if (isCompletedSnapshot(s)) { firstCompletedAge = s._realAge; break; }
    }
    perToken.set(t.mint, { firstCompletedAge });
  }
  const groupOf = new Map();
  for (const t of tokens) {
    const info = perToken.get(t.mint);
    if (info.firstCompletedAge === null) groupOf.set(t.mint, 'C');
    else if (info.firstCompletedAge <= 10) groupOf.set(t.mint, 'A');
    else groupOf.set(t.mint, 'B');
  }
  const bMints = [...groupOf.entries()].filter(([, g]) => g === 'B').map(([m]) => m);
  const cMints = [...groupOf.entries()].filter(([, g]) => g === 'C').map(([m]) => m);
  console.log(`Groupes : B=${bMints.length}  C=${cMints.length}`);

  function pickComparableSnapshot(mint, d) {
    const rows = snapshotsByMint.get(mint) || [];
    const info = perToken.get(mint);
    let best = null, bestDiff = Infinity;
    for (const s of rows) {
      if (info.firstCompletedAge !== null && s._realAge >= info.firstCompletedAge) continue;
      const diff = Math.abs(s._realAge - d);
      if (diff <= AGE_TOLERANCE_S[d] && diff < bestDiff) { best = s; bestDiff = diff; }
    }
    return best;
  }

  const summary = [];

  for (const d of CHECKPOINTS_S) {
    console.log('\n' + '='.repeat(78));
    console.log(`CHECKPOINT ~T+${d}s — tolérance ±${AGE_TOLERANCE_S[d]}s`);
    console.log('='.repeat(78));

    const rowByGroup = {};
    for (const [label, mintList] of [['B', bMints], ['C', cMints]]) {
      const picked = [];
      for (const mint of mintList) {
        const s = pickComparableSnapshot(mint, d);
        if (!s) continue;
        const initSol = initialSolByMint.get(mint);
        const initTok = initialTokenByMint.get(mint);
        const vTok = Number(s.virtual_token_reserves) / 1e6; // base units -> humaines, voir validate-bc-trajectory-gap.js
        picked.push({
          realAge: s._realAge,
          relDevSol: initSol ? Math.abs(s.virtual_sol_reserves - initSol) / initSol : null,
          relDevToken: initTok ? Math.abs(vTok - initTok) / initTok : null,
        });
      }
      rowByGroup[label] = picked;

      console.log(`\n  --- ${label} ---`);
      console.log(`    Couverture réelle : ${picked.length}/${mintList.length}`);
      console.log(`    Âge réel médian obtenu : ${fmt(median(picked.map((r) => r.realAge)), 2)}s (cible ${d}s)`);
      pctStats('P25/P50/P75/P90 vSol', picked.map((r) => r.relDevSol));
      pctStats('P25/P50/P75/P90 vToken', picked.map((r) => r.relDevToken));
      const crSol = crossingRates(picked.map((r) => r.relDevSol));
      const crTok = crossingRates(picked.map((r) => r.relDevToken));
      console.log(`      % vSol   >10%=${fmt(crSol.p10,1)}  >25%=${fmt(crSol.p25,1)}  >50%=${fmt(crSol.p50,1)}  (n=${crSol.n})`);
      console.log(`      % vToken >10%=${fmt(crTok.p10,1)}  >25%=${fmt(crTok.p25,1)}  >50%=${fmt(crTok.p50,1)}  (n=${crTok.n})`);
      summary.push({ d, label, n: picked.length, crSol, crTok, medianAge: median(picked.map((r) => r.realAge)) });
    }
  }

  console.log('\n' + '='.repeat(78));
  console.log('SÉPARATION B/C PAR SEUIL DE VARIATION — vue synthétique (vSol)');
  console.log('  (% de tokens dépassant le seuil, B vs C, par checkpoint — pas un seuil');
  console.log('  de décision, juste un repère de lisibilité de la séparation)');
  console.log('='.repeat(78));
  console.log('  T+s   n(B)  B>10%  B>25%  B>50%    n(C)   C>10%  C>25%  C>50%');
  for (const d of CHECKPOINTS_S) {
    const b = summary.find((s) => s.d === d && s.label === 'B');
    const c = summary.find((s) => s.d === d && s.label === 'C');
    console.log(
      `  ${String(d).padStart(3)}  ${String(b.n).padStart(5)}  ${fmt(b.crSol.p10,1).padStart(5)}  ${fmt(b.crSol.p25,1).padStart(5)}  ${fmt(b.crSol.p50,1).padStart(5)}   ${String(c.n).padStart(6)}  ${fmt(c.crSol.p10,1).padStart(5)}  ${fmt(c.crSol.p25,1).padStart(5)}  ${fmt(c.crSol.p50,1).padStart(5)}`
    );
  }

  console.log('\n' + '='.repeat(78));
  console.log('Limites : mêmes que les analyses précédentes (C = "jamais vu terminé');
  console.log('dans nos lectures", échantillon B petit à chaque point). Aucun score,');
  console.log('seuil de décision ou stratégie construit ici.');
  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

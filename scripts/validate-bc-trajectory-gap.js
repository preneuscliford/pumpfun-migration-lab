#!/usr/bin/env node
'use strict';

// Validation ciblée (lecture seule), demandée le 2026-08-25 après le
// résultat de scripts/curve-completion-analysis.js (écart massif de
// trajectoire B vs C sur l'horloge RPC, écart relatif |Δ|/initial P50 de
// ~0.80-0.93 pour B contre ~0.03-0.09 pour C aux checkpoints 5/10/20/30s).
// Ne construit AUCUN score, seuil de décision ou filtre de production —
// vérifie uniquement si cet écart résiste à des biais de mesure évidents :
//
//   1. Combien de B ont réellement un snapshot pré-completion À ÂGE RÉEL
//      comparable à chaque checkpoint (pas juste "labellisé" T+5s) ?
//   2. B et C sont comparés avec EXACTEMENT la même définition d'âge réel
//      — started_at (l'instant RÉEL de l'appel RPC, pas la cible visée)
//      quand disponible, repli sur age_seconds pour les lignes d'avant
//      cette instrumentation (2026-08-25).
//   3. Médiane/P25/P75/P90 de la variation relative des réserves, B vs C.
//   4. Distribution brute (histogramme par tranches), pas que la médiane.
//   5. Le résultat tient-il en excluant : tokens dont la completion est
//      trop proche du checkpoint (B) ; snapshots à queue_wait_ms élevé ;
//      observations dont l'âge réel dérive trop de la cible (retard de
//      file) ?
//   6. vSol et vToken racontent-ils la même histoire ?
//   7. B et C ont-ils un âge réel réellement comparable à chaque point ?
//
// Question unique : l'écart de trajectoire B/C reste-t-il massif une fois
// les biais évidents de timing/mesure éliminés ?
//
// Usage : node scripts/validate-bc-trajectory-gap.js

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
function stats(name, values) {
  const c = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!c.length) { console.log(`        ${name.padEnd(28)} n=0`); return; }
  console.log(`        ${name.padEnd(28)} n=${String(c.length).padStart(6)}  P10=${fmt(percentile(c,10))} P25=${fmt(percentile(c,25))} P50=${fmt(median(c))} P75=${fmt(percentile(c,75))} P90=${fmt(percentile(c,90))}`);
}
// Distribution BRUTE par tranches (point 4) — pas que des percentiles.
const REL_DEV_BINS = [0, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, Infinity];
function histogram(name, values) {
  const c = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!c.length) { console.log(`        ${name.padEnd(28)} n=0`); return; }
  const counts = new Array(REL_DEV_BINS.length - 1).fill(0);
  for (const v of c) {
    for (let i = 0; i < REL_DEV_BINS.length - 1; i += 1) {
      if (v >= REL_DEV_BINS[i] && v < REL_DEV_BINS[i + 1]) { counts[i] += 1; break; }
    }
  }
  console.log(`        ${name} (n=${c.length}) :`);
  for (let i = 0; i < counts.length; i += 1) {
    const lo = REL_DEV_BINS[i], hi = REL_DEV_BINS[i + 1];
    const label = hi === Infinity ? `>=${lo}` : `[${lo}-${hi})`;
    const pct = ((counts[i] / c.length) * 100).toFixed(1);
    console.log(`          ${label.padEnd(10)} ${String(counts[i]).padStart(6)}  (${pct}%)`);
  }
}

const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');
const CHECKPOINTS_S = [5, 10, 20, 30];
// Tolérance d'âge réel autour de chaque checkpoint — point 5, exclut les
// snapshots dont l'exécution réelle a trop dérivé de la cible visée
// (retard de file), symétriquement pour B et C.
const AGE_TOLERANCE_S = { 5: 2, 10: 3, 20: 5, 30: 7 };
// Point 5a : exclut les B dont la completion observée est trop proche du
// checkpoint (le résultat ne doit pas venir juste de "on regarde juste
// avant l'effondrement").
const MIN_GAP_TO_COMPLETION_S = 3;
// Point 5b : exclut les snapshots dont queue_wait_ms est très élevé —
// robustesse séparée, même si la tolérance d'âge réel ci-dessus couvre
// déjà une bonne part de ce biais.
const HIGH_QUEUE_WAIT_MS = 5000;

function isCompletedSnapshot(s) {
  return !!(s.raw_event && s.raw_event.complete === true) || s.virtual_sol_reserves === 0;
}
// Âge RÉEL au moment de l'appel RPC (started_at), pas l'âge visé au
// moment où le timer de cascade s'est déclenché (age_seconds, capturé
// AVANT l'attente en file — voir le post-mortem du throttle adaptatif
// dans src/listener.js). Repli sur age_seconds pour les lignes d'avant
// l'ajout de started_at (2026-08-25).
function realAgeSeconds(s, createdAtMs) {
  if (s.started_at) return (new Date(s.started_at).getTime() - createdAtMs) / 1000;
  return s.age_seconds;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  console.log('='.repeat(78));
  console.log(`Validation ciblée B vs C — ${new Date().toISOString()}`);
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
    'mint, age_seconds, nominal_delay_s, virtual_sol_reserves, virtual_token_reserves, raw_event, started_at, queue_wait_ms'
  );
  console.log(`Snapshots récupérés : ${snapshots.length}`);
  const withStartedAt = snapshots.filter((s) => s.started_at).length;
  console.log(`  ... dont avec started_at (âge réel exact) : ${withStartedAt} (${((withStartedAt / snapshots.length) * 100).toFixed(1)}%) — repli sur age_seconds pour le reste.`);

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

  // Classement A/B/C — IDENTIQUE à curve-completion-analysis.js (même
  // définition, pour comparer au même B/C que le résultat précédent).
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
  console.log(`Groupes (identiques à l'analyse précédente) : B=${bMints.length}  C=${cMints.length}`);

  // Sélectionne, pour un mint et un checkpoint donné, le snapshot dont
  // l'ÂGE RÉEL est le plus proche de la cible, DANS la tolérance —
  // exactement la même procédure pour B et C (point 2).
  function pickComparableSnapshot(mint, d) {
    const rows = snapshotsByMint.get(mint) || [];
    const info = perToken.get(mint);
    let best = null, bestDiff = Infinity;
    for (const s of rows) {
      if (info.firstCompletedAge !== null && s._realAge >= info.firstCompletedAge) continue; // post-completion exclu
      const diff = Math.abs(s._realAge - d);
      if (diff <= AGE_TOLERANCE_S[d] && diff < bestDiff) { best = s; bestDiff = diff; }
    }
    return best;
  }

  for (const d of CHECKPOINTS_S) {
    console.log('\n' + '='.repeat(78));
    console.log(`CHECKPOINT ~T+${d}s — âge réel (started_at quand dispo), tolérance ±${AGE_TOLERANCE_S[d]}s`);
    console.log('='.repeat(78));

    for (const [label, mintList, isB] of [
      ['B (pré-completion)', bMints, true],
      ['C (jamais vu terminé)', cMints, false],
    ]) {
      const picked = [];
      for (const mint of mintList) {
        const s = pickComparableSnapshot(mint, d);
        if (!s) continue;
        const initSol = initialSolByMint.get(mint);
        const initTok = initialTokenByMint.get(mint);
        const vTok = Number(s.virtual_token_reserves);
        picked.push({
          mint,
          realAge: s._realAge,
          relDevSol: initSol ? Math.abs(s.virtual_sol_reserves - initSol) / initSol : null,
          relDevToken: initTok ? Math.abs(vTok - initTok) / initTok : null,
          queueWaitMs: s.queue_wait_ms,
          gapToCompletion: isB ? perToken.get(mint).firstCompletedAge - s._realAge : null,
        });
      }
      console.log(`\n  --- ${label} ---`);
      // Point 1 : combien ont réellement un snapshot comparable à ce point.
      console.log(`    Snapshots à âge réel comparable trouvés : ${picked.length}/${mintList.length}`);
      // Point 7 : l'âge réel obtenu est-il vraiment comparable à la cible ?
      stats('âge réel obtenu (s), cible=' + d, picked.map((r) => r.realAge));
      // Point 3 : médiane/P25/P75/P90.
      stats('écart relatif vSol |Δ|/initial', picked.map((r) => r.relDevSol));
      stats('écart relatif vToken |Δ|/initial', picked.map((r) => r.relDevToken));
      // Point 4 : distribution brute.
      histogram('distribution écart relatif vSol', picked.map((r) => r.relDevSol));

      // Point 5 : robustesse — exclut B trop proches de leur completion,
      // et toute lecture à queue_wait_ms élevé (les deux groupes).
      const robust = picked.filter((r) => {
        if (isB && r.gapToCompletion !== null && r.gapToCompletion < MIN_GAP_TO_COMPLETION_S) return false;
        if (r.queueWaitMs !== null && r.queueWaitMs !== undefined && r.queueWaitMs > HIGH_QUEUE_WAIT_MS) return false;
        return true;
      });
      console.log(`    Après exclusion (proximité completion${isB ? '' : ' n/a'} + queue_wait_ms>${HIGH_QUEUE_WAIT_MS}ms) : ${robust.length}/${picked.length} conservés`);
      stats('  -> écart relatif vSol (robuste)', robust.map((r) => r.relDevSol));
      stats('  -> écart relatif vToken (robuste)', robust.map((r) => r.relDevToken));
    }
  }

  console.log('\n' + '='.repeat(78));
  console.log('Limites : mêmes que curve-completion-analysis.js (C = "jamais vu');
  console.log('terminé dans nos lectures", pas "n\'a jamais migré" ; échantillon B');
  console.log('encore petit). Aucun score, seuil de décision ou filtre construit ici.');
  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

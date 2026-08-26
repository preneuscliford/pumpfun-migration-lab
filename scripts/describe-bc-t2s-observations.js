#!/usr/bin/env node
'use strict';

// Analyse DESCRIPTIVE (lecture seule), demandée le 2026-08-26 après avoir
// établi que la séparation B/C est déjà nette à T+2s
// (scripts/analyze-bc-early-detection.js). Objectif : comprendre POURQUOI,
// pas construire un signal. Trois hypothèses à départager, pas exclusives :
//   A. de l'activité réelle (achats) a déjà eu lieu entre la création et
//      notre première lecture ;
//   B. l'état initial rapporté diffère déjà entre B et C, avant toute
//      activité ;
//   C. artefact de notre méthode d'observation (délai réel de lecture,
//      mécanique de la bonding curve elle-même).
//
// Classification B/C via tokens.curve_completed_at (posé en direct par le
// listener depuis le 2026-08-24, voir src/listener.js) plutôt qu'un
// rebalayage complet de token_snapshots — même définition que les analyses
// précédentes (RPC a vu complete=true / réserves vidées), mais beaucoup
// moins coûteux : pas besoin de récupérer les snapshots de TOUS les
// tokens pour confirmer les "jamais terminés", seulement ceux du B et
// d'un échantillon C tirés au hasard.
//
// Aucun score, seuil prédictif ou stratégie construit. Ne modifie ni le
// collecteur ni le schéma.
//
// Usage : node scripts/describe-bc-t2s-observations.js

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
  if (!c.length) { console.log(`      ${name.padEnd(34)} n=0 (aucune valeur numérique disponible)`); return; }
  console.log(`      ${name.padEnd(34)} n=${String(c.length).padStart(5)}  min=${fmt(Math.min(...c))} P25=${fmt(percentile(c,25))} P50=${fmt(median(c))} P75=${fmt(percentile(c,75))} max=${fmt(Math.max(...c))}`);
}

const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');
const AGE_TOLERANCE_S = 1; // même tolérance que T+2s dans analyze-bc-early-detection.js
const C_SAMPLE_SIZE = Number(process.env.C_SAMPLE_SIZE) || 600;

function realAgeSeconds(s, createdAtMs) {
  if (s.started_at) return (new Date(s.started_at).getTime() - createdAtMs) / 1000;
  return s.age_seconds;
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  console.log('='.repeat(78));
  console.log(`Analyse descriptive B vs C à T+2s — ${new Date().toISOString()}`);
  console.log('='.repeat(78));

  const allTokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, created_at, curve_completed_at, initial_virtual_sol_reserves, initial_virtual_token_reserves, raw_new_token_event',
    'mint'
  );
  const tokens = allTokens.filter((t) => t.raw_new_token_event && t.created_at && new Date(t.created_at) >= ANALYSIS_SINCE && t.initial_virtual_sol_reserves && t.initial_virtual_token_reserves);
  console.log(`\nTokens dans la fenêtre : ${tokens.length}`);

  // Classification via tokens.curve_completed_at (posé en direct par le
  // listener) — même définition que les analyses précédentes, sans
  // rebalayer token_snapshots pour tout le monde.
  const bTokens = tokens.filter((t) => {
    if (!t.curve_completed_at) return false;
    const gap = (new Date(t.curve_completed_at).getTime() - new Date(t.created_at).getTime()) / 1000;
    return gap > 10;
  });
  const cCandidates = tokens.filter((t) => !t.curve_completed_at);
  console.log(`B (completed >10s) : ${bTokens.length}   C candidats (jamais complété) : ${cCandidates.length}`);

  const cSample = shuffle(cCandidates).slice(0, C_SAMPLE_SIZE);
  console.log(`Échantillon C tiré au hasard : ${cSample.length}`);

  // Transparence sur les champs réellement présents dans le message WS
  // brut (pas de champ deviné) — un exemple de chaque groupe.
  if (bTokens.length) console.log(`\nClés disponibles dans raw_new_token_event (exemple B) : ${Object.keys(bTokens[0].raw_new_token_event).join(', ')}`);
  if (cSample.length) console.log(`Clés disponibles dans raw_new_token_event (exemple C) : ${Object.keys(cSample[0].raw_new_token_event).join(', ')}`);

  const allMints = [...bTokens.map((t) => t.mint), ...cSample.map((t) => t.mint)];
  const snapshots = await fetchSnapshotsForMints(
    supabase,
    allMints,
    'mint, age_seconds, nominal_delay_s, virtual_sol_reserves, virtual_token_reserves, raw_event, started_at, queue_wait_ms, rpc_call_ms'
  );
  console.log(`Snapshots récupérés (B + échantillon C seulement) : ${snapshots.length}`);

  const createdAtByMint = new Map(tokens.map((t) => [t.mint, Date.parse(t.created_at)]));
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

  function pickT2Snapshot(mint) {
    const rows = snapshotsByMint.get(mint) || [];
    let best = null, bestDiff = Infinity;
    for (const s of rows) {
      const diff = Math.abs(s._realAge - 2);
      if (diff <= AGE_TOLERANCE_S && diff < bestDiff) { best = s; bestDiff = diff; }
    }
    return best;
  }

  function describe(label, tokenList) {
    console.log('\n' + '='.repeat(78));
    console.log(`GROUPE ${label} (n candidats = ${tokenList.length})`);
    console.log('='.repeat(78));

    const rows = [];
    for (const t of tokenList) {
      const s = pickT2Snapshot(t.mint);
      if (!s) continue;
      const raw = t.raw_new_token_event || {};
      const initSol = t.initial_virtual_sol_reserves;
      const initTok = Number(t.initial_virtual_token_reserves);
      const vSol = s.virtual_sol_reserves;
      const vTok = Number(s.virtual_token_reserves) / 1e6;
      rows.push({
        mint: t.mint,
        creatorSolAmount: raw.solAmount ?? raw.sol_amount ?? null,
        creatorInitialBuy: raw.initialBuy ?? raw.initial_buy ?? null,
        marketCapSol: raw.marketCapSol ?? raw.market_cap_sol ?? null,
        initSol,
        initTok,
        vSol,
        vTok,
        deltaSolAbs: vSol - initSol,
        deltaTokAbs: vTok - initTok,
        relDevSol: initSol ? Math.abs(vSol - initSol) / initSol : null,
        relDevTok: initTok ? Math.abs(vTok - initTok) / initTok : null,
        realAge: s._realAge,
        nominalDelay: s.nominal_delay_s,
        delayCreatedToStarted: s.started_at ? (new Date(s.started_at).getTime() - createdAtByMint.get(t.mint)) / 1000 : null,
        queueWaitMs: s.queue_wait_ms,
        rpcCallMs: s.rpc_call_ms,
      });
    }
    console.log(`Couverture (snapshot T+2s ±${AGE_TOLERANCE_S}s trouvé) : ${rows.length}/${tokenList.length}`);
    if (!rows.length) return;

    console.log('\n  -- Créateur / message de création --');
    stats('solAmount (créateur, si présent)', rows.map((r) => r.creatorSolAmount));
    stats('initialBuy (si présent)', rows.map((r) => r.creatorInitialBuy));
    stats('marketCapSol (annoncé à la création)', rows.map((r) => r.marketCapSol));

    console.log('\n  -- État initial rapporté (tokens.initial_*) --');
    stats('initial_virtual_sol_reserves', rows.map((r) => r.initSol));
    stats('initial_virtual_token_reserves', rows.map((r) => r.initTok));

    console.log('\n  -- État observé à T+2s (RPC) --');
    stats('vSol à T+2s', rows.map((r) => r.vSol));
    stats('vToken à T+2s', rows.map((r) => r.vTok));

    console.log('\n  -- Variation depuis l\'état initial rapporté --');
    stats('Δ vSol absolu (signé)', rows.map((r) => r.deltaSolAbs));
    stats('Δ vToken absolu (signé)', rows.map((r) => r.deltaTokAbs));
    stats('écart relatif vSol |Δ|/initial', rows.map((r) => r.relDevSol));
    stats('écart relatif vToken |Δ|/initial', rows.map((r) => r.relDevTok));

    console.log('\n  -- Contrôle méthode d\'observation --');
    stats('âge réel du snapshot (s)', rows.map((r) => r.realAge));
    stats('délai réel created_at -> started_at (s)', rows.map((r) => r.delayCreatedToStarted));
    stats('queue_wait_ms', rows.map((r) => r.queueWaitMs));
    stats('rpc_call_ms', rows.map((r) => r.rpcCallMs));
    const nominal2 = rows.filter((r) => r.nominalDelay === 2).length;
    console.log(`      nominal_delay_s == 2 pour ${nominal2}/${rows.length} lignes (devrait être ~toutes)`);

    return rows;
  }

  describe('B (complété >10s)', bTokens);
  describe('C (échantillon, jamais complété)', cSample);

  console.log('\n' + '='.repeat(78));
  console.log('Limites : transactions/volume non disponibles (pas de subscribeTokenTrade');
  console.log('dans ce projet, par choix). Échantillon B toujours petit. Classification');
  console.log('via tokens.curve_completed_at (posée en direct par le listener) — même');
  console.log('définition que les analyses précédentes, non revérifiée ligne par ligne ici.');
  console.log('Aucun score, seuil prédictif ou stratégie construit dans ce script.');
  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

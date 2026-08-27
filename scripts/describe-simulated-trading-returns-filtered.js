#!/usr/bin/env node
'use strict';

// Variante FILTRÉE (lecture seule) de describe-simulated-trading-returns.js,
// demandée le 2026-08-26 : au lieu d'un achat indiscriminé sur tous les
// tokens, ne simuler l'achat que sur les tokens jugés "validés" par
// l'expérience de cette session, selon les critères donnés explicitement
// par l'utilisateur :
//   1. Créateur : solAmount >= 1 SOL
//   2. Initial buy (créateur) : >= 50 000 000 tokens
//   3. Bonding curve à T+2s : variation |Δ|/initial vSol >= 25%
//   4. Bonding curve à T+2s : variation |Δ|/initial vToken >= 10%
//   5. Persistance : mouvement (vSol) encore >= 20% à T+10s
//   6. Concentration premiers holders : aucun wallet > 6% de la supply
//      -> NON APPLIQUÉ. Diagnostic (inspect-filter-criteria-availability.js,
//      run du 2026-08-26) : top_holders_pct_of_supply rempli sur 0/503
//      snapshots échantillonnés (0.0%), holders_error rempli sur 4/503
//      seulement (0.8%) — la capture holders n'aboutit quasiment jamais
//      (cohérent avec HANDOFF.md : 100% d'échec RPC 429 sur ces appels).
//      Impossible d'évaluer ce critère avec les données actuelles. Ce
//      script continue SANS ce filtre et l'affiche en tête de sortie, au
//      lieu de l'ignorer silencieusement ou d'inventer une valeur.
//
// Unités vérifiées avant codage en dur (voir run du diagnostic) :
// solAmount et initialBuy sont DÉJÀ en unités humaines dans
// raw_new_token_event (pas en unités brutes 6 décimales comme
// virtual_token_reserves dans token_snapshots) — aucune conversion.
//
// Même méthode de prix/frais/checkpoints que la version indiscriminée
// (voir describe-simulated-trading-returns.js) : prix marginal
// vSol/vToken, frais de plateforme 1%/leg (2x aller-retour), pas de
// slippage ni de frais de priorité modélisés. Aucun score construit ici
// au-delà du filtre explicitement demandé par l'utilisateur — ce n'est
// pas une stratégie choisie par l'assistant.
//
// Usage : node scripts/describe-simulated-trading-returns-filtered.js

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

const ANALYSIS_SINCE = new Date(process.env.ANALYSIS_SINCE || '2026-08-23T21:21:20Z');
const PLATFORM_FEE_PCT = Number(process.env.PLATFORM_FEE_PCT) || 0.01; // par transaction, aller-retour = x2
const ENTRY_AGE_TARGET_S = 2;
const ENTRY_AGE_TOLERANCE_S = 3;
const PERSISTENCE_AGE_TARGET_S = 10;
const PERSISTENCE_AGE_TOLERANCE_S = 3;
const EXIT_CHECKPOINTS_S = [5, 10, 20, 30, 60, 120, 300, 600, 1200, 1800];
const AGE_TOLERANCE_S = { 5: 2, 10: 3, 20: 5, 30: 7, 60: 10, 120: 20, 300: 45, 600: 90, 1200: 180, 1800: 270 };

// Critères de filtre (donnés explicitement par l'utilisateur le 2026-08-26)
const CREATOR_SOL_AMOUNT_MIN = 1; // SOL
const CREATOR_INITIAL_BUY_MIN = 50_000_000; // tokens
const T2_VSOL_REL_DEV_MIN = 0.25;
const T2_VTOK_REL_DEV_MIN = 0.10;
const T10_VSOL_PERSISTENCE_MIN = 0.20;
const SAMPLE_SIZE = Number(process.env.SAMPLE_SIZE) || 3000;

function isCompletedSnapshot(s) {
  return !!(s.raw_event && s.raw_event.complete === true) || s.virtual_sol_reserves === 0;
}
function realAgeSeconds(s, createdAtMs) {
  if (s.started_at) return (new Date(s.started_at).getTime() - createdAtMs) / 1000;
  return s.age_seconds;
}
function priceOf(s) {
  const vTok = Number(s.virtual_token_reserves) / 1e6;
  if (!vTok) return null;
  return s.virtual_sol_reserves / vTok; // SOL par token, prix marginal
}

function stats(name, values) {
  const c = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!c.length) { console.log(`    ${name.padEnd(30)} n=0`); return; }
  console.log(`    ${name.padEnd(30)} n=${String(c.length).padStart(6)}  P25=${fmt(percentile(c,25))} P50=${fmt(median(c))} P75=${fmt(percentile(c,75))} P90=${fmt(percentile(c,90))}`);
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  console.log('='.repeat(78));
  console.log(`Simulation d'achat FILTRÉ (tokens "validés" seulement) — ${new Date().toISOString()}`);
  console.log(`Frais de plateforme supposés : ${(PLATFORM_FEE_PCT * 100).toFixed(2)}%/transaction, x2 aller-retour`);
  console.log('='.repeat(78));
  console.log('\nCritères appliqués :');
  console.log(`  1. solAmount créateur          >= ${CREATOR_SOL_AMOUNT_MIN} SOL`);
  console.log(`  2. initialBuy créateur         >= ${CREATOR_INITIAL_BUY_MIN.toLocaleString('fr-FR')} tokens`);
  console.log(`  3. |Δ vSol|/initial à T+2s     >= ${(T2_VSOL_REL_DEV_MIN * 100).toFixed(0)}%`);
  console.log(`  4. |Δ vToken|/initial à T+2s   >= ${(T2_VTOK_REL_DEV_MIN * 100).toFixed(0)}%`);
  console.log(`  5. |Δ vSol|/initial à T+10s    >= ${(T10_VSOL_PERSISTENCE_MIN * 100).toFixed(0)}% (persistance)`);
  console.log(`  6. Concentration holders (aucun wallet > 6%) : NON APPLIQUÉ`);
  console.log(`     -> top_holders_pct_of_supply rempli sur 0/503 snapshots testés (diagnostic`);
  console.log(`        du 2026-08-26) : capture holders n'aboutit quasiment jamais, donnée`);
  console.log(`        indisponible pour filtrer. Voir HANDOFF.md.`);

  // Requête unique et bornée (pas fetchAllRows) : .range() et .limit() se
  // marchent dessus dans le client PostgREST. Avec ~90k+ tokens
  // accumulés, un fetch complet de la fenêtre dépasse le timeout Actions
  // (vu lors du premier essai de ce script, annulé) — on échantillonne
  // donc directement dans la requête.
  //
  // Tri par created_at DESC (2026-08-27, corrigé après remarque
  // utilisateur) — PAS par `mint` : l'ordre alphabétique des adresses
  // donnait un sous-ensemble quasi identique à chaque relance, indépendant
  // du moment où le script tourne. Objectif explicite du reset de base :
  // suivre des tokens FRAIS à chaque relance, pas rejouer le même
  // instantané figé. .not('created_at','is',null) : en DESC, Postgres met
  // les NULL en premier par défaut (même piège que
  // verify-reset-and-freshness.js plus tôt dans la session).
  //
  // Le filtre créateur (solAmount/initialBuy) s'applique ENSUITE, côté
  // client, sur cet échantillon déjà borné (pas avant, PostgREST ne
  // permet pas facilement un filtre numérique fiable sur un champ jsonb
  // texte sans colonne dédiée).
  const { data: windowTokens, error: tokensError } = await supabase
    .from('tokens')
    .select('mint, created_at, curve_completed_at, initial_virtual_sol_reserves, initial_virtual_token_reserves, raw_new_token_event')
    .gte('created_at', ANALYSIS_SINCE.toISOString())
    .not('created_at', 'is', null)
    .order('created_at', { ascending: false })
    .limit(SAMPLE_SIZE);
  if (tokensError) throw new Error(`lecture tokens: ${tokensError.message}`);
  console.log(`\nTokens échantillonnés dans la fenêtre : ${windowTokens.length}`);

  const tokens = windowTokens.filter((t) => {
    const raw = t.raw_new_token_event;
    if (!raw) return false;
    const sol = Number(raw.solAmount);
    const buy = Number(raw.initialBuy);
    return Number.isFinite(sol) && sol >= CREATOR_SOL_AMOUNT_MIN && Number.isFinite(buy) && buy >= CREATOR_INITIAL_BUY_MIN;
  });
  console.log(`... après filtre créateur (solAmount/initialBuy) : ${tokens.length}`);
  if (tokens.length) {
    const agesS = tokens.map((t) => (Date.now() - Date.parse(t.created_at)) / 1000).sort((a, b) => a - b);
    console.log(`    âge (s) de ces tokens : min=${agesS[0].toFixed(1)} médiane=${agesS[Math.floor(agesS.length / 2)].toFixed(1)} max=${agesS[agesS.length - 1].toFixed(1)}`);
    const bonkCount = tokens.filter((t) => t.mint.endsWith('bonk')).length;
    const pumpCount = tokens.filter((t) => t.mint.endsWith('pump')).length;
    console.log(`    suffixe mint : .bonk=${bonkCount} .pump=${pumpCount} autre=${tokens.length - bonkCount - pumpCount}`);
    const midAged = [...tokens].sort((a, b) => Date.parse(a.created_at) - Date.parse(b.created_at));
    const sample = [midAged[0], midAged[Math.floor(midAged.length / 2)], midAged[midAged.length - 1]].filter(Boolean);
    console.log('    exemples de mints (vieux/médian/récent) :');
    for (const t of sample) console.log(`      ${t.mint}  created_at=${t.created_at}  solAmount=${t.raw_new_token_event.solAmount}  initialBuy=${t.raw_new_token_event.initialBuy}`);
  }

  const createdAtByMint = new Map(tokens.map((t) => [t.mint, Date.parse(t.created_at)]));
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

  function pickNear(rows, target, tolerance) {
    let best = null, bestDiff = Infinity;
    for (const s of rows) {
      const diff = Math.abs(s._realAge - target);
      if (diff <= tolerance && diff < bestDiff) { best = s; bestDiff = diff; }
    }
    return best;
  }

  // Funnel de filtrage, étape par étape, pour transparence (échantillon
  // attendu petit, voir HANDOFF.md). Les étapes 1/2 (solAmount/initialBuy)
  // ont déjà été appliquées en amont sur l'échantillon de la fenêtre — ici
  // on reconfirme juste sur les tokens retenus, ce qui devrait donner
  // 100% par construction.
  const funnel = {
    totalWindow: windowTokens.length,
    afterCheapFilter: tokens.length,
    total: tokens.length,
    hasRawEvent: 0,
    passSolAmount: 0,
    passInitialBuy: 0,
    hasT2Snapshot: 0,
    passT2VSol: 0,
    passT2VTok: 0,
    hasT10Snapshot: 0,
    passT10Persistence: 0,
  };

  const entries = [];
  for (const t of tokens) {
    const raw = t.raw_new_token_event;
    if (!raw) continue;
    funnel.hasRawEvent += 1;

    const creatorSolAmount = Number(raw.solAmount);
    if (!Number.isFinite(creatorSolAmount) || creatorSolAmount < CREATOR_SOL_AMOUNT_MIN) continue;
    funnel.passSolAmount += 1;

    const creatorInitialBuy = Number(raw.initialBuy);
    if (!Number.isFinite(creatorInitialBuy) || creatorInitialBuy < CREATOR_INITIAL_BUY_MIN) continue;
    funnel.passInitialBuy += 1;

    const initSol = t.initial_virtual_sol_reserves;
    const initTok = Number(t.initial_virtual_token_reserves);
    if (!initSol || !initTok) continue;

    const rows = snapshotsByMint.get(t.mint) || [];
    const entry = pickNear(rows, ENTRY_AGE_TARGET_S, ENTRY_AGE_TOLERANCE_S);
    if (!entry || isCompletedSnapshot(entry)) continue;
    funnel.hasT2Snapshot += 1;

    const entryPrice = priceOf(entry);
    if (!entryPrice) continue;
    const vSolT2 = entry.virtual_sol_reserves;
    const vTokT2 = Number(entry.virtual_token_reserves) / 1e6;
    const relDevSolT2 = Math.abs(vSolT2 - initSol) / initSol;
    const relDevTokT2 = Math.abs(vTokT2 - initTok) / initTok;

    if (relDevSolT2 < T2_VSOL_REL_DEV_MIN) continue;
    funnel.passT2VSol += 1;
    if (relDevTokT2 < T2_VTOK_REL_DEV_MIN) continue;
    funnel.passT2VTok += 1;

    const t10 = pickNear(rows, PERSISTENCE_AGE_TARGET_S, PERSISTENCE_AGE_TOLERANCE_S);
    if (!t10 || isCompletedSnapshot(t10)) continue;
    funnel.hasT10Snapshot += 1;
    const vSolT10 = t10.virtual_sol_reserves;
    const relDevSolT10 = Math.abs(vSolT10 - initSol) / initSol;
    if (relDevSolT10 < T10_VSOL_PERSISTENCE_MIN) continue;
    funnel.passT10Persistence += 1;

    entries.push({ mint: t.mint, entryAge: entry._realAge, entryPrice, rows });
  }

  console.log('\n' + '='.repeat(78));
  console.log('FUNNEL DE FILTRAGE');
  console.log('='.repeat(78));
  console.log(`  Tokens dans la fenêtre                : ${funnel.totalWindow}`);
  console.log(`  ... solAmount + initialBuy OK (amont) : ${funnel.afterCheapFilter}`);
  console.log(`  ... échantillon retenu (bornage coût) : ${funnel.total}`);
  console.log(`  ... avec raw_new_token_event          : ${funnel.hasRawEvent}`);
  console.log(`  ... solAmount >= ${CREATOR_SOL_AMOUNT_MIN} SOL              : ${funnel.passSolAmount}`);
  console.log(`  ... initialBuy >= ${(CREATOR_INITIAL_BUY_MIN / 1e6).toFixed(0)}M tokens        : ${funnel.passInitialBuy}`);
  console.log(`  ... snapshot T+2s exploitable          : ${funnel.hasT2Snapshot}`);
  console.log(`  ... |ΔvSol|/init >= ${(T2_VSOL_REL_DEV_MIN * 100).toFixed(0)}% à T+2s        : ${funnel.passT2VSol}`);
  console.log(`  ... |ΔvToken|/init >= ${(T2_VTOK_REL_DEV_MIN * 100).toFixed(0)}% à T+2s       : ${funnel.passT2VTok}`);
  console.log(`  ... snapshot T+10s exploitable         : ${funnel.hasT10Snapshot}`);
  console.log(`  ... persistance >= ${(T10_VSOL_PERSISTENCE_MIN * 100).toFixed(0)}% à T+10s       : ${funnel.passT10Persistence}`);
  console.log(`\nTokens "validés" retenus pour la simulation : ${entries.length}/${tokens.length}`);

  if (!entries.length) {
    console.log('\nAucun token ne passe le filtre sur cette fenêtre — pas de simulation possible.');
    console.log('='.repeat(78));
    return;
  }

  console.log('\n' + '='.repeat(78));
  console.log('RENDEMENT NET PAR CHECKPOINT (achat à T+2s, "vente" simulée au checkpoint)');
  console.log('  Exclut les checkpoints où la curve est déjà terminée (pas de prix AMM valable).');
  console.log('='.repeat(78));

  for (const d of EXIT_CHECKPOINTS_S) {
    const netReturns = [];
    let terminated = 0, noExit = 0;
    for (const e of entries) {
      const exit = pickNear(e.rows, d, AGE_TOLERANCE_S[d]);
      if (!exit) { noExit += 1; continue; }
      if (isCompletedSnapshot(exit)) { terminated += 1; continue; }
      const exitPrice = priceOf(exit);
      if (!exitPrice) { noExit += 1; continue; }
      const grossReturn = exitPrice / e.entryPrice - 1;
      const netReturn = grossReturn - 2 * PLATFORM_FEE_PCT;
      netReturns.push(netReturn);
    }
    const wins = netReturns.filter((r) => r > 0).length;
    const losses = netReturns.filter((r) => r < 0).length;
    const flat = netReturns.length - wins - losses;
    console.log(`\n  --- T+${d}s ---`);
    console.log(`    Couverture : ${netReturns.length}/${entries.length}  (curve déjà finie à ce point : ${terminated}, pas de lecture dispo : ${noExit})`);
    if (netReturns.length) {
      stats('rendement net (%)', netReturns.map((r) => r * 100));
      console.log(`    % gagnants (net>0)  : ${((wins / netReturns.length) * 100).toFixed(1)}%`);
      console.log(`    % perdants (net<0)  : ${((losses / netReturns.length) * 100).toFixed(1)}%`);
      console.log(`    % neutres (net==0)  : ${((flat / netReturns.length) * 100).toFixed(1)}%`);
    }
  }

  console.log('\n' + '='.repeat(78));
  console.log('Limites (importantes, à lire avant toute interprétation) :');
  console.log('  - Critère 6 (concentration holders) NON appliqué — donnée quasi jamais');
  console.log('    disponible (voir en-tête). Les tokens retenus ici ne sont PAS filtrés');
  console.log('    sur ce critère, contrairement à la demande initiale complète.');
  console.log('  - Prix = ratio marginal vSol/vToken, PAS le prix réellement exécutable.');
  console.log('  - Frais de priorité NON modélisés (pas mesurables depuis nos données) —');
  console.log('    le rendement net réel serait probablement pire que ce qui est affiché.');
  console.log('  - Échantillon attendu petit : les 5 critères combinés sont sélectifs par');
  console.log('    construction (voir funnel ci-dessus) — les statistiques peuvent être');
  console.log('    bruitées sur peu d\'observations, surtout juste après le reset de la base.');
  console.log('  - Filtre construit sur demande explicite de l\'utilisateur (pas une');
  console.log('    stratégie choisie par l\'assistant) — reste une analyse descriptive,');
  console.log('    aucune exécution réelle, aucun argent engagé.');
  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

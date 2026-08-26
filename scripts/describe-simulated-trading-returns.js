#!/usr/bin/env node
'use strict';

// Analyse DESCRIPTIVE (lecture seule), demandée le 2026-08-26 : simuler
// un achat PASSIF (sans exécution réelle, sans argent, sans sélection du
// token — "on n'identifie rien du token, on essaie d'acheter") à la
// première lecture disponible après création, et regarder le rendement
// net à des délais fixes. Objectif : voir la distribution de ce qu'un
// participant non sélectif aurait obtenu, pas construire une stratégie.
//
// Prix utilisé : prix marginal de la bonding curve = vSol / vToken
// (ratio des réserves virtuelles) — PAS le prix réellement exécutable
// pour une taille de trade donnée (glissement/impact de marché ignorés,
// voir limites en bas). C'est une approximation delta-infinitésimal
// standard pour un AMM à produit constant.
//
// Frais modélisés : frais de plateforme pump.fun, appliqués à l'achat ET
// à la vente (aller-retour) — valeur généralement citée publiquement
// autour de 1%/transaction, PLATFORM_FEE_PCT ci-dessous, à ajuster si la
// vraie valeur diffère. Frais réseau Solana (~0.000005 SOL/tx) ignorés :
// négligeables face à 1% sauf pour des trades minuscules. Frais de
// priorité (nécessaires pour être rapide dans les premières secondes,
// justement ce que l'utilisateur soupçonne coûteux) NON modélisés — pas
// mesurables depuis nos données, annoncé explicitement plutôt que deviné.
//
// Aucun score, seuil prédictif ou stratégie construit. Ne modifie ni le
// collecteur ni le schéma.
//
// Usage : node scripts/describe-simulated-trading-returns.js

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
const SAMPLE_SIZE = Number(process.env.SAMPLE_SIZE) || 3000;
const PLATFORM_FEE_PCT = Number(process.env.PLATFORM_FEE_PCT) || 0.01; // par transaction, aller-retour = x2
const ENTRY_AGE_TARGET_S = 2;
const ENTRY_AGE_TOLERANCE_S = 3; // un peu plus large pour maximiser la couverture d'entrée
const EXIT_CHECKPOINTS_S = [5, 10, 20, 30, 60, 120, 300, 600, 1200, 1800];
const AGE_TOLERANCE_S = { 5: 2, 10: 3, 20: 5, 30: 7, 60: 10, 120: 20, 300: 45, 600: 90, 1200: 180, 1800: 270 };

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

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
  console.log(`Simulation d'achat passif (sans argent, sans sélection) — ${new Date().toISOString()}`);
  console.log(`Frais de plateforme supposés : ${(PLATFORM_FEE_PCT * 100).toFixed(2)}%/transaction, x2 aller-retour`);
  console.log('='.repeat(78));

  const windowTokens = await fetchAllRows(
    supabase,
    'tokens',
    'mint, created_at, curve_completed_at',
    'mint',
    (q) => q.gte('created_at', ANALYSIS_SINCE.toISOString())
  );
  console.log(`\nTokens dans la fenêtre : ${windowTokens.length}`);
  const tokens = shuffle(windowTokens).slice(0, SAMPLE_SIZE);
  console.log(`Échantillon tiré au hasard pour borner le coût de la requête : ${tokens.length}`);

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

  // Entrée : première lecture disponible proche de T+2s (première lecture
  // du gate universel), curve pas déjà terminée à ce moment (sinon "achat"
  // n'a pas de sens).
  const entries = [];
  for (const t of tokens) {
    const rows = snapshotsByMint.get(t.mint) || [];
    const entry = pickNear(rows, ENTRY_AGE_TARGET_S, ENTRY_AGE_TOLERANCE_S);
    if (!entry) continue;
    if (isCompletedSnapshot(entry)) continue; // déjà "vendu"/terminé avant qu'on ait pu entrer
    const entryPrice = priceOf(entry);
    if (!entryPrice) continue;
    entries.push({ mint: t.mint, entryAge: entry._realAge, entryPrice, entryIdx: rows.indexOf(entry), rows });
  }
  console.log(`\nTokens avec point d'entrée exploitable (~T+${ENTRY_AGE_TARGET_S}s, curve pas déjà finie) : ${entries.length}/${tokens.length}`);

  console.log('\n' + '='.repeat(78));
  console.log('RENDEMENT NET PAR CHECKPOINT (achat à l\'entrée, "vente" simulée au checkpoint)');
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
  console.log('  - Prix = ratio marginal vSol/vToken, PAS le prix réellement exécutable');
  console.log('    pour une taille de trade donnée (glissement/impact de marché ignorés).');
  console.log('  - Frais de priorité (nécessaires pour être rapide) NON modélisés — pas');
  console.log('    mesurables depuis nos données. Le rendement net réel serait donc pire');
  console.log('    que ce qui est affiché ici, potentiellement significativement.');
  console.log('  - Frais réseau Solana (~0.000005 SOL/tx) ignorés, négligeables sauf trade');
  console.log('    minuscule.');
  console.log('  - Achat/vente supposés instantanés au prix observé — aucune latence');
  console.log('    d\'exécution simulée au-delà de celle déjà présente dans la collecte.');
  console.log('  - Aucun score, seuil prédictif ou stratégie construit ici — indiscriminé');
  console.log('    sur tous les tokens, comme demandé.');
  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

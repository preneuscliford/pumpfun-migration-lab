#!/usr/bin/env node
'use strict';

// Écouteur PumpPortal — subscribeNewToken + subscribeMigration uniquement
// (toujours pas de subscribeTokenTrade). Aucun trading, aucune clé de
// wallet : ce script lit un flux public, plus quelques appels RPC Solana
// publics en lecture seule, et écrit dans Supabase.
//
// Cascade V2 à gate d'activité (2026-08-23), remplace la cascade fixe à 4
// points du 2026-08-21 — objectif inchangé : voir si la progression de la
// bonding curve distingue les migrations progressives (groupe B, >10s) des
// tokens qui ne migreront jamais (groupe C, témoin), sans exploser le
// quota Supabase Free ni le RPC public gratuit pour la majorité des
// tokens qui ne migreront jamais. Toujours pas de subscribeTokenTrade, pas
// de reconstruction via l'historique de transactions.
//
//   1. Gate universel (TOUS les tokens) : lectures à GATE_DELAYS_S.
//   2. Une lecture dépasse ACTIVITY_REL_DEV_THRESHOLD (écart relatif vs
//      initial_virtual_sol_reserves) ? -> cascade étendue EXTENDED_DELAYS_S.
//      Sinon, arrêt — pas la peine de suivre un token qui n'a montré aucun
//      mouvement dans les 10 premières secondes.
//   3. Toujours actif à la fin de l'étendue ? -> longue traîne espacée
//      LONG_TAIL_DELAYS_S. Arrêt immédiat, à tout moment, dès qu'une
//      migration est détectée (subscribeMigration, temps réel — pas besoin
//      d'attendre un snapshot pour l'apprendre) ou qu'une lecture montre
//      complete=true / réserve à 0 (compte déjà vidé, rien à apprendre de
//      plus).
// Seuil calibré le 2026-08-23 sur les données réelles déjà collectées
// (scripts/calibrate-activity-threshold.js) : le bruit flottant pur reste
// sous 1e-8, la masse d'activité réelle démarre autour de 1e-4.
//
// Les holders (src/holders.js, ~20 appels RPC — bien plus cher qu'une
// lecture de bonding curve) suivent le MÊME gate : capturés une seule fois,
// au premier point de la cascade étendue, donc seulement pour les tokens
// jugés actifs — pas pour tout le monde comme avant.
//
// Rétention à deux niveaux (voir sql/schema.sql) : les métriques utiles
// (bc_ratio_t5s/t10s/t20s/t30s, bc_first_active_at_s, bc_peak_ratio) sont
// recopiées sur tokens EN DIRECT à chaque lecture, pas recalculées après
// coup — token_snapshots (détail brut) et raw_new_token_event/
// raw_migration_event (JSON brut) peuvent donc être purgés après un délai
// borné sans perdre l'essentiel. Purge par petits lots à chaque cycle de
// nettoyage plutôt qu'en une fois (une suppression trop large dépasse le
// statement timeout ou la longueur d'URL PostgREST — repéré le 2026-08-23).
//
// Contrainte GitHub Actions : un job est tué au bout de 6h. Ce script ne
// tente donc pas de tourner en continu indéfiniment — il s'arrête
// proprement après MAX_RUNTIME_MS, déclenche un nouveau run de lui-même
// via l'API GitHub Actions, puis quitte. Le workflow watchdog.yml sert de
// filet de sécurité si cette relance automatique échoue (crash avant la
// relance, par exemple). C'est une solution expérimentale pour démarrer,
// pas une architecture définitive.
//
// Forme exacte des messages PumpPortal non vérifiable depuis
// l'environnement de développement (réseau bloqué vers l'extérieur) :
// classifyEvent()/buildTokenRow()/buildMigrationRow() sont écrits en
// best-effort à partir de la documentation publique, avec le JSON brut
// TOUJOURS conservé (raw_new_token_event/raw_migration_event) comme
// filet de sécurité. Un événement qu'on n'arrive pas à classifier est
// loggé (event_type='unknown_event') plutôt que silencieusement perdu —
// à inspecter après le premier run réel pour affiner la classification.

const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const { fetchBondingCurveState, deriveBondingCurvePda } = require('./bondingCurve');
const { fetchHolderConcentration } = require('./holders');

// Constantes surchargeables par variable d'environnement — pratique pour
// les tests d'intégration locaux (voir test/) sans attendre 6h.
const PUMPPORTAL_WS_URL = process.env.PUMPPORTAL_WS_URL || 'wss://pumpportal.fun/api/data';
const MAX_RUNTIME_MS = Number(process.env.MAX_RUNTIME_MS) || (5 * 60 + 50) * 60 * 1000; // 5h50 — marge sous la limite dure de 6h
const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS) || 10_000;
const STALE_CONNECTION_MS = Number(process.env.STALE_CONNECTION_MS) || 5 * 60 * 1000; // pas de message depuis 5min -> connexion suspecte
const CLEANUP_INTERVAL_MS = Number(process.env.CLEANUP_INTERVAL_MS) || 30 * 60 * 1000;
const RECONNECT_BASE_MS = Number(process.env.RECONNECT_BASE_MS) || 1000;
const RECONNECT_MAX_MS = Number(process.env.RECONNECT_MAX_MS) || 30_000;
const OBSERVATION_WINDOW_MS = Number(process.env.OBSERVATION_WINDOW_MS) || 6 * 3600 * 1000;
const RELAY_CHECK_INTERVAL_MS = Number(process.env.RELAY_CHECK_INTERVAL_MS) || 30_000;

const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';

// Cascade V2 (voir en-tête du fichier) — délais en SECONDES (pas ms,
// contrairement à l'ancienne BONDING_CURVE_SNAPSHOT_DELAYS_MS) séparés par
// des virgules, surchargeables par variable d'environnement pour les tests
// d'intégration locaux (voir test/) sans attendre les délais réels.
function parseDelaysS(raw, fallback) {
  if (!raw) return fallback;
  const parsed = raw
    .split(',')
    .map(Number)
    .filter((n) => Number.isFinite(n) && n > 0);
  return parsed.length ? parsed : fallback;
}
const GATE_DELAYS_S = parseDelaysS(process.env.GATE_DELAYS_S, [2, 5, 10]);
const EXTENDED_DELAYS_S = parseDelaysS(process.env.EXTENDED_DELAYS_S, [20, 30, 45, 60]);
const LONG_TAIL_DELAYS_S = parseDelaysS(process.env.LONG_TAIL_DELAYS_S, [120, 300, 600, 1200, 1800]);

// Seuil d'écart relatif |observé - initial_virtual_sol_reserves| /
// initial_virtual_sol_reserves au-delà duquel un token est jugé "actif" et
// passe à la cascade étendue. Calibré le 2026-08-23 sur les snapshots déjà
// collectés (scripts/calibrate-activity-threshold.js) : le bruit flottant
// pur reste sous 1e-8, la masse d'activité réelle démarre autour de 1e-4 —
// séparation nette entre les deux.
const ACTIVITY_REL_DEV_THRESHOLD = Number(process.env.ACTIVITY_REL_DEV_THRESHOLD) || 1e-4;

// Espacement minimum entre deux appels RPC bonding-curve/holders, tous
// tokens confondus — protège le RPC public gratuit d'un afflux de
// créations groupées plutôt que de compter sur le hasard de l'espacement
// naturel.
const BONDING_CURVE_RPC_MIN_INTERVAL_MS = Number(process.env.BONDING_CURVE_RPC_MIN_INTERVAL_MS) || 300;

// Rétention à deux niveaux (voir sql/schema.sql) : token_snapshots purgée
// après SNAPSHOT_RETENTION_MS, raw_new_token_event/raw_migration_event mis
// à NULL après RAW_JSON_RETENTION_MS — le reste de la ligne tokens (résumé,
// groupe A/B/C, métriques dérivées bc_*) n'est jamais purgé.
const SNAPSHOT_RETENTION_MS = Number(process.env.SNAPSHOT_RETENTION_MS) || 4 * 24 * 3600 * 1000;
const RAW_JSON_RETENTION_MS = Number(process.env.RAW_JSON_RETENTION_MS) || 7 * 24 * 3600 * 1000;
// Purge par petits lots à chaque cycle plutôt qu'en une fois — une
// suppression/mise à jour trop large dépasse le statement timeout Supabase
// ou la longueur d'URL PostgREST (repéré le 2026-08-23, voir le
// git log de scripts/reset-database.js avant son retrait). PURGE_MAX_BATCHES
// borne le temps passé sur la purge à chaque cycle ; les cycles suivants
// rattrapent le reste.
const PURGE_BATCH_SIZE = 150;
const PURGE_MAX_BATCHES = Number(process.env.PURGE_MAX_BATCHES) || 20;

// --------------------------------------------------------------------
// Classification / extraction — pures, testables sans réseau ni Supabase.
// --------------------------------------------------------------------

function classifyEvent(msg) {
  if (!msg || typeof msg !== 'object') return 'unknown';
  if (msg.txType === 'create') return 'new_token';
  if (msg.txType === 'migrate' || msg.txType === 'migration') return 'migration';
  // Repli heuristique si txType est absent ou différent de ce qu'on
  // attend (protocole non vérifié) : forme d'une création (nom/symbole/
  // état de bonding curve présents) vs forme d'une migration (mint +
  // destination de pool, sans nom/symbole).
  if (msg.mint && msg.name !== undefined && msg.symbol !== undefined && msg.bondingCurveKey) return 'new_token';
  if (msg.mint && msg.pool !== undefined) return 'migration';
  return 'unknown';
}

function numOrNull(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

function buildTokenRow(msg, nowIso) {
  return {
    mint: msg.mint,
    symbol: msg.symbol ?? null,
    name: msg.name ?? null,
    creator: msg.traderPublicKey ?? msg.creator ?? null,
    created_at: nowIso,
    initial_virtual_sol_reserves: numOrNull(msg.vSolInBondingCurve),
    initial_virtual_token_reserves: numOrNull(msg.vTokensInBondingCurve),
    initial_market_cap_sol: numOrNull(msg.marketCapSol),
    metadata_uri: msg.uri ?? null,
    raw_new_token_event: msg,
  };
}

function buildMigrationRow(msg, nowIso) {
  return {
    mint: msg.mint,
    migrated: true,
    migrated_at: nowIso,
    migration_pool: msg.pool ?? null,
    raw_migration_event: msg,
  };
}

// --------------------------------------------------------------------
// Accès Supabase — isolé derrière une petite interface pour pouvoir tester
// la logique d'ingestion avec un faux client (voir test/).
// --------------------------------------------------------------------

function createDb(supabaseClient) {
  return {
    async upsertNewTokens(rows) {
      if (!rows.length) return;
      const { error } = await supabaseClient.from('tokens').upsert(rows, { onConflict: 'mint' });
      if (error) throw new Error(`upsertNewTokens: ${error.message}`);
    },
    // upsert PARTIEL (colonnes de migration uniquement) : ne doit jamais
    // écraser name/symbol/etc. d'un token déjà connu. Si le token n'a
    // jamais été vu en création (trou de connexion), ceci l'insère quand
    // même avec les colonnes de création à NULL — ce NULL est justement
    // le signal "on n'a pas les features de création pour ce token-là",
    // pas la peine d'une colonne dédiée.
    async upsertMigrations(rows) {
      if (!rows.length) return;
      const { error } = await supabaseClient.from('tokens').upsert(rows, { onConflict: 'mint', defaultToNull: false });
      if (error) throw new Error(`upsertMigrations: ${error.message}`);
    },
    async logIngestion(eventType, detail) {
      const { error } = await supabaseClient.from('ingestion_log').insert({ event_type: eventType, detail: detail ?? null });
      if (error) console.error(`logIngestion(${eventType}) a échoué: ${error.message}`);
    },
    async insertSnapshot(row) {
      const { error } = await supabaseClient.from('token_snapshots').insert(row);
      if (error) throw new Error(`insertSnapshot: ${error.message}`);
    },
    // Recopie EN DIRECT les métriques dérivées de la cascade sur tokens
    // (voir sql/schema.sql) — pas un batch après coup. Lecture-modification-
    // écriture plutôt qu'une expression SQL atomique (GREATEST/COALESCE via
    // une fonction Postgres) : plus simple, et sans risque de course ici
    // puisque les lectures d'un même token sont sérialisées (setTimeout
    // croissants sur la même file RPC), jamais concurrentes entre elles.
    async updateTokenDerivedMetrics(mint, { ratio, ageSeconds, checkpointColumn }) {
      const { data: current, error: selectError } = await supabaseClient
        .from('tokens')
        .select('bc_peak_ratio, bc_first_active_at_s, bc_cascade_reads')
        .eq('mint', mint)
        .maybeSingle();
      if (selectError) throw new Error(`updateTokenDerivedMetrics(select): ${selectError.message}`);

      const update = { bc_cascade_reads: (current?.bc_cascade_reads ?? 0) + 1 };
      if (checkpointColumn && ratio !== null && ratio !== undefined) update[checkpointColumn] = ratio;
      if (ratio !== null && ratio !== undefined) {
        const currentPeak = current?.bc_peak_ratio;
        update.bc_peak_ratio =
          currentPeak === null || currentPeak === undefined || Math.abs(ratio - 1) > Math.abs(currentPeak - 1) ? ratio : currentPeak;
        const relDev = Math.abs(ratio - 1);
        if (relDev > ACTIVITY_REL_DEV_THRESHOLD && (current?.bc_first_active_at_s === null || current?.bc_first_active_at_s === undefined)) {
          update.bc_first_active_at_s = ageSeconds;
        }
      }
      const { error } = await supabaseClient.from('tokens').update(update).eq('mint', mint);
      if (error) throw new Error(`updateTokenDerivedMetrics(update): ${error.message}`);
    },
    // Supprime jusqu'à PURGE_MAX_BATCHES lots de token_snapshots plus
    // vieux que windowMs. Ne boucle PAS jusqu'à épuisement — un cycle de
    // nettoyage reste rapide, le rattrapage se fait sur les cycles
    // suivants (voir CLEANUP_INTERVAL_MS dans main()).
    async purgeOldSnapshots(windowMs) {
      const cutoff = new Date(Date.now() - windowMs).toISOString();
      let totalDeleted = 0;
      for (let batch = 0; batch < PURGE_MAX_BATCHES; batch += 1) {
        const { data: rows, error: selectError } = await supabaseClient
          .from('token_snapshots')
          .select('id')
          .lt('captured_at', cutoff)
          .limit(PURGE_BATCH_SIZE);
        if (selectError) throw new Error(`purgeOldSnapshots(select): ${selectError.message}`);
        if (!rows.length) break;
        const { error: deleteError, count } = await supabaseClient
          .from('token_snapshots')
          .delete({ count: 'exact' })
          .in(
            'id',
            rows.map((r) => r.id)
          );
        if (deleteError) throw new Error(`purgeOldSnapshots(delete): ${deleteError.message}`);
        totalDeleted += count ?? 0;
        if (rows.length < PURGE_BATCH_SIZE) break;
      }
      return totalDeleted;
    },
    // NULLise raw_new_token_event/raw_migration_event des tokens créés il y
    // a plus de windowMs — garde tout le reste de la ligne (résumé, groupe,
    // métriques bc_*) indéfiniment. Même logique de lots que ci-dessus.
    async purgeOldRawJson(windowMs) {
      const cutoff = new Date(Date.now() - windowMs).toISOString();
      let totalUpdated = 0;
      for (let batch = 0; batch < PURGE_MAX_BATCHES; batch += 1) {
        const { data: rows, error: selectError } = await supabaseClient
          .from('tokens')
          .select('mint')
          .lt('created_at', cutoff)
          .not('raw_new_token_event', 'is', null)
          .limit(PURGE_BATCH_SIZE);
        if (selectError) throw new Error(`purgeOldRawJson(select): ${selectError.message}`);
        if (!rows.length) break;
        const { error: updateError, count } = await supabaseClient
          .from('tokens')
          .update({ raw_new_token_event: null, raw_migration_event: null }, { count: 'exact' })
          .in(
            'mint',
            rows.map((r) => r.mint)
          );
        if (updateError) throw new Error(`purgeOldRawJson(update): ${updateError.message}`);
        totalUpdated += count ?? 0;
        if (rows.length < PURGE_BATCH_SIZE) break;
      }
      return totalUpdated;
    },
    // Ferme la fenêtre d'observation des tokens non migrés créés il y a
    // plus de OBSERVATION_WINDOW_MS. Ne touche PAS au résumé lui-même
    // (gardé pour tous, migrés ou non — voir README) ni au JSON brut : ce
    // sont purgeOldSnapshots/purgeOldRawJson ci-dessus qui s'en chargent,
    // séparément et sur une fenêtre différente (voir sql/schema.sql).
    async closeExpiredWindows(windowMs) {
      const cutoff = new Date(Date.now() - windowMs).toISOString();
      const { error, count } = await supabaseClient
        .from('tokens')
        .update({ observation_closed_at: new Date().toISOString() }, { count: 'exact' })
        .eq('migrated', false)
        .is('observation_closed_at', null)
        .lt('created_at', cutoff);
      if (error) throw new Error(`closeExpiredWindows: ${error.message}`);
      return count ?? 0;
    },
  };
}

// --------------------------------------------------------------------
// Buffer d'événements de migration — regroupe les écritures pour rester
// économe en requêtes plutôt qu'un update par événement. Les créations ne
// passent PAS par ce buffer (voir main()) : la cascade V2 lit la bonding
// curve dès T+2s, largement avant qu'un flush périodique (FLUSH_INTERVAL_MS,
// ~10s) n'ait eu lieu — les bufferiser provoquait une violation de
// contrainte de clé étrangère sur token_snapshots (le token n'existait pas
// encore côté Supabase à la première lecture de la cascade, bug constaté en
// prod le 2026-08-23 juste après le déploiement V2). Les migrations n'ont
// pas cette dépendance en aval, donc rester bufferisées reste sûr.
// --------------------------------------------------------------------

class EventBuffer {
  constructor(db) {
    this.db = db;
    this.migrationRows = [];
  }

  addMigration(row) {
    this.migrationRows.push(row);
  }

  get size() {
    return this.migrationRows.length;
  }

  async flush() {
    const migrations = this.migrationRows.splice(0, this.migrationRows.length);
    if (migrations.length) await this.db.upsertMigrations(migrations);
    return { migrations: migrations.length };
  }
}

// --------------------------------------------------------------------
// Cascade de bonding curve — gate d'activité à deux niveaux (voir en-tête
// du fichier). Pas de polling continu : une poignée de lectures
// ponctuelles par token, sérialisées via une file d'attente pour rester
// raisonnable envers le RPC public gratuit.
// --------------------------------------------------------------------

// File qui espace les appels RPC d'au moins minIntervalMs, tous appelants
// confondus. Chaque appel attend son tour puis s'exécute ; le résultat
// (succès ou échec) de fn() est celui renvoyé à l'appelant.
function createRpcThrottle(minIntervalMs) {
  let queue = Promise.resolve();
  return function enqueue(fn) {
    const result = queue.then(fn);
    queue = result.catch(() => {}).then(() => new Promise((resolve) => setTimeout(resolve, minIntervalMs)));
    return result;
  };
}

// Colonnes dérivées "point de contrôle" (voir sql/schema.sql) — seuls ces
// 4 délais nominaux ont une colonne dédiée sur tokens ; les autres points
// de la cascade (2s, 45s, 60s, longue traîne) servent à la décision
// active/inactif et au peak/first-active, pas à un checkpoint individuel.
const CHECKPOINT_COLUMNS = { 5: 'bc_ratio_t5s', 10: 'bc_ratio_t10s', 20: 'bc_ratio_t20s', 30: 'bc_ratio_t30s' };

function computeRelDev(observed, initial) {
  if (!initial) return null;
  return Math.abs(observed - initial) / initial;
}

// Une lecture de cascade : bonding curve (+ holders si demandé), écrit le
// snapshot brut et met à jour les métriques dérivées sur tokens dans la
// foulée. Retourne {relDev, complete, virtualSolReserves} pour que
// l'appelant décide de la suite (actif ? migré/vidé ?), ou null en cas
// d'échec RPC — un échec n'interrompt pas la cascade, juste ce point-là.
async function captureCascadeRead(
  db,
  rpcThrottle,
  mint,
  createdAtMs,
  initialVSol,
  nominalDelayS,
  { captureHolders = false, fetchFn = fetchBondingCurveState, holdersFetchFn = fetchHolderConcentration } = {}
) {
  const ageSeconds = Math.round((Date.now() - createdAtMs) / 1000);
  try {
    const state = await rpcThrottle(() => fetchFn(SOLANA_RPC_URL, mint));
    const row = {
      mint,
      age_seconds: ageSeconds,
      nominal_delay_s: nominalDelayS,
      virtual_sol_reserves: state.virtual_quote_reserves_sol,
      virtual_token_reserves: state.virtual_token_reserves,
      raw_event: state,
    };
    if (captureHolders) {
      try {
        const { pda } = deriveBondingCurvePda(mint);
        const holders = await rpcThrottle(() => holdersFetchFn(SOLANA_RPC_URL, mint, pda));
        row.total_supply = holders.total_supply;
        row.curve_held_amount = holders.curve_held_amount;
        row.top_holders_count = holders.top_holders_count;
        row.top_holders_pct_of_supply = holders.top_holders_pct_of_supply;
        row.holders_raw = holders;
      } catch (err) {
        row.holders_error = err.message;
      }
    }
    await db.insertSnapshot(row);

    const ratio = initialVSol ? state.virtual_quote_reserves_sol / initialVSol : null;
    await db.updateTokenDerivedMetrics(mint, { ratio, ageSeconds, checkpointColumn: CHECKPOINT_COLUMNS[nominalDelayS] });

    return { relDev: computeRelDev(state.virtual_quote_reserves_sol, initialVSol), complete: state.complete, virtualSolReserves: state.virtual_quote_reserves_sol };
  } catch (err) {
    await db.logIngestion('bonding_curve_snapshot_error', `${mint} @${ageSeconds}s (T+${nominalDelayS}s): ${err.message}`).catch(() => {});
    return null;
  }
}

// Orchestre la cascade complète pour un token qui vient d'être créé : gate
// universel -> étendue si actif -> longue traîne si toujours actif (voir
// en-tête du fichier). Retourne {stop} pour permettre un arrêt immédiat
// depuis l'extérieur (migration détectée en temps réel, voir main()) sans
// attendre le prochain point programmé. isShuttingDown est une fonction
// (pas une valeur) car on veut lire l'état AU MOMENT où le timer se
// déclenche, pas au moment où il est programmé — un run peut commencer son
// arrêt entre les deux.
function scheduleTokenCascade(
  db,
  rpcThrottle,
  mint,
  createdAtMs,
  initialVSol,
  isShuttingDown,
  {
    gateDelaysS = GATE_DELAYS_S,
    extendedDelaysS = EXTENDED_DELAYS_S,
    longTailDelaysS = LONG_TAIL_DELAYS_S,
    onDone = () => {},
    fetchFn = fetchBondingCurveState,
    holdersFetchFn = fetchHolderConcentration,
  } = {}
) {
  const timers = [];
  let stopped = false;
  let done = false;

  // Appelé exactement une fois, dès qu'on SAIT qu'aucune autre lecture ne
  // sera programmée pour ce token (arrêt externe, résolu, jugé inactif à
  // une étape, ou dernier point de la longue traîne atteint) — permet à
  // main() de retirer l'entrée de activeCascades plutôt que de la laisser
  // traîner en mémoire jusqu'à la fin du run.
  function markDone() {
    if (done) return;
    done = true;
    onDone(mint);
  }

  function stop() {
    stopped = true;
    for (const t of timers) clearTimeout(t);
    timers.length = 0;
    markDone();
  }

  function scheduleAt(delayS, fn) {
    const remaining = Math.max(delayS * 1000 - (Date.now() - createdAtMs), 0);
    timers.push(setTimeout(fn, remaining));
  }

  // "Résolu" = déjà migré/vidé (complete=true ou réserve à 0) : plus rien
  // à apprendre de ce token, on arrête tout de suite plutôt que de gaspiller
  // les points restants de la cascade en cours.
  function isResolved(result) {
    return !!result && (result.complete || result.virtualSolReserves === 0);
  }

  async function runRead(delayS, opts) {
    if (stopped || isShuttingDown()) return null;
    const result = await captureCascadeRead(db, rpcThrottle, mint, createdAtMs, initialVSol, delayS, { fetchFn, holdersFetchFn, ...opts });
    if (isResolved(result)) stop();
    return result;
  }

  function isActive(results) {
    return results.some((r) => r && r.relDev !== null && r.relDev > ACTIVITY_REL_DEV_THRESHOLD);
  }

  function scheduleLongTail() {
    let longTailDone = 0;
    for (const delayS of longTailDelaysS) {
      scheduleAt(delayS, async () => {
        await runRead(delayS).catch(() => {});
        longTailDone += 1;
        if (longTailDone === longTailDelaysS.length) markDone();
      });
    }
  }

  function scheduleExtended() {
    const results = [];
    let extendedDone = 0;
    for (const delayS of extendedDelaysS) {
      scheduleAt(delayS, async () => {
        const result = await runRead(delayS, { captureHolders: delayS === extendedDelaysS[0] }).catch(() => null);
        results.push(result);
        extendedDone += 1;
        if (extendedDone === extendedDelaysS.length && !stopped) {
          if (isActive(results)) scheduleLongTail();
          else markDone();
        }
      });
    }
  }

  const gateResults = [];
  let gateDone = 0;
  for (const delayS of gateDelaysS) {
    scheduleAt(delayS, async () => {
      const result = await runRead(delayS).catch(() => null);
      gateResults.push(result);
      gateDone += 1;
      if (gateDone === gateDelaysS.length && !stopped) {
        if (isActive(gateResults)) scheduleExtended();
        else markDone();
      }
    });
  }

  return { stop };
}

// --------------------------------------------------------------------
// Relais entre runs GitHub Actions (contournement de la limite de 6h par
// job). Nécessite `permissions: actions: write` dans le workflow pour que
// GITHUB_TOKEN puisse déclencher un nouveau run.
// --------------------------------------------------------------------

async function triggerNextRun({ token, repo, ref, workflowFile }) {
  if (!token || !repo) {
    throw new Error('GITHUB_TOKEN ou GITHUB_REPOSITORY absent — impossible de relancer automatiquement (le watchdog prendra le relais).');
  }
  const res = await fetch(`https://api.github.com/repos/${repo}/actions/workflows/${workflowFile}/dispatches`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/vnd.github+json',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ref: ref || 'main' }),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} en relançant le workflow: ${text.slice(0, 300)}`);
  }
}

// --------------------------------------------------------------------
// Orchestration principale.
// --------------------------------------------------------------------

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function main() {
  const startTime = Date.now();
  const supabaseUrl = requireEnv('SUPABASE_URL');
  const supabaseKey = requireEnv('SUPABASE_SERVICE_KEY');
  const db = createDb(createClient(supabaseUrl, supabaseKey, { auth: { persistSession: false } }));
  const buffer = new EventBuffer(db);
  const rpcThrottle = createRpcThrottle(BONDING_CURVE_RPC_MIN_INTERVAL_MS);
  // mint -> {stop} des cascades en cours, pour pouvoir arrêter net dès
  // qu'une migration arrive (voir le handler 'migration' ci-dessous) sans
  // attendre le prochain point programmé.
  const activeCascades = new Map();

  let ws = null;
  let lastMessageAt = Date.now();
  let reconnectAttempt = 0;
  let shuttingDown = false;
  const timers = [];

  function connect() {
    ws = new WebSocket(PUMPPORTAL_WS_URL);

    ws.on('open', () => {
      reconnectAttempt = 0;
      lastMessageAt = Date.now();
      ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
      ws.send(JSON.stringify({ method: 'subscribeMigration' }));
      db.logIngestion('connected').catch(() => {});
      console.log(`[${new Date().toISOString()}] connecté, abonné à subscribeNewToken + subscribeMigration`);
    });

    ws.on('message', (data) => {
      lastMessageAt = Date.now();
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        return; // message non-JSON (rare) : ignoré, pas fatal
      }
      const nowIso = new Date().toISOString();
      const type = classifyEvent(msg);
      if (type === 'new_token') {
        const row = buildTokenRow(msg, nowIso);
        // Upsert immédiat, PAS de mise en buffer (voir EventBuffer) : la
        // cascade ne doit être programmée qu'une fois le token réellement
        // écrit côté Supabase, sinon sa première lecture (T+2s) viole la
        // clé étrangère token_snapshots -> tokens. Un upsert par création
        // reste largement dans le budget de requêtes Supabase Free vu le
        // débit (~1 création/3s).
        //
        // L'upsert étant maintenant async, un événement 'migration' pour ce
        // mint peut arriver AVANT que la cascade ne soit programmée (donc
        // avant qu'elle existe dans activeCascades) — un placeholder
        // synchrone absorbe un stop() prématuré via son propre flag, sur le
        // même principe que le "stopped" interne de scheduleTokenCascade.
        let stoppedBeforeScheduled = false;
        activeCascades.set(row.mint, { stop: () => { stoppedBeforeScheduled = true; } });
        db.upsertNewTokens([row])
          .then(() => {
            if (stoppedBeforeScheduled) {
              activeCascades.delete(row.mint);
              return;
            }
            const cascade = scheduleTokenCascade(
              db,
              rpcThrottle,
              row.mint,
              Date.parse(row.created_at),
              row.initial_virtual_sol_reserves,
              () => shuttingDown,
              { onDone: (doneMint) => activeCascades.delete(doneMint) }
            );
            activeCascades.set(row.mint, cascade);
          })
          .catch((err) => {
            activeCascades.delete(row.mint);
            db.logIngestion('bonding_curve_snapshot_error', `upsert immédiat échoué pour ${row.mint}, cascade non programmée: ${err.message}`).catch(() => {});
          });
      } else if (type === 'migration') {
        const migRow = buildMigrationRow(msg, nowIso);
        buffer.addMigration(migRow);
        // Arrêt immédiat de la cascade en cours pour ce mint (voir en-tête
        // du fichier) : la migration est le signal le plus fiable qu'il
        // n'y a plus rien à apprendre de la bonding curve, pas la peine
        // d'attendre le prochain point programmé pour s'en rendre compte.
        // stop() déclenche onDone, qui retire l'entrée de activeCascades —
        // pas besoin de le refaire ici.
        activeCascades.get(migRow.mint)?.stop();
      } else {
        // Probablement un accusé de souscription ou un format inattendu —
        // loggé pour inspection plutôt que silencieusement perdu.
        db.logIngestion('unknown_event', JSON.stringify(msg).slice(0, 500)).catch(() => {});
      }
    });

    ws.on('close', () => {
      if (shuttingDown) return;
      db.logIngestion('disconnected').catch(() => {});
      const delay = Math.min(RECONNECT_BASE_MS * 2 ** reconnectAttempt, RECONNECT_MAX_MS);
      reconnectAttempt += 1;
      setTimeout(connect, delay);
    });

    ws.on('error', (err) => {
      db.logIngestion('reconnect_attempt', err.message).catch(() => {});
      // 'close' suit un 'error' réseau — la reconnexion y est déjà gérée.
    });
  }

  connect();

  timers.push(
    setInterval(() => {
      if (buffer.size > 0) {
        buffer.flush().catch((err) => console.error(`flush échoué: ${err.message}`));
      }
    }, FLUSH_INTERVAL_MS)
  );

  timers.push(
    setInterval(() => {
      if (ws && Date.now() - lastMessageAt > STALE_CONNECTION_MS) {
        db.logIngestion('reconnect_attempt', 'watchdog: aucun message reçu récemment, reconnexion forcée').catch(() => {});
        ws.terminate();
      }
    }, 30_000)
  );

  timers.push(
    setInterval(() => {
      db.closeExpiredWindows(OBSERVATION_WINDOW_MS)
        .then((count) => db.logIngestion('cleanup_run', `${count} fenêtre(s) d'observation fermée(s)`))
        .catch((err) => console.error(`cleanup échoué: ${err.message}`));
    }, CLEANUP_INTERVAL_MS)
  );

  timers.push(
    setInterval(() => {
      db.purgeOldSnapshots(SNAPSHOT_RETENTION_MS)
        .then((count) => db.logIngestion('snapshots_purged', `${count} snapshot(s) purgé(s) (fenêtre ${Math.round(SNAPSHOT_RETENTION_MS / 86_400_000)}j)`))
        .catch((err) => console.error(`purge snapshots échouée: ${err.message}`));
    }, CLEANUP_INTERVAL_MS)
  );

  timers.push(
    setInterval(() => {
      db.purgeOldRawJson(RAW_JSON_RETENTION_MS)
        .then((count) => db.logIngestion('raw_json_purged', `${count} token(s) JSON brut nullisé (fenêtre ${Math.round(RAW_JSON_RETENTION_MS / 86_400_000)}j)`))
        .catch((err) => console.error(`purge JSON brut échouée: ${err.message}`));
    }, CLEANUP_INTERVAL_MS)
  );

  timers.push(
    setInterval(async () => {
      if (Date.now() - startTime < MAX_RUNTIME_MS) return;
      shuttingDown = true;
      for (const t of timers) clearInterval(t);
      if (ws) ws.close();
      await buffer.flush().catch((err) => console.error(`flush final échoué: ${err.message}`));
      await db.logIngestion('relay_handoff', 'fin de run planifiée, relance en cours').catch(() => {});
      try {
        await triggerNextRun({
          token: process.env.GITHUB_TOKEN,
          repo: process.env.GITHUB_REPOSITORY,
          ref: process.env.GITHUB_REF_NAME,
          workflowFile: 'listener.yml',
        });
        console.log('Relance déclenchée avec succès.');
      } catch (err) {
        console.error(`Relance automatique échouée (le watchdog prendra le relais) : ${err.message}`);
      }
      process.exit(0);
    }, RELAY_CHECK_INTERVAL_MS)
  );

  const shutdown = async (signal) => {
    console.log(`Signal ${signal} reçu, arrêt propre...`);
    shuttingDown = true;
    for (const t of timers) clearInterval(t);
    if (ws) ws.close();
    await buffer.flush().catch(() => {});
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

module.exports = {
  classifyEvent,
  buildTokenRow,
  buildMigrationRow,
  createDb,
  EventBuffer,
  triggerNextRun,
  createRpcThrottle,
  computeRelDev,
  captureCascadeRead,
  scheduleTokenCascade,
  main,
};

if (require.main === module) {
  main().catch((err) => {
    console.error('Erreur fatale:', err.message);
    process.exit(1);
  });
}

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
//      LONG_TAIL_DELAYS_S. Arrêt UNIQUEMENT quand une lecture RPC montre
//      elle-même complete=true / réserve à 0 (compte vidé) — PAS sur
//      subscribeMigration (changé le 2026-08-24 : cet événement s'est
//      révélé arriver en retard de plusieurs minutes, parfois plus de
//      30min, sur l'état réel on-chain, voir src/report.js section
//      CALIBRATION — l'utiliser comme condition d'arrêt faisait
//      systématiquement manquer la confirmation RPC de la complétion).
//      migrated/migrated_at restent enregistrés normalement, comme
//      information séparée (pumpportal_migration_at) ; curve_completed_at
//      (horloge RPC, voir sql/schema.sql) est la nouvelle source de
//      vérité pour "quand la curve a fini", avec l'écart entre les deux
//      calculé par Postgres (curve_completion_lag_seconds).
// Seuil calibré le 2026-08-23 sur les données réelles déjà collectées
// (scripts/calibrate-activity-threshold.js) : le bruit flottant pur reste
// sous 1e-8, la masse d'activité réelle démarre autour de 1e-4.
//
// Les holders (src/holders.js, ~20 appels RPC — bien plus cher qu'une
// lecture de bonding curve) suivent le MÊME gate : capturés une seule fois,
// au premier point de la cascade étendue, donc seulement pour les tokens
// jugés actifs — pas pour tout le monde comme avant. Depuis le 2026-08-24,
// passent par une file RPC INDÉPENDANTE de la file bonding curve (voir
// createRpcThrottle) : une capture holders (lente, retries sur 429 inclus
// — le RPC public nous renvoie 429 sur ~100% de ces tentatives) ne doit
// plus jamais retarder une lecture bonding curve, qui reste prioritaire
// par construction. Un budget quotidien (HOLDERS_DAILY_BUDGET) protège
// contre un martèlement indéfini d'un endpoint qui refuse
// systématiquement.
//
// File bonding curve adaptative (2026-08-25, voir
// createAdaptiveBondingCurveThrottle) : mesuré la veille
// (scripts/measure-bonding-curve-429-rate.js) 0% d'échec définitif par 429
// sur bonding curve à l'espacement fixe de 300ms (~9,3% de 429 absorbés
// par le retry interne, jamais visibles en échec) — de la marge existe
// pour accélérer. Mais l'espacement fixe seul ne bornait pas le pire cas
// en rafale de créations (P99 queue_wait_ms=121s, max=145s mesurés le
// même jour). Remplacé par un seau de jetons (token bucket) dont
// l'intervalle et la capacité de rafale s'ajustent en continu (AIMD :
// accélère prudemment sur série propre, ralentit fort et tout de suite au
// moindre 429), plus un garde-fou de délai (BC_DEADLINE_MS) qui force le
// passage d'une lecture bloquée trop longtemps quel que soit l'état de
// l'adaptatif. Holders INCHANGÉ (reste sur createRpcThrottle, espacement
// fixe) — rien à gagner à accélérer un endpoint qui refuse 100% du temps.
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

// Délai avant une unique retentative d'insertSnapshot après une violation
// de token_snapshots_mint_fkey (voir captureCascadeRead) — 2026-08-26.
const INSERT_SNAPSHOT_FK_RETRY_DELAY_MS = Number(process.env.INSERT_SNAPSHOT_FK_RETRY_DELAY_MS) || 1500;

// Espacement minimum entre deux appels RPC bonding-curve/holders, tous
// tokens confondus — protège le RPC public gratuit d'un afflux de
// créations groupées plutôt que de compter sur le hasard de l'espacement
// naturel. Valeur INITIALE du throttle adaptatif bonding curve depuis le
// 2026-08-25 (voir createAdaptiveBondingCurveThrottle) — reste la valeur
// fixe utilisée par holders (inchangé).
const BONDING_CURVE_RPC_MIN_INTERVAL_MS = Number(process.env.BONDING_CURVE_RPC_MIN_INTERVAL_MS) || 300;

// Throttle adaptatif bonding curve (2026-08-25) — mesuré la veille
// (scripts/measure-bonding-curve-429-rate.js, ~51 000 tentatives à
// l'espacement fixe de 300ms) : 0 échec définitif par 429, ~9,3% de 429
// absorbés par le retry interne de rpcCall (bondingCurve.js) — de la
// marge existe pour accélérer. Mais l'espacement fixe seul ne bornait pas
// le pire cas en rafale de créations : P90 queue_wait_ms=48s, P99=121s,
// max=145s mesurés le même jour (scripts/analyze-v3-instrumentation.js)
// alors que 90% des lectures étaient propres. BC_MIN_INTERVAL_MS par
// défaut ne dépasse jamais BC_INITIAL_INTERVAL_MS : sans ce plafonnement,
// un intervalle initial déjà rapide (ex. tests d'intégration, 10ms) se
// ferait remonter au plancher de prod (80ms) dès le premier palier
// d'accélération — comportement non voulu, seulement un filet de
// sécurité pour l'intervalle de prod (300ms).
const BC_INITIAL_INTERVAL_MS = Number(process.env.BC_INITIAL_INTERVAL_MS) || BONDING_CURVE_RPC_MIN_INTERVAL_MS;
const BC_MIN_INTERVAL_MS = Number(process.env.BC_MIN_INTERVAL_MS) || Math.min(80, BC_INITIAL_INTERVAL_MS);
const BC_MAX_INTERVAL_MS = Number(process.env.BC_MAX_INTERVAL_MS) || 3000;
const BC_INTERVAL_STEP_DOWN_MS = Number(process.env.BC_INTERVAL_STEP_DOWN_MS) || 20;
const BC_INTERVAL_BACKOFF_FACTOR = Number(process.env.BC_INTERVAL_BACKOFF_FACTOR) || 2;
const BC_CLEAN_STREAK_TO_SPEED_UP = Number(process.env.BC_CLEAN_STREAK_TO_SPEED_UP) || 30;
const BC_MIN_CAPACITY = 1;
const BC_MAX_CAPACITY = Number(process.env.BC_MAX_CAPACITY) || 4;
// Garde-fou : une lecture en attente depuis plus longtemps que ceci est
// dispatchée immédiatement, jeton ou pas — borne le pire cas
// indépendamment de l'état de l'adaptatif (demandé explicitement le
// 2026-08-24 : "garantir qu'une requête prévue à T+5s ne puisse pas être
// retardée de plusieurs dizaines de secondes uniquement par notre queue").
const BC_DEADLINE_MS = Number(process.env.BC_DEADLINE_MS) || 18_000;
// Post-mortem du 2026-08-25, quelques heures après le premier déploiement
// (scripts/analyze-adaptive-throttle.js) : 99% des lectures tombaient sur
// le garde-fou (~18000ms) au lieu d'accélérer. Cause — ce seuil à 250ms
// était calibré sur la latence mesurée à l'ANCIEN espacement fixe (300ms,
// P90=207ms, sans contention), pas sur la réalité de l'adaptatif lui-même
// : dès que la file accélère un peu, le P90 de rpc_call_ms observé monte
// à 700-744ms (contention réseau normale, pas des 429) — bien plus de 10%
// des appels dépassaient donc 250ms, chacun déclenchant noteRateLimited()
// à tort (intervalle doublé, capacité effondrée à 1, palier de propreté
// remis à zéro), empêchant quasi toujours d'atteindre les
// BC_CLEAN_STREAK_TO_SPEED_UP lectures propres nécessaires pour accélérer
// — la file restait donc bloquée près de son plafond, engorgée. Relevé à
// 450ms : sous le premier palier de backoff réel de rpcCall (500ms), donc
// toujours capable de détecter un vrai retry, mais au-dessus du bruit de
// contention observé en production.
const BC_SOFT_429_RPC_CALL_MS_THRESHOLD = Number(process.env.BC_SOFT_429_RPC_CALL_MS_THRESHOLD) || 450;

// File holders SÉPARÉE (2026-08-24) — voir en-tête de createRpcThrottle :
// une capture holders (~20 appels internes à holders.js, chacun retenté
// avec backoff sur 429) ne doit plus bloquer la file bonding curve. Son
// propre espacement reste plus prudent (holders.js espace déjà ses appels
// internes de 500ms) puisque le RPC public nous throttle systématiquement
// dessus (100% d'échecs HTTP 429 constatés le 2026-08-24, voir
// scripts/check-holders-error.js) — pas la peine d'insister plus vite.
const HOLDERS_RPC_MIN_INTERVAL_MS = Number(process.env.HOLDERS_RPC_MIN_INTERVAL_MS) || 500;

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
    // curve_completed_at_observed (2026-08-24) : posé une seule fois, à la
    // première lecture RPC qui montre la curve terminée — .is(...,null)
    // rend l'appel idempotent (un second appel pour le même mint, en
    // théorie impossible puisque la cascade s'arrête dès isResolved(),
    // resterait sans effet plutôt que d'écraser la première valeur).
    // Horloge séparée de migrated_at (pumpportal_migration_at) : voir
    // sql/schema.sql pour curve_completion_lag_seconds, calculé par
    // Postgres à partir des deux.
    async markCurveCompleted(mint) {
      const { error } = await supabaseClient
        .from('tokens')
        .update({ curve_completed_at: new Date().toISOString() })
        .eq('mint', mint)
        .is('curve_completed_at', null);
      if (error) throw new Error(`markCurveCompleted: ${error.message}`);
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
// confondus. Chaque appel attend son tour puis s'exécute. Retourne
// {result, queueWaitMs, rpcCallMs} plutôt que le résultat nu — mesure
// ajoutée le 2026-08-24 après avoir constaté des lectures bonding curve
// enregistrées avec des heures de retard sur leur délai nominal, sans
// pouvoir dire si le temps se perdait en file ou dans l'appel RPC
// lui-même (retries sur 429 inclus). queueWaitMs = temps entre l'entrée
// dans la file et le début de l'exécution ; rpcCallMs = durée de fn()
// elle-même. Les deux sont écrits sur token_snapshots (voir
// captureCascadeRead) pour pouvoir le mesurer après coup sans deviner.
function createRpcThrottle(minIntervalMs) {
  let queue = Promise.resolve();
  return function enqueue(fn, scheduledAtMs) {
    const queuedAtMs = Date.now();
    const timed = queue.then(async () => {
      const startedAtMs = Date.now();
      const result = await fn();
      return {
        result,
        queueWaitMs: startedAtMs - queuedAtMs,
        rpcCallMs: Date.now() - startedAtMs,
        // scheduledAt/queuedAt/startedAt/completedAt (2026-08-25) : fournis
        // aussi ici, pas seulement par le throttle adaptatif, depuis que
        // rpcThrottle (bonding curve) est repassé sur cette file simple —
        // voir le post-mortem sur createAdaptiveBondingCurveThrottle
        // ci-dessous pour le pourquoi.
        scheduledAt: scheduledAtMs ?? null,
        queuedAt: queuedAtMs,
        startedAt: startedAtMs,
        completedAt: Date.now(),
      };
    });
    queue = timed.catch(() => {}).then(() => new Promise((resolve) => setTimeout(resolve, minIntervalMs)));
    return timed;
  };
}

// ==========================================================================
// DÉSACTIVÉ le 2026-08-25 (voir main(), rpcThrottle est repassé sur
// createRpcThrottle ci-dessus) — POST-MORTEM du 3e problème découvert sur
// ce throttle adaptatif, plus grave que les deux précédents (engorgement à
// 99%, puis rythme des lectures forcées trop rapide) : une VRAIE divergence
// de file, avec perte silencieuse de lectures.
//
// Preuve (scripts/inspect-queue-wait-outlier.js, ligne réelle en base) :
// une lecture mise en file (queued_at) à 22:38:37, dispatchée (started_at)
// seulement à 02:36:35 le lendemain — ~3h58 d'attente RÉELLE, pas un bug
// de mesure. Le process listener s'auto-relance toutes les ~5h50
// (MAX_RUNTIME_MS) via process.exit(0), qui tue IMMÉDIATEMENT tout ce qui
// reste dans `pending` sans le vider ni le journaliser — donc la majorité
// du retard réel (au-delà de ce qui a eu la chance d'être dispatché dans
// les quelques secondes entre le déclenchement de l'arrêt et l'exit final)
// n'apparaît même pas dans token_snapshots : ces lectures n'ont juste
// jamais eu lieu, silencieusement.
//
// Cause racine : le 2e correctif (pacer les lectures forcées au rythme
// intervalMs courant plutôt qu'à BC_MIN_INTERVAL_MS) était correct dans
// son principe, mais expose un angle mort — rien ne garantit que le
// rythme "sûr" choisi par l'AIMD reste AU-DESSUS du débit réel
// d'arrivée des lectures programmées. Dès qu'un nombre suffisant de
// faux/vrais signaux de ralentissement pousse intervalMs vers
// BC_MAX_INTERVAL_MS (3000ms, soit ~0,33 req/s) et l'y maintient — la
// récupération est lente et additive (BC_INTERVAL_STEP_DOWN_MS=20ms, sous
// condition d'un palier de BC_CLEAN_STREAK_TO_SPEED_UP=30 lectures propres
// CONSÉCUTIVES, statistiquement difficile à atteindre avec un taux de
// signaux mesuré à ~9% même en régime calme) — alors que le débit réel
// d'arrivée mesuré ailleurs (~1,5-2 req/s) reste largement au-dessus, la
// file diverge : ce que l'AIMD croit "prudent" n'a aucune garantie
// d'être "suffisant". L'ancien espacement fixe (300ms, ~3,33 req/s) n'a
// jamais ce problème car son débit ne descend JAMAIS, quel que soit le
// signal — c'est précisément pour ça qu'il est resté stable, quoique avec
// une latence de queue en rafale non bornée (P99=121s, max=145s), sans
// jamais perdre une seule lecture programmée.
//
// Le code ci-dessous est conservé (fonctionnel, testé hors ligne — voir
// les scénarios AIMD/garde-fou/pacage dans le scratchpad de la session)
// pour référence en cas de reprise future d'une adaptation plus prudente
// (ex. surveiller la profondeur de `pending` elle-même comme signal
// PRIORITAIRE de reprise de vitesse, plutôt que de se fier uniquement à
// des séries de lectures propres). Pas branché dans main() pour l'instant.
// ==========================================================================
//
// Throttle adaptatif dédié à la bonding curve (2026-08-25) — voir les
// constantes BC_* juste au-dessus pour le contexte/les chiffres mesurés
// qui ont motivé ce choix. Seau de jetons (token bucket) : `capacity`
// jetons disponibles immédiatement à tout instant (absorbe une rafale de
// créations sans attendre), rechargés au rythme `intervalMs`. Les deux
// s'ajustent en continu — AIMD (Additive Increase / Multiplicative
// Decrease, le mécanisme standard des limiteurs de débit adaptatifs) :
// accélère prudemment par petits paliers après une série de lectures
// propres, ralentit fort et tout de suite dès qu'un 429 est rencontré
// (même rattrapé par le retry interne de rpcCall — voir
// BC_SOFT_429_RPC_CALL_MS_THRESHOLD). Le garde-fou BC_DEADLINE_MS force le
// passage d'une lecture qui aurait trop attendu même sans jeton
// disponible : sans lui, l'adaptatif pourrait rester prudent
// indéfiniment pendant qu'une lecture reste bloquée derrière une rafale.
// N'est PAS utilisé pour holders (reste sur createRpcThrottle ci-dessus,
// simple espacement fixe) — inchangé par design (100% de 429 mesurés
// là-bas, rien à gagner à accélérer tant que cet endpoint refuse
// systématiquement).
function createAdaptiveBondingCurveThrottle() {
  let intervalMs = BC_INITIAL_INTERVAL_MS;
  let capacity = BC_MIN_CAPACITY;
  let tokens = capacity;
  let cleanStreak = 0;
  let lastRefillAt = Date.now();
  const pending = [];
  let pumpTimer = null;
  // Dernière fois qu'une lecture est passée par le chemin FORCÉ (garde-
  // fou de délai) — sert uniquement à espacer deux passages forcés
  // consécutifs d'au moins intervalMs (voir pump(), 2e correctif du
  // 2026-08-25). 0 = jamais encore forcé, donc "il y a très longtemps" en
  // temps réel : le tout premier passage forcé n'a rien à espacer par
  // rapport à.
  let lastForcedDispatchAt = 0;

  function refill() {
    const now = Date.now();
    const elapsed = now - lastRefillAt;
    if (elapsed <= 0) return;
    const newTokens = Math.floor(elapsed / intervalMs);
    if (newTokens > 0) {
      tokens = Math.min(capacity, tokens + newTokens);
      lastRefillAt += newTokens * intervalMs;
    }
  }

  function noteClean() {
    cleanStreak += 1;
    if (cleanStreak % BC_CLEAN_STREAK_TO_SPEED_UP !== 0) return;
    intervalMs = Math.max(BC_MIN_INTERVAL_MS, intervalMs - BC_INTERVAL_STEP_DOWN_MS);
    if (cleanStreak % (BC_CLEAN_STREAK_TO_SPEED_UP * 2) === 0) {
      capacity = Math.min(BC_MAX_CAPACITY, capacity + 1);
    }
  }

  function noteRateLimited() {
    cleanStreak = 0;
    intervalMs = Math.min(BC_MAX_INTERVAL_MS, Math.round(intervalMs * BC_INTERVAL_BACKOFF_FACTOR));
    capacity = BC_MIN_CAPACITY;
    tokens = Math.min(tokens, capacity);
  }

  function schedulePump(delayMs) {
    if (pumpTimer) return;
    pumpTimer = setTimeout(() => {
      pumpTimer = null;
      pump();
    }, Math.max(0, delayMs));
  }

  function pump() {
    if (!pending.length) return;
    refill();
    // 1) Dispatch normal via les jetons disponibles — peut lâcher jusqu'à
    //    `capacity` lectures d'un coup (rafale VOULUE, bornée par le
    //    seau de jetons).
    while (pending.length && tokens >= 1) {
      const item = pending[0];
      pending.shift();
      tokens -= 1;
      dispatch(item, false);
    }
    // 2) Garde-fou de délai : AU PLUS UNE lecture forcée par appel de
    //    pump(), jamais toute la file en une fois — ET jamais plus vite
    //    que le rythme COURANT de l'adaptatif (intervalMs) depuis la
    //    dernière lecture forcée. Deux correctifs cumulés du 2026-08-25,
    //    tous deux découverts en mesurant en production
    //    (scripts/analyze-adaptive-throttle.js) :
    //    1er (8d5c872) : sans la limite "une seule par appel", un
    //       engorgement faisait passer TOUTE la file en retard d'un coup
    //       dès qu'elle atteignait le seuil — rafale simultanée vers le
    //       RPC, contention réelle, re-déclenchement de noteRateLimited().
    //    2e (celui-ci) : même après le 1er correctif, le pacage entre
    //       lectures forcées consécutives utilisait BC_MIN_INTERVAL_MS
    //       (80ms en prod) comme plancher — dès qu'une bonne part de la
    //       file passait par le garde-fou (observé : 82,7% après 8min,
    //       95,55% après 1h, donc en aggravation), le débit RÉEL vers le
    //       RPC se calait sur ce plancher (~12,5 req/s), largement
    //       au-dessus du rythme fixe de 300ms déjà validé sans échec, et
    //       bien plus vite que ce que l'adaptatif jugeait prudent à cet
    //       instant (jusqu'à BC_MAX_INTERVAL_MS=3000ms après
    //       ralentissement). Le garde-fou devenait de facto le chemin
    //       PRINCIPAL, plus une exception. La détection "délai dépassé"
    //       (pastDeadline, en temps réel) reste séparée du pacage du
    //       rythme d'émission (pacingOk, contre lastForcedDispatchAt) :
    //       une lecture n'est forcée QUE si les deux sont vrais.
    //       Conséquence assumée : sous surcharge réelle et soutenue,
    //       l'attente peut dépasser BC_DEADLINE_MS pour certaines
    //       lectures — préférable à re-déclencher la surcharge en
    //       essayant de tenir un délai que le rythme sûr actuel ne
    //       permet pas.
    if (pending.length) {
      const now = Date.now();
      const item = pending[0];
      const pastDeadline = now - item.queuedAtMs >= BC_DEADLINE_MS;
      const pacingOk = now - lastForcedDispatchAt >= intervalMs;
      if (pastDeadline && pacingOk) {
        pending.shift();
        lastForcedDispatchAt = now;
        dispatch(item, true);
      }
    }
    if (pending.length) {
      // Réveille dès que : un jeton devient disponible, OU l'item le plus
      // ancien a ET dépassé son garde-fou ET le pacage entre lectures
      // forcées redevient permis (les deux conditions de la section
      // ci-dessus) — jamais avant, jamais après.
      const now = Date.now();
      const item = pending[0];
      const timeToNextToken = intervalMs - (now - lastRefillAt);
      const timeToOwnDeadline = BC_DEADLINE_MS - (now - item.queuedAtMs);
      const timeToPacingOk = intervalMs - (now - lastForcedDispatchAt);
      const timeToForceable = Math.max(timeToOwnDeadline, timeToPacingOk);
      schedulePump(Math.min(timeToNextToken, timeToForceable));
    }
  }

  async function dispatch(item, forcedByDeadline) {
    const startedAtMs = Date.now();
    try {
      const result = await item.fn();
      const rpcCallMs = Date.now() - startedAtMs;
      if (rpcCallMs >= BC_SOFT_429_RPC_CALL_MS_THRESHOLD) noteRateLimited();
      else noteClean();
      item.resolve({
        result,
        queueWaitMs: startedAtMs - item.queuedAtMs,
        rpcCallMs,
        scheduledAt: item.scheduledAtMs ?? null,
        queuedAt: item.queuedAtMs,
        startedAt: startedAtMs,
        completedAt: Date.now(),
        forcedByDeadline,
      });
    } catch (err) {
      if (/429/.test(err.message)) noteRateLimited();
      item.reject(err);
    }
    // Pas de pump() ici (post-mortem 2026-08-25) : rien ne devient
    // dispatchable PARCE QU'un appel vient de se terminer — les jetons se
    // rechargent avec le temps (déjà couvert par le minuteur de
    // schedulePump), pas à la complétion d'un appel. Un pump() ici
    // court-circuitait l'espacement du garde-fou de délai : la résolution
    // d'un dispatch forcé relançait aussitôt pump() (dans un microtask,
    // donc quasi au même instant), qui forçait le suivant, qui relançait
    // encore pump()... — toute la file en retard partait alors d'un coup
    // au lieu d'être étalée par schedulePump().
  }

  return function enqueue(fn, scheduledAtMs) {
    return new Promise((resolve, reject) => {
      pending.push({ fn, queuedAtMs: Date.now(), scheduledAtMs, resolve, reject });
      pump();
    });
  };
}

// Budget quotidien holders (2026-08-24) : découvert que le RPC public
// nous renvoie HTTP 429 sur 100% des tentatives holders observées
// (scripts/check-holders-error.js) — chaque tentative épuise ses retries
// avant d'échouer, occupant sa propre file (désormais séparée de la file
// bonding curve, voir main()) pendant plusieurs secondes pour rien à
// chaque fois. Un plafond quotidien évite de marteler indéfiniment un
// endpoint qui refuse systématiquement, même une fois isolé dans sa
// propre file — la bonding curve reste prioritaire par construction
// (files indépendantes), ceci est une protection supplémentaire.
const HOLDERS_DAILY_BUDGET = Number(process.env.HOLDERS_DAILY_BUDGET) || 3000;
let holdersBudgetDay = null;
let holdersBudgetUsed = 0;
function holdersBudgetAvailable() {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== holdersBudgetDay) {
    holdersBudgetDay = today;
    holdersBudgetUsed = 0;
  }
  return holdersBudgetUsed < HOLDERS_DAILY_BUDGET;
}
function consumeHoldersBudget() {
  holdersBudgetUsed += 1;
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
//
// rpcThrottle et holdersThrottle sont deux files INDÉPENDANTES depuis le
// 2026-08-24 : une capture holders peut prendre plusieurs secondes
// (retries sur 429 compris — voir plus bas, le RPC public nous throttle
// systématiquement sur ces appels) sans jamais retarder une lecture
// bonding curve, qui reste prioritaire par construction plutôt que par
// promesse.
async function captureCascadeRead(
  db,
  rpcThrottle,
  holdersThrottle,
  mint,
  createdAtMs,
  initialVSol,
  nominalDelayS,
  { captureHolders = false, fetchFn = fetchBondingCurveState, holdersFetchFn = fetchHolderConcentration } = {}
) {
  const ageSeconds = Math.round((Date.now() - createdAtMs) / 1000);
  const scheduledAtMs = createdAtMs + nominalDelayS * 1000;
  try {
    const {
      result: state,
      queueWaitMs,
      rpcCallMs,
      scheduledAt,
      queuedAt,
      startedAt,
      completedAt,
    } = await rpcThrottle(() => fetchFn(SOLANA_RPC_URL, mint), scheduledAtMs);
    const row = {
      mint,
      age_seconds: ageSeconds,
      nominal_delay_s: nominalDelayS,
      virtual_sol_reserves: state.virtual_quote_reserves_sol,
      virtual_token_reserves: state.virtual_token_reserves,
      raw_event: state,
      queue_wait_ms: queueWaitMs,
      rpc_call_ms: rpcCallMs,
      // Horodatages absolus (2026-08-25), en plus des durées dérivées
      // ci-dessus — demandé explicitement pour pouvoir rejouer précisément
      // "prévu vs exécuté" plus tard sans recalcul approximatif.
      // scheduledAt/queuedAt/startedAt/completedAt sont undefined pour
      // holders (createRpcThrottle, inchangé) mais rpcThrottle ici est
      // toujours le throttle adaptatif bonding curve, qui les fournit
      // systématiquement.
      scheduled_at: scheduledAt != null ? new Date(scheduledAt).toISOString() : null,
      queued_at: queuedAt != null ? new Date(queuedAt).toISOString() : null,
      started_at: startedAt != null ? new Date(startedAt).toISOString() : null,
      completed_at: completedAt != null ? new Date(completedAt).toISOString() : null,
    };
    if (captureHolders && holdersBudgetAvailable()) {
      consumeHoldersBudget();
      try {
        const { pda } = deriveBondingCurvePda(mint);
        const { result: holders, queueWaitMs: holdersQueueWaitMs, rpcCallMs: holdersRpcCallMs } = await holdersThrottle(() =>
          holdersFetchFn(SOLANA_RPC_URL, mint, pda)
        );
        row.total_supply = holders.total_supply;
        row.curve_held_amount = holders.curve_held_amount;
        row.top_holders_count = holders.top_holders_count;
        row.top_holders_pct_of_supply = holders.top_holders_pct_of_supply;
        row.holders_raw = holders;
        row.holders_queue_wait_ms = holdersQueueWaitMs;
        row.holders_rpc_call_ms = holdersRpcCallMs;
      } catch (err) {
        row.holders_error = err.message;
      }
    }
    // Filet de sécurité (2026-08-26) : observé en production, 100% des
    // insertSnapshot échouant sur token_snapshots_mint_fkey juste après un
    // reset de la base, y compris pour des tokens vieux de 2s dont l'upsert
    // dans `tokens` n'avait pourtant renvoyé aucune erreur - cause exacte
    // non confirmée (écarté : dédoublonnage en mémoire, cache, trigger/RLS
    // côté DB, connexion figée d'un process long-lived - un process
    // fraîchement relancé reproduisait quand même le problème). Ressemble à
    // un aléa de cohérence lecture-après-écriture côté Supabase plutôt qu'à
    // un bug de logique ici. Une seule retentative après un court délai
    // absorbe ce cas sans changer le comportement normal (chemin heureux
    // inchangé, coût nul si insertSnapshot réussit du premier coup).
    try {
      await db.insertSnapshot(row);
    } catch (err) {
      if (!/token_snapshots_mint_fkey/.test(err.message)) throw err;
      await new Promise((r) => setTimeout(r, INSERT_SNAPSHOT_FK_RETRY_DELAY_MS));
      await db.insertSnapshot(row);
    }

    const ratio = initialVSol ? state.virtual_quote_reserves_sol / initialVSol : null;
    await db.updateTokenDerivedMetrics(mint, { ratio, ageSeconds, checkpointColumn: CHECKPOINT_COLUMNS[nominalDelayS] });

    // curve_completed_at_observed (2026-08-24) : premier instant où LE RPC
    // lui-même montre la curve terminée, indépendant de subscribeMigration
    // (mesuré en retard de plusieurs minutes à quelques dizaines de
    // minutes sur l'état réel — voir src/report.js, section CALIBRATION).
    // pumpportal_migration_at (colonne migrated_at) n'est ni modifiée ni
    // remplacée : les deux horloges restent séparées, l'écart entre elles
    // est calculé par Postgres (curve_completion_lag_seconds).
    if (state.complete || state.virtual_quote_reserves_sol === 0) {
      await db.markCurveCompleted(mint).catch(() => {});
    }

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
  holdersThrottle,
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
    const result = await captureCascadeRead(db, rpcThrottle, holdersThrottle, mint, createdAtMs, initialVSol, delayS, { fetchFn, holdersFetchFn, ...opts });
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
  // Espacement fixe (300ms), PAS le throttle adaptatif — désactivé le
  // 2026-08-25 après avoir confirmé qu'il pouvait diverger (file jamais
  // vidée, lectures perdues silencieusement à chaque auto-relance du
  // process) quand son rythme "sûr" retombait sous le débit réel
  // d'arrivée. Voir le post-mortem complet juste au-dessus de
  // createAdaptiveBondingCurveThrottle. Cet espacement fixe reste la
  // configuration validée sans perte sur ~51 000 requêtes (0% d'échec
  // définitif par 429) — sa seule limite connue (latence de queue en
  // rafale, P99=121s/max=145s) est préférable à une divergence.
  const rpcThrottle = createRpcThrottle(BONDING_CURVE_RPC_MIN_INTERVAL_MS);
  // File indépendante pour holders (2026-08-24) : une capture holders ne
  // doit plus jamais retarder une lecture bonding curve, qui reste
  // prioritaire par construction (deux files séparées) plutôt que par
  // promesse sur une file partagée — voir l'en-tête de createRpcThrottle.
  const holdersThrottle = createRpcThrottle(HOLDERS_RPC_MIN_INTERVAL_MS);

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
        // Plus besoin de suivre les cascades en cours (activeCascades a été
        // retiré le 2026-08-24) : subscribeMigration ne déclenche plus
        // d'arrêt externe (voir le handler 'migration' ci-dessous), donc
        // rien n'a plus besoin d'appeler .stop() depuis l'extérieur — une
        // cascade s'arrête seule, sur son propre isResolved().
        db.upsertNewTokens([row])
          .then(() => {
            scheduleTokenCascade(
              db,
              rpcThrottle,
              holdersThrottle,
              row.mint,
              Date.parse(row.created_at),
              row.initial_virtual_sol_reserves,
              () => shuttingDown
            );
          })
          .catch((err) => {
            db.logIngestion('bonding_curve_snapshot_error', `upsert immédiat échoué pour ${row.mint}, cascade non programmée: ${err.message}`).catch(() => {});
          });
      } else if (type === 'migration') {
        const migRow = buildMigrationRow(msg, nowIso);
        buffer.addMigration(migRow);
        // NE déclenche PLUS l'arrêt de la cascade (changé le 2026-08-24) :
        // subscribeMigration s'est révélé arriver en retard de plusieurs
        // minutes, parfois plus de 30, sur l'état RPC réel (voir
        // src/report.js, section CALIBRATION) — l'utiliser comme condition
        // d'arrêt faisait manquer la confirmation RPC de la complétion la
        // plupart du temps. migrated/migrated_at restent enregistrés
        // normalement ci-dessus, comme information séparée
        // (pumpportal_migration_at) ; seule captureCascadeRead (RPC
        // lui-même montrant complete=true ou réserve à 0) arrête
        // désormais une cascade.
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
  createAdaptiveBondingCurveThrottle,
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

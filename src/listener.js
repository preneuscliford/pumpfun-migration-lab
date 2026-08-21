#!/usr/bin/env node
'use strict';

// Écouteur PumpPortal — subscribeNewToken + subscribeMigration uniquement
// (toujours pas de subscribeTokenTrade). Aucun trading, aucune clé de
// wallet : ce script lit un flux public, plus quelques appels RPC Solana
// publics en lecture seule, et écrit dans Supabase.
//
// Expérience "trajectoire de la bonding curve" (2026-08-21) : pour chaque
// token créé, on programme 4 lectures ponctuelles de sa bonding curve via
// RPC Solana public (getAccountInfo, voir src/bondingCurve.js) à délais
// fixes après la création — pas de polling continu, pas de
// subscribeTokenTrade, pas de reconstruction via l'historique de
// transactions. Objectif unique : voir si la progression vQuote/vToken à
// 1/3/5min distingue les migrations progressives (groupe B, >10s) des
// tokens qui ne migreront jamais (groupe C, témoin). Le calcul de la
// progression elle-même se fait dans report.js, pas ici — le listener ne
// fait qu'enregistrer les points de mesure bruts dans token_snapshots.
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

// Délais après création auxquels on lit la bonding curve (30s/1min/3min/
// 5min par défaut — voir en-tête du fichier). Liste de millisecondes
// séparées par des virgules.
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
const BONDING_CURVE_SNAPSHOT_DELAYS_MS = (process.env.BONDING_CURVE_SNAPSHOT_DELAYS_MS || '30000,60000,180000,300000')
  .split(',')
  .map(Number)
  .filter((n) => Number.isFinite(n) && n > 0);
// Espacement minimum entre deux appels RPC bonding-curve, tous tokens
// confondus — protège le RPC public gratuit d'un afflux de créations
// groupées plutôt que de compter sur le hasard de l'espacement naturel.
const BONDING_CURVE_RPC_MIN_INTERVAL_MS = Number(process.env.BONDING_CURVE_RPC_MIN_INTERVAL_MS) || 300;

// Concentration des détenteurs (2026-08-21, voir sql/schema.sql) : capturée
// UNE SEULE FOIS par token, sur le snapshot dont le délai correspond à
// cette valeur — pas à chaque snapshot, ~20 appels RPC par lecture (voir
// src/holders.js) sur un listener qui suit TOUS les tokens créés (pas le
// sous-ensemble filtré de scripts/watch-axiom-candidates.js), donc un
// point dans le temps plutôt qu'une trajectoire complète pour rester
// raisonnable envers le quota RPC. Doit correspondre à une valeur présente
// dans BONDING_CURVE_SNAPSHOT_DELAYS_MS, sinon elle n'est jamais capturée.
const HOLDERS_SNAPSHOT_DELAY_MS = Number(process.env.HOLDERS_SNAPSHOT_DELAY_MS) || 30000;

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
    // Ferme la fenêtre d'observation des tokens non migrés créés il y a
    // plus de OBSERVATION_WINDOW_MS. Ne touche PAS aux lignes tokens
    // elles-mêmes (le résumé est gardé pour tous, migrés ou non — voir
    // README). La purge de token_snapshots est un TODO pour l'option B :
    // en V1 cette table reste vide, rien à purger.
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
// Buffer d'événements — regroupe les écritures pour rester économe en
// requêtes plutôt qu'un insert par événement.
// --------------------------------------------------------------------

class EventBuffer {
  constructor(db) {
    this.db = db;
    this.newTokenRows = [];
    this.migrationRows = [];
  }

  addNewToken(row) {
    this.newTokenRows.push(row);
  }

  addMigration(row) {
    this.migrationRows.push(row);
  }

  get size() {
    return this.newTokenRows.length + this.migrationRows.length;
  }

  async flush() {
    const tokens = this.newTokenRows.splice(0, this.newTokenRows.length);
    const migrations = this.migrationRows.splice(0, this.migrationRows.length);
    if (tokens.length) await this.db.upsertNewTokens(tokens);
    if (migrations.length) await this.db.upsertMigrations(migrations);
    return { tokens: tokens.length, migrations: migrations.length };
  }
}

// --------------------------------------------------------------------
// Snapshots de bonding curve — trajectoire à délais fixes après création
// (voir en-tête du fichier). Pas de polling continu : une poignée de
// lectures ponctuelles par token, sérialisées via une file d'attente pour
// rester raisonnable envers le RPC public gratuit.
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

// holdersFetchFn injectable séparément de fetchFn (test/integration.js n'a
// pas besoin de fausser les deux de la même façon — la bonding curve et les
// holders touchent des méthodes RPC différentes).
async function captureBondingCurveSnapshot(
  db,
  rpcThrottle,
  mint,
  createdAtMs,
  { captureHolders = false, fetchFn = fetchBondingCurveState, holdersFetchFn = fetchHolderConcentration } = {}
) {
  const ageSeconds = Math.round((Date.now() - createdAtMs) / 1000);
  try {
    const state = await rpcThrottle(() => fetchFn(SOLANA_RPC_URL, mint));
    const row = {
      mint,
      age_seconds: ageSeconds,
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
  } catch (err) {
    await db.logIngestion('bonding_curve_snapshot_error', `${mint} @${ageSeconds}s: ${err.message}`).catch(() => {});
  }
}

// Programme les lectures pour un token qui vient d'être créé. isShuttingDown
// est une fonction (pas une valeur) car on veut lire l'état AU MOMENT où le
// timer se déclenche, pas au moment où il est programmé — un run peut
// commencer son arrêt entre les deux (voir main()). On laisse alors ce
// snapshot de côté plutôt que de risquer une écriture pendant le relais.
function scheduleBondingCurveSnapshots(
  db,
  rpcThrottle,
  mint,
  createdAtMs,
  isShuttingDown,
  delaysMs = BONDING_CURVE_SNAPSHOT_DELAYS_MS,
  holdersDelayMs = HOLDERS_SNAPSHOT_DELAY_MS
) {
  return delaysMs.map((delayMs) => {
    const remaining = Math.max(delayMs - (Date.now() - createdAtMs), 0);
    return setTimeout(() => {
      if (isShuttingDown()) return;
      captureBondingCurveSnapshot(db, rpcThrottle, mint, createdAtMs, { captureHolders: delayMs === holdersDelayMs }).catch(() => {});
    }, remaining);
  });
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
        buffer.addNewToken(row);
        scheduleBondingCurveSnapshots(db, rpcThrottle, row.mint, Date.parse(row.created_at), () => shuttingDown);
      } else if (type === 'migration') {
        buffer.addMigration(buildMigrationRow(msg, nowIso));
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
  captureBondingCurveSnapshot,
  scheduleBondingCurveSnapshots,
  main,
};

if (require.main === module) {
  main().catch((err) => {
    console.error('Erreur fatale:', err.message);
    process.exit(1);
  });
}

#!/usr/bin/env node
'use strict';

// Écouteur PumpPortal — V1 : subscribeNewToken + subscribeMigration
// uniquement (pas de subscribeTokenTrade, pas de polling). Aucun trading,
// aucune clé de wallet : ce script ne fait que lire un flux public et
// écrire dans Supabase.
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
        buffer.addNewToken(buildTokenRow(msg, nowIso));
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

module.exports = { classifyEvent, buildTokenRow, buildMigrationRow, createDb, EventBuffer, triggerNextRun, main };

if (require.main === module) {
  main().catch((err) => {
    console.error('Erreur fatale:', err.message);
    process.exit(1);
  });
}

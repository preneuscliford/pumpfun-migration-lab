#!/usr/bin/env node
'use strict';

// Test d'intégration local : fait tourner src/listener.js comme un vrai
// process enfant contre un faux serveur WS (imitant PumpPortal) et un
// faux serveur HTTP (imitant l'API REST Supabase), avec des timers
// raccourcis via variables d'environnement. Vérifie que le pipeline
// complet se connecte, s'abonne, classe et envoie les événements, se
// reconnecte après une coupure, et déclenche le relais de fin de run.
//
// N'appelle ni pumpportal.fun ni supabase.co — tout est local.
// Usage : node test/integration.js

const http = require('http');
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');
const path = require('path');

const httpRequests = [];
const httpServer = http.createServer((req, res) => {
  let body = '';
  req.on('data', (chunk) => (body += chunk));
  req.on('end', () => {
    httpRequests.push({ method: req.method, url: req.url, body });
    res.writeHead(201, { 'Content-Type': 'application/json', 'Content-Range': '0-0/1' });
    res.end('[]');
  });
});

const wss = new WebSocketServer({ port: 0 });
let subscriptions = [];
let reconnected = false;
let firstConnection = true;

wss.on('connection', (socket) => {
  const isReconnect = !firstConnection;
  firstConnection = false;
  if (isReconnect) reconnected = true;

  socket.on('message', (data) => {
    const msg = JSON.parse(data.toString());
    subscriptions.push(msg.method);
  });

  // Envoie un faux événement de création peu après la connexion.
  setTimeout(() => {
    socket.send(
      JSON.stringify({
        txType: 'create',
        mint: 'FakeMint111',
        name: 'Test Token',
        symbol: 'TEST',
        traderPublicKey: 'FakeDevWallet',
        vSolInBondingCurve: '30',
        vTokensInBondingCurve: '1000000000',
        marketCapSol: '27',
        bondingCurveKey: 'FakeCurve',
        uri: 'ipfs://fake',
      })
    );
  }, 200);

  // Puis un événement inconnu (doit être loggé, pas planter).
  setTimeout(() => {
    socket.send(JSON.stringify({ message: 'Successfully subscribed to keys.' }));
  }, 300);

  // Puis, seulement sur la PREMIERE connexion, force une coupure pour
  // tester la reconnexion automatique.
  if (!isReconnect) {
    setTimeout(() => socket.close(), 600);
  } else {
    // Sur la reconnexion, envoie un événement de migration.
    setTimeout(() => {
      socket.send(JSON.stringify({ txType: 'migrate', mint: 'FakeMint111', pool: 'raydium' }));
    }, 200);
  }
});

async function main() {
  await new Promise((resolve) => httpServer.listen(0, resolve));
  const httpPort = httpServer.address().port;
  const wsPort = wss.address().port;

  const child = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'listener.js')], {
    env: {
      ...process.env,
      SUPABASE_URL: `http://127.0.0.1:${httpPort}`,
      SUPABASE_SERVICE_KEY: 'fake-key-for-test',
      PUMPPORTAL_WS_URL: `ws://127.0.0.1:${wsPort}`,
      MAX_RUNTIME_MS: '2500',
      RELAY_CHECK_INTERVAL_MS: '500',
      FLUSH_INTERVAL_MS: '400',
      CLEANUP_INTERVAL_MS: '100000',
      STALE_CONNECTION_MS: '100000',
      RECONNECT_BASE_MS: '100',
      RECONNECT_MAX_MS: '300',
      GITHUB_TOKEN: '', // volontairement vide -> exerce le chemin "relance échouée, watchdog prendra le relais"
      GITHUB_REPOSITORY: '',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  child.stdout.on('data', (d) => (stdout += d));
  let stderr = '';
  child.stderr.on('data', (d) => (stderr += d));

  const exitCode = await new Promise((resolve) => {
    child.on('exit', (code) => resolve(code));
    setTimeout(() => {
      child.kill('SIGKILL');
      resolve('TIMEOUT');
    }, 8000);
  });

  console.log('--- stdout du process ---');
  console.log(stdout);
  if (stderr) {
    console.log('--- stderr ---');
    console.log(stderr);
  }

  const assert = require('assert');
  assert.strictEqual(exitCode, 0, `code de sortie attendu 0, obtenu ${exitCode}`);
  assert.ok(subscriptions.includes('subscribeNewToken'), 'devrait avoir envoyé subscribeNewToken');
  assert.ok(subscriptions.includes('subscribeMigration'), 'devrait avoir envoyé subscribeMigration');
  assert.ok(reconnected, 'devrait s\'être reconnecté après la coupure forcée');

  const tokensCalls = httpRequests.filter((r) => r.url.includes('/tokens'));
  assert.ok(tokensCalls.length >= 2, `devrait avoir au moins 2 appels vers tokens (création + migration), obtenu ${tokensCalls.length}`);
  const createdRow = JSON.parse(tokensCalls[0].body);
  assert.strictEqual(createdRow[0].mint, 'FakeMint111');
  assert.strictEqual(createdRow[0].symbol, 'TEST');

  const migrationCallBody = tokensCalls.find((c) => JSON.parse(c.body)[0].migrated === true);
  assert.ok(migrationCallBody, 'devrait avoir un appel upsert avec migrated:true');
  const migRow = JSON.parse(migrationCallBody.body)[0];
  assert.strictEqual(Object.keys(migRow).includes('name'), false, 'la ligne de migration ne doit PAS inclure name (pas d\'écrasement)');

  const unknownLog = httpRequests.find((r) => r.url.includes('/ingestion_log') && r.body.includes('unknown_event'));
  assert.ok(unknownLog, 'l\'événement non classifié devrait être loggé dans ingestion_log');

  const relayLog = httpRequests.find((r) => r.url.includes('/ingestion_log') && r.body.includes('relay_handoff'));
  assert.ok(relayLog, 'la fin de run devrait logguer relay_handoff');

  console.log('\nTOUS LES TESTS D\'INTEGRATION OK');
  console.log(`(${httpRequests.length} requêtes HTTP reçues, subscriptions=${subscriptions.join(',')}, reconnecté=${reconnected})`);

  httpServer.close();
  wss.close();
  process.exit(0);
}

main().catch((err) => {
  console.error('ECHEC:', err);
  httpServer.close();
  wss.close();
  process.exit(1);
});

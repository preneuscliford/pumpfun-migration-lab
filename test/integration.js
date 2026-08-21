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
const { Keypair } = require('@solana/web3.js');

// Mint valide (format base58 correct) pour que la dérivation de PDA côté
// listener ne plante pas — 'FakeMint111' ne suffirait pas, ce n'est pas un
// pubkey valide.
const fakeMint = Keypair.generate().publicKey.toBase58();

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

// Faux RPC Solana : répond à getAccountInfo avec un compte bonding-curve
// synthétique de 151 octets (même layout que src/bondingCurve.js), pour
// vérifier que le listener sait décoder une vraie réponse et écrire un
// snapshot — pas juste qu'il tente l'appel.
function buildFakeBondingCurveAccountBase64() {
  const buf = Buffer.alloc(151);
  let off = 8; // discriminator, contenu indifférent
  buf.writeBigUInt64LE(1073000000000000n, off); off += 8; // virtual_token_reserves
  buf.writeBigUInt64LE(30000000000n, off); off += 8; // virtual_quote_reserves (30 SOL)
  buf.writeBigUInt64LE(793100000000000n, off); off += 8; // real_token_reserves
  buf.writeBigUInt64LE(12000000000n, off); off += 8; // real_quote_reserves
  buf.writeBigUInt64LE(1000000000000000n, off); off += 8; // token_total_supply
  buf.writeUInt8(0, off); off += 1; // complete = false (encore en cours de bonding)
  Keypair.generate().publicKey.toBuffer().copy(buf, off); off += 32; // creator
  buf.writeUInt8(0, off); off += 1; // is_mayhem_mode
  buf.writeUInt8(0, off); off += 1; // is_cashback_coin
  Buffer.alloc(32).copy(buf, off); off += 32; // quote_mint = pubkey zéro (SOL natif)
  return buf.toString('base64');
}

// Concentration des holders (voir src/holders.js) : l'ATA de la bonding
// curve doit être dérivée avec le VRAI programme du mint (classique ici,
// pas Token-2022 — voir la découverte du 2026-08-21 dans holders.js), donc
// précalculée ici pour l'inclure dans la fausse réponse getTokenLargestAccounts
// et vérifier que le listener l'exclut correctement des holders.
const { deriveBondingCurvePda } = require('../src/bondingCurve');
const { deriveAta } = require('../src/holders');
const { PublicKey } = require('@solana/web3.js');
const CLASSIC_TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const { pda: fakeBondingCurvePda } = deriveBondingCurvePda(fakeMint);
const fakeCurveAta = deriveAta(fakeBondingCurvePda, new PublicKey(fakeMint), CLASSIC_TOKEN_PROGRAM_ID).toBase58();
const fakeHolderWallet = Keypair.generate().publicKey.toBase58();

const rpcRequests = [];
const rpcServer = http.createServer((req, res) => {
  let body = '';
  req.on('data', (chunk) => (body += chunk));
  req.on('end', () => {
    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch {
      parsed = {};
    }
    rpcRequests.push(parsed);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const [addressParam, optsParam] = parsed.params || [];
    if (parsed.method === 'getAccountInfo' && optsParam?.encoding === 'jsonParsed') {
      // Deux usages distincts derrière ce même (méthode, encodage) : lire le
      // programme propriétaire du mint (fetchMintTokenProgram) et résoudre
      // le propriétaire de chaque compte holder (boucle dans holders.js) —
      // la fausse réponse sert les deux, aucun des deux ne regarde l'adresse.
      res.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          result: { value: { owner: CLASSIC_TOKEN_PROGRAM_ID.toBase58(), data: { parsed: { info: { owner: fakeHolderWallet } } } } },
        })
      );
    } else if (parsed.method === 'getAccountInfo') {
      res.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          result: {
            value: {
              owner: '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
              data: [buildFakeBondingCurveAccountBase64(), 'base64'],
            },
          },
        })
      );
    } else if (parsed.method === 'getTokenSupply') {
      res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { value: { amount: '1000000000000000', decimals: 6 } } }));
    } else if (parsed.method === 'getTokenLargestAccounts') {
      res.end(
        JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          result: {
            value: [
              { address: fakeCurveAta, amount: '793100000000000' },
              { address: 'FakeHolderAccount1111111111111111111111111', amount: '5000000000' },
            ],
          },
        })
      );
    } else {
      res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result: null }));
    }
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
        mint: fakeMint,
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
      socket.send(JSON.stringify({ txType: 'migrate', mint: fakeMint, pool: 'raydium' }));
    }, 200);
  }
});

async function main() {
  await new Promise((resolve) => httpServer.listen(0, resolve));
  await new Promise((resolve) => rpcServer.listen(0, resolve));
  const httpPort = httpServer.address().port;
  const rpcPort = rpcServer.address().port;
  const wsPort = wss.address().port;

  const child = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'listener.js')], {
    env: {
      ...process.env,
      SUPABASE_URL: `http://127.0.0.1:${httpPort}`,
      SUPABASE_SERVICE_KEY: 'fake-key-for-test',
      PUMPPORTAL_WS_URL: `ws://127.0.0.1:${wsPort}`,
      SOLANA_RPC_URL: `http://127.0.0.1:${rpcPort}`,
      BONDING_CURVE_SNAPSHOT_DELAYS_MS: '50',
      HOLDERS_SNAPSHOT_DELAY_MS: '50',
      BONDING_CURVE_RPC_MIN_INTERVAL_MS: '10',
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
  assert.strictEqual(createdRow[0].mint, fakeMint);
  assert.strictEqual(createdRow[0].symbol, 'TEST');

  const migrationCallBody = tokensCalls.find((c) => JSON.parse(c.body)[0].migrated === true);
  assert.ok(migrationCallBody, 'devrait avoir un appel upsert avec migrated:true');
  const migRow = JSON.parse(migrationCallBody.body)[0];
  assert.strictEqual(Object.keys(migRow).includes('name'), false, 'la ligne de migration ne doit PAS inclure name (pas d\'écrasement)');

  const unknownLog = httpRequests.find((r) => r.url.includes('/ingestion_log') && r.body.includes('unknown_event'));
  assert.ok(unknownLog, 'l\'événement non classifié devrait être loggé dans ingestion_log');

  const relayLog = httpRequests.find((r) => r.url.includes('/ingestion_log') && r.body.includes('relay_handoff'));
  assert.ok(relayLog, 'la fin de run devrait logguer relay_handoff');

  // Trajectoire de la bonding curve : le délai de snapshot est raccourci à
  // 50ms (BONDING_CURVE_SNAPSHOT_DELAYS_MS), donc un seul snapshot est
  // attendu, tiré peu après la création — vérifie que le listener a bien
  // appelé le faux RPC (getAccountInfo) et écrit la ligne décodée.
  assert.ok(
    rpcRequests.some((r) => r.method === 'getAccountInfo'),
    'le listener devrait avoir appelé getAccountInfo pour lire la bonding curve'
  );
  const snapshotCall = httpRequests.find((r) => r.url.includes('/token_snapshots'));
  assert.ok(snapshotCall, 'devrait avoir écrit un snapshot dans token_snapshots');
  // insertSnapshot() envoie un objet unique (comme logIngestion), pas un
  // tableau (contrairement à upsertNewTokens/upsertMigrations) — pas de [0].
  const snapshotRow = JSON.parse(snapshotCall.body);
  assert.strictEqual(snapshotRow.mint, fakeMint);
  assert.strictEqual(snapshotRow.virtual_sol_reserves, 30); // 30000000000 lamports / 1e9, voir buildFakeBondingCurveAccountBase64
  assert.strictEqual(snapshotRow.virtual_token_reserves, '1073000000000000');
  assert.ok(typeof snapshotRow.age_seconds === 'number' && snapshotRow.age_seconds >= 0, 'age_seconds devrait être un entier positif');

  // Concentration des holders : HOLDERS_SNAPSHOT_DELAY_MS=50 coïncide avec
  // l'unique délai de snapshot testé, donc ce même snapshot doit porter les
  // colonnes holders — vérifie que le listener a bien appelé getTokenLargestAccounts/
  // getTokenSupply et exclu le compte de la curve (fakeCurveAta) du calcul.
  assert.ok(
    rpcRequests.some((r) => r.method === 'getTokenLargestAccounts'),
    'le listener devrait avoir appelé getTokenLargestAccounts pour les holders'
  );
  assert.strictEqual(snapshotRow.total_supply, 1000000000000000);
  assert.strictEqual(snapshotRow.curve_held_amount, 793100000000000, 'devrait identifier fakeCurveAta comme le compte de la curve');
  assert.strictEqual(snapshotRow.top_holders_count, 1, 'devrait exclure fakeCurveAta et ne garder que le vrai holder');
  assert.ok(Math.abs(snapshotRow.top_holders_pct_of_supply - 0.0005) < 1e-6, `top_holders_pct_of_supply inattendu: ${snapshotRow.top_holders_pct_of_supply}`);

  console.log('\nTOUS LES TESTS D\'INTEGRATION OK');
  console.log(`(${httpRequests.length} requêtes HTTP reçues, ${rpcRequests.length} requêtes RPC reçues, subscriptions=${subscriptions.join(',')}, reconnecté=${reconnected})`);

  httpServer.close();
  rpcServer.close();
  wss.close();
  process.exit(0);
}

main().catch((err) => {
  console.error('ECHEC:', err);
  httpServer.close();
  rpcServer.close();
  wss.close();
  process.exit(1);
});

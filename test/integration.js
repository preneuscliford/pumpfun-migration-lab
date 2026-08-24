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
  // vSolInBondingCurve délibérément DIFFÉRENT des 30 SOL renvoyés par le
  // faux RPC (buildFakeBondingCurveAccountBase64) — écart relatif de 50%,
  // très au-dessus d'ACTIVITY_REL_DEV_THRESHOLD, pour que ce token soit
  // jugé "actif" au gate et exerce la cascade étendue + les holders (gatés
  // sur l'activité depuis la V2).
  setTimeout(() => {
    socket.send(
      JSON.stringify({
        txType: 'create',
        mint: fakeMint,
        name: 'Test Token',
        symbol: 'TEST',
        traderPublicKey: 'FakeDevWallet',
        vSolInBondingCurve: '20',
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
      // Cascade V2 en secondes (fractionnaires ici pour rester rapide en
      // test) : gate à 100/150/200ms, étendue à 300/350/400/450ms — la
      // longue traîne (10s+) ne doit jamais se déclencher dans la fenêtre
      // du test, volontairement hors de portée de MAX_RUNTIME_MS ci-dessous.
      GATE_DELAYS_S: '0.1,0.15,0.2',
      EXTENDED_DELAYS_S: '0.3,0.35,0.4,0.45',
      LONG_TAIL_DELAYS_S: '10,20,30,40,50',
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

  // POST uniquement (upsert) : exclut les GET (.select().maybeSingle() de
  // updateTokenDerivedMetrics, corps vide) qui touchent aussi /tokens.
  const tokensCalls = httpRequests.filter((r) => r.url.includes('/tokens') && r.method === 'POST');
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

  // Cascade V2 : le token est actif dès le gate (vSolInBondingCurve=20 vs
  // 30 SOL renvoyés par le faux RPC, 50% d'écart) donc gate ET étendue
  // doivent tourner — au moins 7 lectures bonding curve (3 gate + 4
  // étendue), la longue traîne (10s+) hors de portée du test.
  const bondingCurveReads = rpcRequests.filter((r) => r.method === 'getAccountInfo' && r.params?.[1]?.encoding === 'base64');
  assert.ok(bondingCurveReads.length >= 7, `attendu >=7 lectures bonding curve (gate+étendue), obtenu ${bondingCurveReads.length}`);

  const snapshotCalls = httpRequests.filter((r) => r.url.includes('/token_snapshots'));
  assert.ok(snapshotCalls.length >= 7, `attendu >=7 snapshots écrits, obtenu ${snapshotCalls.length}`);
  // insertSnapshot() envoie un objet unique (comme logIngestion), pas un
  // tableau (contrairement à upsertNewTokens/upsertMigrations) — pas de [0].
  const snapshotRows = snapshotCalls.map((c) => JSON.parse(c.body));
  for (const row of snapshotRows) {
    assert.strictEqual(row.mint, fakeMint);
    assert.strictEqual(row.virtual_sol_reserves, 30); // 30000000000 lamports / 1e9, voir buildFakeBondingCurveAccountBase64
    assert.strictEqual(row.virtual_token_reserves, '1073000000000000');
    assert.ok(typeof row.age_seconds === 'number' && row.age_seconds >= 0, 'age_seconds devrait être un entier positif');
  }
  // nominal_delay_s couvre à la fois le gate (0.1/0.15/0.2) et l'étendue
  // (0.3/0.35/0.4/0.45) — preuve que le token classé actif a bien continué
  // au-delà du gate plutôt que de s'arrêter à 3 lectures.
  const nominalDelays = snapshotRows.map((r) => r.nominal_delay_s).sort((a, b) => a - b);
  assert.ok(nominalDelays.includes(0.1) && nominalDelays.includes(0.3), `attendu des lectures gate ET étendue, obtenu ${nominalDelays.join(',')}`);

  // Concentration des holders : gatée sur l'activité depuis la V2, capturée
  // au premier point de la cascade étendue (0.3s) — un seul snapshot doit
  // porter les colonnes holders, pas tous.
  assert.ok(
    rpcRequests.some((r) => r.method === 'getTokenLargestAccounts'),
    'le listener devrait avoir appelé getTokenLargestAccounts pour les holders'
  );
  const holdersRow = snapshotRows.find((r) => r.total_supply != null);
  assert.ok(holdersRow, 'un des snapshots devrait porter les données holders');
  assert.strictEqual(holdersRow.nominal_delay_s, 0.3, 'les holders devraient être capturés au premier point de la cascade étendue');
  assert.strictEqual(holdersRow.total_supply, 1000000000000000);
  assert.strictEqual(holdersRow.curve_held_amount, 793100000000000, 'devrait identifier fakeCurveAta comme le compte de la curve');
  assert.strictEqual(holdersRow.top_holders_count, 1, 'devrait exclure fakeCurveAta et ne garder que le vrai holder');
  assert.ok(Math.abs(holdersRow.top_holders_pct_of_supply - 0.0005) < 1e-6, `top_holders_pct_of_supply inattendu: ${holdersRow.top_holders_pct_of_supply}`);
  // 2, pas 1 : ce scénario envoie délibérément un événement de création en
  // double (à la reconnexion, voir plus haut) pour tester la reconnexion —
  // depuis que subscribeMigration ne déclenche plus l'arrêt d'une cascade
  // (2026-08-24, il s'est révélé en retard de plusieurs minutes sur l'état
  // RPC réel), rien n'annule plus plus la première cascade quand la seconde
  // démarre : les deux tournent indépendamment jusqu'à leur propre
  // résolution RPC, donc les holders sont capturés une fois par cascade.
  // Dans une création normale (non dupliquée), une seule cascade tourne et
  // les holders ne sont toujours capturés qu'une fois.
  assert.strictEqual(snapshotRows.filter((r) => r.total_supply != null).length, 2, 'les holders devraient être capturés une fois par cascade (2 cascades dans ce scénario de reconnexion)');

  // Métriques dérivées recopiées sur tokens en direct (updateTokenDerivedMetrics).
  const derivedUpdateCalls = httpRequests.filter(
    (r) => r.url.includes('/tokens') && r.method === 'PATCH' && (r.body.includes('bc_ratio_t') || r.body.includes('bc_cascade_reads'))
  );
  assert.ok(derivedUpdateCalls.length >= 7, `attendu >=7 mises à jour des métriques dérivées, obtenu ${derivedUpdateCalls.length}`);

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

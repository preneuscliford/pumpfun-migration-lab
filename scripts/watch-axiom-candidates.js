#!/usr/bin/env node
'use strict';

// Observation locale, ÉPHÉMÈRE : pas de Supabase, pas d'écriture nulle
// part, rien n'est stocké. Se connecte au flux public PumpPortal
// (subscribeNewToken uniquement — pas d'abonnement migration, on
// n'observe que la création), filtre les nouveaux tokens dont les
// features à la création tombent dans la plage P40-P60 du PROFIL DE
// RÉFÉRENCE GROUPE B (migrations progressives, calculé le 2026-08-21 —
// voir la section "PROFIL DE RÉFÉRENCE" de `node src/report.js`), puis
// VÉRIFIE l'état RÉEL de la bonding curve on-chain (via src/bondingCurve.js,
// même code que le listener/le script de diagnostic) avant d'ouvrir la
// page Axiom du token pour que vous le suiviez vous-même.
//
// Deuxième étage ajouté sur demande (2026-08-21) : le filtre sur les
// seules features WS de création laissait passer trop de tokens à la
// fois (~1 match/2-3s même en P40-P60). Le WS donne l'état ANNONCÉ à la
// création ; l'appel RPC donne l'état RÉEL au moment où on regarde,
// quelques centaines de ms plus tard — ça filtre aussi les tokens dont
// le compte a déjà disparu ou dont l'état ne correspond pas à ce qui a
// été annoncé.
//
// Local, pas en production : pas de file d'attente/throttle partagé
// comme dans le listener (300ms, pensé pour des milliers d'appels/jour
// sur un run continu) — ici le volume après filtre est déjà faible, donc
// chaque vérification part directement, jusqu'à la limite réelle du RPC
// public plutôt qu'une limite arbitraire choisie par prudence.
//
// Aucune décision automatique, aucun trading, aucun score : un filtre
// d'attention à deux étages, rien de plus. C'est vous qui suivez le
// token ensuite.
//
// Filtre étage 1 (P40-P60 du profil groupe B, snapshot du 2026-08-21, à
// recalculer via `node --env-file=.env src/report.js` quand le profil
// aura significativement bougé avec plus de données) :
//   - market cap initial (SOL) : [31.79, 33.83]
//   - achat du créateur (SOL)  : [1.99, 3.00]
// Filtre étage 2 : le compte bonding curve doit exister on-chain et ne
// pas être déjà `complete` (déjà migré/vidé — voir la validation du
// layout dans scripts/check-bonding-curve-rpc.js).
//
// Usage : node scripts/watch-axiom-candidates.js
// (Ctrl+C pour arrêter — tourne indéfiniment tant que vous le laissez.)

const WebSocket = require('ws');
const { exec } = require('child_process');
const { fetchBondingCurveState } = require('../src/bondingCurve');

const PUMPPORTAL_WS_URL = process.env.PUMPPORTAL_WS_URL || 'wss://pumpportal.fun/api/data';
const SOLANA_RPC_URL = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
// Coupe-circuit pour tester/régler le filtre sans spammer le navigateur —
// AXIOM_NO_OPEN=1 affiche toujours les correspondances mais n'ouvre rien.
const NO_OPEN = process.env.AXIOM_NO_OPEN === '1';

// P40-P60 (pas P25-P75, trop large : ~1 match/2-3s en test réel, voir
// commentaire ci-dessus) du profil groupe B, n=115, recalculé le
// 2026-08-21 directement sur market_cap_sol et solAmount (P50=P60 sur
// les deux : beaucoup de créateurs achètent des montants ronds
// identiques, d'où la borne haute qui colle à la médiane plutôt que de
// s'écarter).
const MARKET_CAP_SOL_RANGE = [31.79, 33.83];
const CREATOR_BUY_SOL_RANGE = [1.99, 3.0];

function inRange(v, [lo, hi]) {
  return typeof v === 'number' && !Number.isNaN(v) && v >= lo && v <= hi;
}

// Même heuristique que src/listener.js classifyEvent() : txType='create'
// en premier, repli sur la forme du message si absent (protocole non
// documenté officiellement par PumpPortal).
function isNewTokenEvent(msg) {
  if (!msg || typeof msg !== 'object') return false;
  if (msg.txType === 'create') return true;
  return Boolean(msg.mint && msg.name !== undefined && msg.symbol !== undefined && msg.bondingCurveKey);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Constat en test réel (2026-08-21) : vérifier le compte bonding curve
// IMMÉDIATEMENT après l'événement WS échoue systématiquement ("compte
// introuvable") — le RPC public gratuit met un instant à voir ce que
// PumpPortal a déjà vu. Réessaie quelques fois avec un court délai
// plutôt qu'un seul essai immédiat voué à l'échec.
async function fetchBondingCurveStateWithRetry(rpcUrl, mint, attempts = 4, delayMs = 500) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    if (i > 0) await sleep(delayMs);
    try {
      return await fetchBondingCurveState(rpcUrl, mint);
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr;
}

function openInBrowser(url) {
  const cmd =
    process.platform === 'win32' ? `start "" "${url}"` : process.platform === 'darwin' ? `open "${url}"` : `xdg-open "${url}"`;
  exec(cmd, (err) => {
    if (err) console.error(`  (échec d'ouverture automatique du navigateur : ${err.message} — ouvrez manuellement : ${url})`);
  });
}

function main() {
  const ws = new WebSocket(PUMPPORTAL_WS_URL);
  let seen = 0;
  let candidate = 0;
  let matched = 0;

  ws.on('open', () => {
    ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
    console.log(`[${new Date().toISOString()}] connecté à PumpPortal (subscribeNewToken uniquement).`);
    console.log(`Filtre actif : market_cap ∈ [${MARKET_CAP_SOL_RANGE.join(', ')}] SOL, achat_créateur ∈ [${CREATOR_BUY_SOL_RANGE.join(', ')}] SOL`);
    console.log('Rien n\'est stocké — observation locale uniquement. Ctrl+C pour arrêter.\n');
  });

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return; // message non-JSON (rare), ignoré
    }
    if (!isNewTokenEvent(msg)) return;
    seen += 1;

    const marketCap = Number(msg.marketCapSol);
    const creatorBuy = Number(msg.solAmount);
    const mayhem = typeof msg.is_mayhem_mode === 'boolean' ? msg.is_mayhem_mode : null;

    if (!inRange(marketCap, MARKET_CAP_SOL_RANGE) || !inRange(creatorBuy, CREATOR_BUY_SOL_RANGE)) return;

    // Étage 1 passé (features WS annoncées à la création) — étage 2 :
    // vérifier l'état RÉEL on-chain avant d'ouvrir quoi que ce soit.
    // Pas de throttle partagé ici : on est en local, volume déjà faible
    // après l'étage 1.
    candidate += 1;
    const candidateNum = candidate;
    console.log(`[candidat ${candidateNum}] ${msg.symbol || '(sans symbole)'} — ${msg.mint} — vérification bonding curve on-chain...`);

    fetchBondingCurveStateWithRetry(SOLANA_RPC_URL, msg.mint)
      .then((state) => {
        if (state.complete) {
          console.log(`  [candidat ${candidateNum}] écarté : compte déjà complete=true (migré/vidé avant même la vérification).\n`);
          return;
        }
        matched += 1;
        const url = `https://axiom.trade/t/${msg.mint}`;
        console.log(`[MATCH ${matched}] ${msg.symbol || '(sans symbole)'} — ${msg.mint}`);
        console.log(`  à la création (WS) : market_cap=${marketCap} SOL  achat_créateur=${creatorBuy} SOL  is_mayhem_mode=${mayhem === null ? 'n/a' : mayhem}`);
        console.log(`  on-chain (RPC, maintenant) : vSol=${state.virtual_quote_reserves_sol}  vToken=${state.virtual_token_reserves}  complete=${state.complete}`);
        console.log(`  -> ${url}${NO_OPEN ? '  (AXIOM_NO_OPEN=1 : pas ouvert)' : ''}\n`);
        if (!NO_OPEN) openInBrowser(url);
      })
      .catch((err) => {
        // Echec RPC (compte pas encore finalisé, endpoint public surchargé,
        // etc.) : informatif, pas bloquant — on ouvre quand même plutôt
        // que de perdre un candidat pour une raison purement réseau.
        matched += 1;
        const url = `https://axiom.trade/t/${msg.mint}`;
        console.log(`[MATCH ${matched}] ${msg.symbol || '(sans symbole)'} — ${msg.mint}`);
        console.log(`  à la création (WS) : market_cap=${marketCap} SOL  achat_créateur=${creatorBuy} SOL  is_mayhem_mode=${mayhem === null ? 'n/a' : mayhem}`);
        console.log(`  ATTENTION : vérification on-chain échouée (${err.message}) — ouvert quand même.`);
        console.log(`  -> ${url}${NO_OPEN ? '  (AXIOM_NO_OPEN=1 : pas ouvert)' : ''}\n`);
        if (!NO_OPEN) openInBrowser(url);
      });
  });

  ws.on('close', () => {
    console.log(`\nConnexion fermée. ${seen} création(s) vue(s), ${candidate} candidat(s) étage 1, ${matched} correspondance(s) finale(s).`);
  });

  ws.on('error', (err) => {
    console.error(`Erreur WS : ${err.message}`);
  });

  process.on('SIGINT', () => {
    console.log(`\n${seen} création(s) vue(s), ${candidate} candidat(s) étage 1, ${matched} correspondance(s) finale(s). Arrêt.`);
    ws.close();
    process.exit(0);
  });
}

main();

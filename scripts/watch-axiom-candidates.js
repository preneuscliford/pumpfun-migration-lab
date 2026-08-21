#!/usr/bin/env node
'use strict';

// Observation locale, ÉPHÉMÈRE : pas de Supabase, pas d'écriture nulle
// part, rien n'est stocké. Se connecte au flux public PumpPortal
// (subscribeNewToken uniquement — pas d'abonnement migration, on
// n'observe que la création), filtre les nouveaux tokens dont les
// features à la création tombent dans la plage P40-P60 du PROFIL DE
// RÉFÉRENCE GROUPE B (migrations progressives, calculé le 2026-08-21 —
// voir la section "PROFIL DE RÉFÉRENCE" de `node src/report.js`), puis
// VÉRIFIE que la bonding curve a réellement BOUGÉ on-chain (via
// src/bondingCurve.js, même code que le listener/le script de
// diagnostic) avant d'ouvrir la page Axiom du token — pas juste qu'elle
// existe encore, un vrai signe d'activité.
//
// Deuxième étage ajouté sur demande (2026-08-21) : le filtre sur les
// seules features WS de création laissait passer trop de tokens à la
// fois (~1 match/2-3s même en P40-P60). Le WS donne l'état ANNONCÉ à la
// création.
//
// Troisième étage ajouté juste après (même échange) : vérifier la
// bonding curve UNE FOIS immédiatement après la création ne sert à rien
// — à cet instant-là, RIEN n'a encore bougé, la quasi-totalité des
// tokens ont encore leurs réserves par défaut (voir le profil groupe B
// dans report.js : médiane à 0 dès 30s pour la moitié des migrations
// progressives, mais la plupart des tokens qui ne migrent jamais restent
// gelés à leurs valeurs de création pendant des minutes). Donc : on
// surveille la bonding curve à intervalles réguliers après la création,
// et on n'ouvre QUE quand elle s'écarte réellement de l'état annoncé à
// la création (ou qu'elle migre, ce qui est un mouvement encore plus
// net) — pas de mouvement détecté avant la limite de temps -> abandon
// silencieux, rien n'est ouvert.
//
// Local, pas en production : pas de file d'attente/throttle partagé
// comme dans le listener (300ms, pensé pour des milliers d'appels/jour
// sur un run continu) — ici le volume après filtre est déjà faible, donc
// chaque vérification part directement, jusqu'à la limite réelle du RPC
// public plutôt qu'une limite arbitraire choisie par prudence.
//
// Aucune décision automatique, aucun trading, aucun score : un filtre
// d'attention à plusieurs étages, rien de plus. C'est vous qui suivez le
// token ensuite.
//
// Filtre étage 1 (P40-P60 du profil groupe B, snapshot du 2026-08-21, à
// recalculer via `node --env-file=.env src/report.js` quand le profil
// aura significativement bougé avec plus de données) :
//   - market cap initial (SOL) : [31.79, 33.83]
//   - achat du créateur (SOL)  : [1.99, 3.00]
// Filtre étage 2 : la bonding curve doit s'écarter d'au moins
// MOVEMENT_THRESHOLD_SOL de l'état annoncé à la création (ou migrer),
// détecté par sondage périodique — voir waitForMovement(). Sinon,
// abandon silencieux après MOVEMENT_MAX_WAIT_MS.
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

function fmt(v, digits = 4) {
  return Number.isFinite(v) ? v.toFixed(digits) : 'n/a';
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

// Écart minimum (en SOL) par rapport à l'état annoncé à la création pour
// considérer que la bonding curve a réellement bougé — au-dessus du bruit
// de quelques lamports qu'on observe même sur des tokens totalement
// inactifs (voir report.js / inspect-trajectories.js).
const MOVEMENT_THRESHOLD_SOL = 0.05;
const MOVEMENT_POLL_INTERVAL_MS = 2000;
const MOVEMENT_MAX_WAIT_MS = 60000;

// Sonde la bonding curve toutes les MOVEMENT_POLL_INTERVAL_MS jusqu'à ce
// qu'elle s'écarte de baselineVSol d'au moins MOVEMENT_THRESHOLD_SOL, ou
// qu'elle migre (complete=true — mouvement maximal), ou jusqu'à
// MOVEMENT_MAX_WAIT_MS écoulées (abandon). Les erreurs RPC transitoires
// (compte pas encore visible, endpoint surchargé) sont avalées et
// réessayées au prochain sondage plutôt que de faire échouer tout de
// suite — normal juste après la création (commitment 'confirmed', voir
// src/bondingCurve.js).
async function waitForMovement(mint, baselineVSol) {
  const deadline = Date.now() + MOVEMENT_MAX_WAIT_MS;
  let lastState = null;
  while (Date.now() < deadline) {
    try {
      const state = await fetchBondingCurveState(SOLANA_RPC_URL, mint);
      lastState = state;
      if (state.complete) return { state, reason: 'complete' };
      if (Number.isFinite(baselineVSol) && Math.abs(state.virtual_quote_reserves_sol - baselineVSol) >= MOVEMENT_THRESHOLD_SOL) {
        return { state, reason: 'moved' };
      }
    } catch {
      // compte pas encore visible / erreur transitoire : on continue le sondage.
    }
    await sleep(MOVEMENT_POLL_INTERVAL_MS);
  }
  return { state: lastState, reason: 'timeout' };
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
    // surveiller la bonding curve jusqu'à ce qu'elle bouge vraiment avant
    // d'ouvrir quoi que ce soit. Pas de throttle partagé ici : on est en
    // local, volume déjà faible après l'étage 1.
    candidate += 1;
    const candidateNum = candidate;
    const baselineVSol = Number(msg.vSolInBondingCurve);
    console.log(
      `[candidat ${candidateNum}] ${msg.symbol || '(sans symbole)'} — ${msg.mint} — vSol création=${fmt(baselineVSol)} SOL, surveillance jusqu'à mouvement (max ${MOVEMENT_MAX_WAIT_MS / 1000}s)...`
    );

    waitForMovement(msg.mint, baselineVSol).then(({ state, reason }) => {
      if (reason === 'timeout') {
        console.log(`  [candidat ${candidateNum}] abandonné : aucun mouvement détecté en ${MOVEMENT_MAX_WAIT_MS / 1000}s.\n`);
        return;
      }
      matched += 1;
      const url = `https://axiom.trade/t/${msg.mint}`;
      const why = reason === 'complete' ? 'MIGRÉ pendant la surveillance' : `bougé de ${fmt(Math.abs(state.virtual_quote_reserves_sol - baselineVSol))} SOL`;
      console.log(`[MATCH ${matched}] ${msg.symbol || '(sans symbole)'} — ${msg.mint} — ${why}`);
      console.log(`  à la création (WS) : market_cap=${marketCap} SOL  achat_créateur=${creatorBuy} SOL  vSol=${fmt(baselineVSol)}  is_mayhem_mode=${mayhem === null ? 'n/a' : mayhem}`);
      console.log(`  on-chain (RPC, maintenant) : vSol=${state.virtual_quote_reserves_sol}  vToken=${state.virtual_token_reserves}  complete=${state.complete}`);
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

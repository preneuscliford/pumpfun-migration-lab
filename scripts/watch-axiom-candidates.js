#!/usr/bin/env node
'use strict';

// Observation locale : pas de Supabase, pas de collecte en continu comme
// le listener de production. Se connecte au flux public PumpPortal
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
// Stockage local ajouté sur demande (2026-08-21) : chaque token
// effectivement ouvert dans le navigateur (donc uniquement les MATCH,
// pas les candidats abandonnés) est ajouté à data/axiom-matches.jsonl —
// une ligne JSON par match, avec ses données au moment de l'ouverture
// (features WS de création + état on-chain qui a déclenché le match).
// Format JSONL (pas un tableau JSON classique) : chaque ligne s'ajoute
// indépendamment (fs.appendFileSync), donc un Ctrl+C ou un crash en
// cours de route ne corrompt jamais les lignes déjà écrites — un
// tableau JSON unique aurait exigé de relire/réécrire tout le fichier à
// chaque match. data/ est dans .gitignore : ce sont vos observations
// locales, pas des données du projet à publier sur le repo public.
//
// Suivi post-match ajouté sur retour terrain (2026-08-21, comparaison
// PumpWall vs Wrixel) : "ces tokens ont des fausses pump, parfois le dev
// sold et le token est mort, ou les top holders". Le mouvement détecté à
// l'étage 2 ne dit rien de si ça TIENT — un faux pump qui se dégonfle
// ressemble exactement à un vrai pump au moment du match. Donc après
// avoir ouvert le navigateur, on continue de sonder la bonding curve
// (toutes les POST_MATCH_POLL_INTERVAL_MS, pendant POST_MATCH_DURATION_MS)
// et on n'écrit la ligne JSONL qu'une fois cette fenêtre terminée, avec
// la trajectoire complète et un verdict descriptif (tenu / retombé /
// repli partiel / migré) — verdict fourni pour vous faire gagner du
// temps en le relisant, pas une décision automatique : c'est vous qui
// jugez en le regardant sur Axiom.
//
// Concentration des détenteurs ajoutée dans la foulée (même échange,
// "trop d'acheteurs... des wallets qui achètent dès le mouvement du
// token et détiennent une part significative, ça peut [ressembler à du]
// sniper" — préoccupation snipers/bots). Au moment du match, lit via
// src/holders.js qui détient réellement le token (getTokenLargestAccounts,
// compte de la bonding curve exclu) et quelle part de l'offre totale ça
// représente — descriptif, n'empêche jamais l'ouverture, juste ajouté aux
// logs et au JSONL (holders_at_match).
//
// Usage : node scripts/watch-axiom-candidates.js
// (Ctrl+C pour arrêter — tourne indéfiniment tant que vous le laissez.
// Les matchs dont le suivi post-match n'est pas terminé au moment du
// Ctrl+C ne seront pas écrits dans le JSONL.)

const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const { exec } = require('child_process');
const { fetchBondingCurveState, deriveBondingCurvePda } = require('../src/bondingCurve');
const { fetchHolderConcentration } = require('../src/holders');

const DATA_DIR = path.join(__dirname, '..', 'data');
const DATA_FILE = path.join(DATA_DIR, 'axiom-matches.jsonl');

function appendMatchRecord(record) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.appendFileSync(DATA_FILE, JSON.stringify(record) + '\n');
}

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
// considérer que la bonding curve a réellement bougé. Réglé à 5 SOL sur
// demande (2026-08-21) — bien au-dessus du bruit de quelques lamports
// observé même sur des tokens inactifs, ça vise une vraie traction, pas
// juste "quelqu'un a acheté un peu".
const MOVEMENT_THRESHOLD_SOL = 5;
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

// Fenêtre de suivi après un match (voir en-tête du fichier) : 3 minutes,
// un point toutes les 20s (9 points) — assez pour voir un retournement
// rapide sans faire traîner chaque candidat indéfiniment. Surchargeables
// par variable d'environnement pour tester le cycle complet rapidement.
const POST_MATCH_POLL_INTERVAL_MS = Number(process.env.POST_MATCH_POLL_INTERVAL_MS) || 20000;
const POST_MATCH_DURATION_MS = Number(process.env.POST_MATCH_DURATION_MS) || 180000;

async function trackPostMatch(mint, matchVSol) {
  const track = [];
  let peakVSol = matchVSol;
  let migratedDuringTracking = false;
  const start = Date.now();
  while (Date.now() - start < POST_MATCH_DURATION_MS) {
    await sleep(POST_MATCH_POLL_INTERVAL_MS);
    const t_s = Math.round((Date.now() - start) / 1000);
    try {
      const state = await fetchBondingCurveState(SOLANA_RPC_URL, mint);
      track.push({ t_s, virtual_sol_reserves: state.virtual_quote_reserves_sol, complete: state.complete });
      if (state.complete) {
        migratedDuringTracking = true;
        break;
      }
      if (state.virtual_quote_reserves_sol > peakVSol) peakVSol = state.virtual_quote_reserves_sol;
    } catch (err) {
      track.push({ t_s, error: err.message });
    }
  }
  return { track, peakVSol, migratedDuringTracking };
}

// Verdict DESCRIPTIF, pas une décision : sert à relire le fichier plus
// vite, le jugement reste le vôtre en regardant Axiom. "retombé" exige
// d'avoir rendu au moins 70% du gain observé au moment du match — un
// seuil de lecture, pas un seuil validé statistiquement (trop tôt, un
// seul cas comparé pour l'instant).
function computeVerdict({ baselineVSol, matchVSol, peakVSol, finalVSol, migratedDuringTracking }) {
  if (migratedDuringTracking) return 'migré pendant le suivi';
  if (finalVSol === null || finalVSol === undefined) return 'suivi incomplet (erreurs RPC)';
  const gainAtMatch = matchVSol - baselineVSol;
  const retracedFromPeak = peakVSol - finalVSol;
  if (gainAtMatch > 0 && retracedFromPeak >= gainAtMatch * 0.7) return 'retombé (probable faux pump)';
  if (finalVSol >= peakVSol * 0.9) return 'tenu';
  return 'repli partiel';
}

// Concentration des détenteurs au moment du match (voir src/holders.js) :
// beaucoup de wallets qui achètent dès le mouvement et détiennent une
// part significative du token peut indiquer des snipers/bots plutôt
// qu'un intérêt organique. Descriptif, pas un filtre — n'empêche pas
// l'ouverture, juste ajouté aux logs/au JSONL pour votre lecture.
// N'échoue jamais bruyamment : un problème RPC ici ne doit pas faire
// perdre le match lui-même.
async function fetchHolderSummary(mint, bondingCurvePda) {
  try {
    return await fetchHolderConcentration(SOLANA_RPC_URL, mint, bondingCurvePda);
  } catch (err) {
    return { error: err.message };
  }
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
    console.log(`Filtre actif : market_cap ∈ [${MARKET_CAP_SOL_RANGE.join(', ')}] SOL, achat_créateur ∈ [${CREATOR_BUY_SOL_RANGE.join(', ')}] SOL, mouvement ≥ ${MOVEMENT_THRESHOLD_SOL} SOL`);
    console.log(`Chaque match ouvert est ajouté à ${path.relative(process.cwd(), DATA_FILE)} (local, pas sur Supabase). Ctrl+C pour arrêter.\n`);
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
    const { pda: bondingCurvePda } = deriveBondingCurvePda(msg.mint);
    console.log(
      `[candidat ${candidateNum}] ${msg.symbol || '(sans symbole)'} — ${msg.mint} — vSol création=${fmt(baselineVSol)} SOL, surveillance jusqu'à mouvement (max ${MOVEMENT_MAX_WAIT_MS / 1000}s)...`
    );

    waitForMovement(msg.mint, baselineVSol).then(async ({ state, reason }) => {
      if (reason === 'timeout') {
        console.log(`  [candidat ${candidateNum}] abandonné : aucun mouvement détecté en ${MOVEMENT_MAX_WAIT_MS / 1000}s.\n`);
        return;
      }
      matched += 1;
      const url = `https://axiom.trade/t/${msg.mint}`;
      const movedSol = Math.abs(state.virtual_quote_reserves_sol - baselineVSol);
      const why = reason === 'complete' ? 'MIGRÉ pendant la surveillance' : `bougé de ${fmt(movedSol)} SOL`;
      console.log(`[MATCH ${matched}] ${msg.symbol || '(sans symbole)'} — ${msg.mint} — ${why}`);
      console.log(`  à la création (WS) : market_cap=${marketCap} SOL  achat_créateur=${creatorBuy} SOL  vSol=${fmt(baselineVSol)}  is_mayhem_mode=${mayhem === null ? 'n/a' : mayhem}`);
      console.log(`  on-chain (RPC, maintenant) : vSol=${state.virtual_quote_reserves_sol}  vToken=${state.virtual_token_reserves}  complete=${state.complete}`);

      const holders = await fetchHolderSummary(msg.mint, bondingCurvePda);
      if (holders.error) {
        console.log(`  détenteurs : lecture échouée (${holders.error})`);
      } else {
        console.log(
          `  détenteurs : ${holders.top_holders_count} wallet(s) hors curve détiennent ${fmt(holders.top_holders_pct_of_supply, 1)}% de l'offre totale`
        );
      }
      console.log(`  -> ${url}${NO_OPEN ? '  (AXIOM_NO_OPEN=1 : pas ouvert)' : ''}\n`);
      if (!NO_OPEN) openInBrowser(url);

      // L'ouverture est immédiate ; l'écriture JSONL attend la fin du
      // suivi post-match (voir en-tête du fichier) pour inclure la
      // trajectoire complète et un verdict.
      if (reason === 'complete') {
        appendMatchRecord({
          matched_at: new Date().toISOString(),
          mint: msg.mint,
          symbol: msg.symbol || null,
          name: msg.name || null,
          creator: msg.traderPublicKey ?? msg.creator ?? null,
          axiom_url: url,
          reason,
          movement_sol: movedSol,
          creation: {
            market_cap_sol: marketCap,
            creator_buy_sol: creatorBuy,
            virtual_sol_reserves: baselineVSol,
            virtual_token_reserves: Number(msg.vTokensInBondingCurve) || null,
            is_mayhem_mode: mayhem,
            metadata_uri: msg.uri || null,
          },
          at_match: { virtual_sol_reserves: state.virtual_quote_reserves_sol, virtual_token_reserves: state.virtual_token_reserves, complete: true },
          holders_at_match: holders,
          post_match_track: [],
          verdict: 'migré au moment du match',
          raw_new_token_event: msg,
        });
        return;
      }

      console.log(`  [candidat ${candidateNum}] suivi post-match en cours (${POST_MATCH_DURATION_MS / 1000}s, ${POST_MATCH_POLL_INTERVAL_MS / 1000}s/point)...`);
      trackPostMatch(msg.mint, state.virtual_quote_reserves_sol).then(({ track, peakVSol, migratedDuringTracking }) => {
        const lastPoint = [...track].reverse().find((p) => typeof p.virtual_sol_reserves === 'number');
        const finalVSol = lastPoint ? lastPoint.virtual_sol_reserves : null;
        const verdict = computeVerdict({ baselineVSol, matchVSol: state.virtual_quote_reserves_sol, peakVSol, finalVSol, migratedDuringTracking });
        console.log(`  [candidat ${candidateNum}] suivi terminé — verdict : ${verdict} (finalVSol=${fmt(finalVSol)}, peak=${fmt(peakVSol)})\n`);

        appendMatchRecord({
          matched_at: new Date().toISOString(),
          mint: msg.mint,
          symbol: msg.symbol || null,
          name: msg.name || null,
          creator: msg.traderPublicKey ?? msg.creator ?? null,
          axiom_url: url,
          reason,
          movement_sol: movedSol,
          creation: {
            market_cap_sol: marketCap,
            creator_buy_sol: creatorBuy,
            virtual_sol_reserves: baselineVSol,
            virtual_token_reserves: Number(msg.vTokensInBondingCurve) || null,
            is_mayhem_mode: mayhem,
            metadata_uri: msg.uri || null,
          },
          at_match: {
            virtual_sol_reserves: state.virtual_quote_reserves_sol,
            virtual_token_reserves: state.virtual_token_reserves,
            complete: state.complete,
          },
          holders_at_match: holders,
          post_match_track: track,
          verdict,
          raw_new_token_event: msg,
        });
      });
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

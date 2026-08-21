'use strict';

// Concentration des détenteurs d'un token — sert à distinguer un
// mouvement de prix porté par beaucoup de petits acheteurs (organique)
// d'un mouvement porté par une poignée de wallets qui accaparent une
// grosse part de l'offre (signature typique d'un sniper/bot, ou d'une
// équipe qui prépare un dump). Demandé le 2026-08-21 en complément du
// suivi post-match (bondingCurve.js) : la bonding curve dit SI le prix
// bouge, ceci dit QUI l'a fait bouger.
//
// getTokenLargestAccounts renvoie jusqu'à 20 COMPTES token (pas des
// wallets) triés par solde — il faut résoudre le propriétaire de chacun
// (getAccountInfo en jsonParsed) et exclure le compte de la bonding
// curve elle-même (elle détient les tokens pas encore vendus, ce n'est
// pas un "acheteur").

const { PublicKey } = require('@solana/web3.js');
const { rpcCall } = require('./bondingCurve');

// Constantes standard de l'écosystème Solana (SPL Token / Associated
// Token Account), pas propres à pump.fun.
const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const ASSOCIATED_TOKEN_PROGRAM_ID = new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL');

// Découvert le 2026-08-21 en testant sur Helius : certains mints pump.fun
// utilisent Token-2022 (extension metadataPointer visible dans
// getAccountInfo), pas le programme SPL Token classique. Dériver l'ATA de
// la bonding curve avec le mauvais programme donne une adresse qui ne
// correspondra jamais au vrai compte de la curve — il faut lire le
// programme propriétaire RÉEL du mint (fetchMintTokenProgram ci-dessous)
// plutôt que de le supposer fixe.

function deriveAta(owner, mint, tokenProgramId) {
  const [ata] = PublicKey.findProgramAddressSync(
    [owner.toBuffer(), tokenProgramId.toBuffer(), mint.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  return ata;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Lit le programme propriétaire réel du mint (Token classique vs
// Token-2022) — nécessaire pour dériver correctement l'ATA de la curve.
async function fetchMintTokenProgram(rpcUrl, mint) {
  const info = await rpcCall(rpcUrl, 'getAccountInfo', [mint, { encoding: 'jsonParsed', commitment: 'confirmed' }]);
  const owner = info?.value?.owner;
  if (!owner) throw new Error(`mint introuvable : ${mint}`);
  return new PublicKey(owner);
}

// bondingCurvePda : la PDA déjà dérivée par deriveBondingCurvePda() côté
// appelant (évite de la recalculer ici, l'appelant l'a généralement déjà
// sous la main).
async function fetchHolderConcentration(rpcUrl, mint, bondingCurvePda) {
  const mintPubkey = new PublicKey(mint);
  const tokenProgramId = await fetchMintTokenProgram(rpcUrl, mint);
  const curveAta = deriveAta(bondingCurvePda, mintPubkey, tokenProgramId).toBase58();

  // Retry avec backoff sur ces deux appels seulement : constaté le
  // 2026-08-21 qu'un mint tout juste créé (appelé au moment du match, donc
  // parfois quelques secondes seulement après la création) peut renvoyer
  // "Invalid param: not a Token mint" sur getTokenLargestAccounts — même
  // quand getAccountInfo voit déjà le mint (voir fetchMintTokenProgram
  // ci-dessus, appelé juste avant sans erreur). L'indexeur spécifique à
  // getTokenLargestAccounts semble accuser plus de retard que la lecture
  // de compte brute ; un premier essai à 500ms n'a pas suffi en test réel,
  // d'où un backoff plus généreux (jusqu'à ~15s cumulés). Sans risque pour
  // la réactivité de l'outil : l'ouverture du navigateur ne dépend plus de
  // ce résultat (voir watch-axiom-candidates.js).
  let largest, supplyInfo;
  const delaysMs = [500, 1000, 2000, 4000, 8000];
  for (let attempt = 0; ; attempt += 1) {
    try {
      [largest, supplyInfo] = await Promise.all([
        rpcCall(rpcUrl, 'getTokenLargestAccounts', [mint, { commitment: 'confirmed' }]),
        rpcCall(rpcUrl, 'getTokenSupply', [mint, { commitment: 'confirmed' }]),
      ]);
      break;
    } catch (err) {
      if (attempt >= delaysMs.length) throw err;
      await sleep(delaysMs[attempt]);
    }
  }

  const totalSupply = Number(supplyInfo.value.amount);
  const accounts = largest.value || [];
  const curveAccount = accounts.find((a) => a.address === curveAta);

  // Résout le propriétaire de chaque compte (sauf celui de la curve,
  // déjà identifié) — SÉRIALISÉ, pas en parallèle : jusqu'à ~19 appels
  // getAccountInfo d'un coup déclenchait un HTTP 429 sur le RPC public
  // gratuit, même sérialisé à 150ms d'écart (constaté en test le
  // 2026-08-21, avec un autre process tournant en parallèle sur la même
  // IP). 500ms par prudence — cette fonction n'est appelée qu'aux
  // matchs (déjà rares après les étages 1/2), pas en boucle serrée.
  const resolved = [];
  for (const a of accounts) {
    if (a.address === curveAta) continue;
    try {
      const info = await rpcCall(rpcUrl, 'getAccountInfo', [a.address, { encoding: 'jsonParsed', commitment: 'confirmed' }]);
      const owner = info?.value?.data?.parsed?.info?.owner ?? null;
      resolved.push({ address: a.address, owner, amount: Number(a.amount) });
    } catch {
      resolved.push({ address: a.address, owner: null, amount: Number(a.amount) });
    }
    await new Promise((r) => setTimeout(r, 500));
  }

  const topHeldAmount = resolved.reduce((sum, a) => sum + a.amount, 0);

  return {
    total_supply: totalSupply,
    curve_held_amount: curveAccount ? Number(curveAccount.amount) : null,
    top_holders_count: resolved.length,
    top_holders_amount: topHeldAmount,
    top_holders_pct_of_supply: totalSupply ? (topHeldAmount / totalSupply) * 100 : null,
    top_holders: resolved
      .sort((a, b) => b.amount - a.amount)
      .map((h) => ({ owner: h.owner, amount: h.amount, pct: totalSupply ? (h.amount / totalSupply) * 100 : null })),
  };
}

module.exports = { deriveAta, fetchHolderConcentration, TOKEN_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID };

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

function deriveAta(owner, mint) {
  const [ata] = PublicKey.findProgramAddressSync(
    [owner.toBuffer(), TOKEN_PROGRAM_ID.toBuffer(), mint.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM_ID
  );
  return ata;
}

// bondingCurvePda : la PDA déjà dérivée par deriveBondingCurvePda() côté
// appelant (évite de la recalculer ici, l'appelant l'a généralement déjà
// sous la main).
async function fetchHolderConcentration(rpcUrl, mint, bondingCurvePda) {
  const mintPubkey = new PublicKey(mint);
  const curveAta = deriveAta(bondingCurvePda, mintPubkey).toBase58();

  const [largest, supplyInfo] = await Promise.all([
    rpcCall(rpcUrl, 'getTokenLargestAccounts', [mint, { commitment: 'confirmed' }]),
    rpcCall(rpcUrl, 'getTokenSupply', [mint, { commitment: 'confirmed' }]),
  ]);

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

'use strict';

// Lecture de l'état de la bonding curve pump.fun via RPC Solana public.
// Logique partagée entre le listener (snapshots programmés à intervalles
// fixes après la création) et scripts/check-bonding-curve-rpc.js (script
// de diagnostic isolé qui a servi à valider ce layout sur un token réel).
//
// Layout du compte (151 octets), validé le 2026-08-21 sur un token migré
// réel (voir scripts/check-bonding-curve-rpc.js pour le détail de la
// validation : is_mayhem_mode décodé exactement égal à la valeur connue
// à la création, complete=true cohérent, quote_mint = sentinelle SOL
// natif, token_total_supply cohérent avec les 6 décimales pump.fun) :
//   0-7    discriminator Anchor (8 octets)
//   8-15   virtual_token_reserves (u64)
//   16-23  virtual_quote_reserves (u64) — réserve SOL virtuelle pour un
//          pool coté en SOL
//   24-31  real_token_reserves (u64)
//   32-39  real_quote_reserves (u64)
//   40-47  token_total_supply (u64)
//   48     complete (1 octet, bool)
//   49-80  creator (32 octets, pubkey)
//   81     is_mayhem_mode (1 octet, bool)
//   82     is_cashback_coin (1 octet, bool)
//   83-114 quote_mint (32 octets, pubkey)
//   115-150 padding / réservé (36 octets)

const { PublicKey } = require('@solana/web3.js');

const PUMP_FUN_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const EXPECTED_LAYOUT_BYTES = 151;

function deriveBondingCurvePda(mint) {
  const mintPubkey = new PublicKey(mint);
  const [pda, bump] = PublicKey.findProgramAddressSync(
    [Buffer.from('bonding-curve'), mintPubkey.toBuffer()],
    PUMP_FUN_PROGRAM_ID
  );
  return { pda, bump };
}

// Retourne un objet {error} plutôt que de planter si la taille ne
// correspond pas — c'est en soi un résultat exploitable (compte
// différent de ce qu'on attend) plutôt qu'un crash.
function decodeBondingCurve(base64Data) {
  const buf = Buffer.from(base64Data, 'base64');
  if (buf.length < EXPECTED_LAYOUT_BYTES) {
    return { error: `taille inattendue: ${buf.length} octets (attendu ${EXPECTED_LAYOUT_BYTES})`, rawByteLength: buf.length };
  }
  let offset = 8; // discriminator Anchor
  const readU64 = () => {
    const v = buf.readBigUInt64LE(offset);
    offset += 8;
    return v;
  };
  const readPubkey = () => {
    const v = new PublicKey(buf.subarray(offset, offset + 32)).toBase58();
    offset += 32;
    return v;
  };
  const virtualTokenReserves = readU64();
  const virtualQuoteReserves = readU64();
  const realTokenReserves = readU64();
  const realQuoteReserves = readU64();
  const tokenTotalSupply = readU64();
  const complete = buf.readUInt8(offset) === 1;
  offset += 1;
  const creator = readPubkey();
  const isMayhemMode = buf.readUInt8(offset) === 1;
  offset += 1;
  const isCashbackCoin = buf.readUInt8(offset) === 1;
  offset += 1;
  const quoteMint = readPubkey();
  return {
    virtual_token_reserves: virtualTokenReserves.toString(),
    virtual_quote_reserves: virtualQuoteReserves.toString(),
    virtual_quote_reserves_sol: Number(virtualQuoteReserves) / 1e9,
    real_token_reserves: realTokenReserves.toString(),
    real_quote_reserves: realQuoteReserves.toString(),
    real_quote_reserves_sol: Number(realQuoteReserves) / 1e9,
    token_total_supply: tokenTotalSupply.toString(),
    complete,
    creator,
    is_mayhem_mode: isMayhemMode,
    is_cashback_coin: isCashbackCoin,
    quote_mint: quoteMint,
    rawByteLength: buf.length,
  };
}

async function rpcCall(rpcUrl, method, params) {
  const res = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  if (json.error) throw new Error(`RPC ${method}: ${JSON.stringify(json.error)}`);
  return json.result;
}

// Lit et décode l'état ACTUEL (pas l'historique) de la bonding curve d'un
// mint. Lève une erreur si le compte est introuvable ou si le décodage
// échoue — à l'appelant de décider quoi en faire.
async function fetchBondingCurveState(rpcUrl, mint) {
  const { pda } = deriveBondingCurvePda(mint);
  const accountInfo = await rpcCall(rpcUrl, 'getAccountInfo', [pda.toBase58(), { encoding: 'base64' }]);
  if (!accountInfo || !accountInfo.value) {
    throw new Error(`compte bonding curve introuvable pour ${mint} (pda ${pda.toBase58()})`);
  }
  const decoded = decodeBondingCurve(accountInfo.value.data[0]);
  if (decoded.error) throw new Error(`décodage échoué pour ${mint}: ${decoded.error}`);
  return decoded;
}

module.exports = {
  PUMP_FUN_PROGRAM_ID,
  deriveBondingCurvePda,
  decodeBondingCurve,
  rpcCall,
  fetchBondingCurveState,
};

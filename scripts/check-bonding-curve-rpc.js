#!/usr/bin/env node
'use strict';

// Test isolé, demandé avant toute décision sur l'option B : est-il
// techniquement possible de retrouver l'état de la bonding curve d'un
// token pump.fun via le RPC Solana public GRATUIT, SANS passer par
// PumpPortal subscribeTokenTrade ?
//
// Ne touche NI au listener NI au schéma — script de diagnostic autonome
// à part entière, lecture seule sur Supabase (pour choisir un token réel
// déjà observé) et sur le RPC Solana public. N'écrit rien nulle part.
//
// Layout du compte (151 octets), confirmé via recherche (IDL public de
// pump.fun + SDKs communautaires) et vérifié ci-dessous sur un token réel :
//   0-7    discriminator Anchor (8 octets)
//   8-15   virtual_token_reserves (u64)
//   16-23  virtual_quote_reserves (u64) — = réserve SOL virtuelle pour un
//          pool coté en SOL, d'où le renommage depuis "virtualSolReserves"
//   24-31  real_token_reserves (u64)
//   32-39  real_quote_reserves (u64) — idem, = réserve SOL réelle en SOL
//   40-47  token_total_supply (u64)
//   48     complete (1 octet, bool)
//   49-80  creator (32 octets, pubkey)
//   81     is_mayhem_mode (1 octet, bool)
//   82     is_cashback_coin (1 octet, bool)
//   83-114 quote_mint (32 octets, pubkey)
//   115-150 padding / réservé (36 octets)
// Program ID pump.fun : 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
// PDA de la bonding curve dérivée des seeds ["bonding-curve", mint] — déjà
// confirmée correcte (owner du compte retrouvé = ce program ID).
// Si le décodage produit des valeurs incohérentes (ex. très éloignées de
// initial_virtual_sol_reserves connu à la création, ou is_mayhem_mode qui
// ne correspond pas à raw_new_token_event), le layout reste à revoir —
// le test le montrera, pas la peine de deviner.
//
// Limite méthodologique importante : getAccountInfo ne donne que l'état
// ACTUEL du compte. Pour un token déjà migré, c'est son état FINAL
// (complete=true attendu), pas sa trajectoire T+30s/T+1min/etc. Pour
// reconstruire la trajectoire passée sans PumpPortal, il faut soit un
// noeud RPC archive (payant), soit reconstruire à partir de l'historique
// de transactions du compte (getSignaturesForAddress + getTransaction) —
// c'est ce que teste la partie 2/3 de ce script.
//
// Usage : node scripts/check-bonding-curve-rpc.js

const { PublicKey } = require('@solana/web3.js');
const { createClient } = require('@supabase/supabase-js');

const RPC_URL = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
const PUMP_FUN_PROGRAM_ID = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function rpcCall(method, params) {
  const res = await fetch(RPC_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  if (json.error) throw new Error(`RPC ${method}: ${JSON.stringify(json.error)}`);
  return json.result;
}

// Décode le layout confirmé (151 octets, voir en-tête du fichier). Retourne
// un objet {error} plutôt que de planter si la taille ne correspond pas —
// c'est en soi un résultat de test (layout encore faux ou compte différent).
const EXPECTED_LAYOUT_BYTES = 151;

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

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  // Un token migré PROGRESSIF (>10s, groupe B — pas le groupe A exclu de
  // la population cible) déjà observé. On prend celui qui a mis le plus
  // longtemps à migrer, pour avoir le plus d'historique de transactions
  // à inspecter dans le test 2/3.
  const { data: candidates, error } = await supabase
    .from('tokens')
    .select('mint, symbol, created_at, migrated_at, time_to_migration_seconds, initial_virtual_sol_reserves, raw_new_token_event')
    .eq('migrated', true)
    .gt('time_to_migration_seconds', 10)
    .order('time_to_migration_seconds', { ascending: false })
    .limit(1);
  if (error) throw new Error(`lecture tokens: ${error.message}`);
  if (!candidates.length) {
    console.log('Aucun token migré progressif (>10s) trouvé pour le moment — relancer ce test plus tard.');
    return;
  }
  const token = candidates[0];

  const rawIsMayhemMode =
    token.raw_new_token_event && typeof token.raw_new_token_event.is_mayhem_mode === 'boolean'
      ? token.raw_new_token_event.is_mayhem_mode
      : null;

  console.log('='.repeat(72));
  console.log(`Token choisi (groupe B, le plus lent observé) : ${token.symbol || '(sans symbole)'} — ${token.mint}`);
  console.log(`  créé ${token.created_at}, migré ${token.migrated_at} (délai ${token.time_to_migration_seconds}s)`);
  console.log(`  initial_virtual_sol_reserves connu (à la création, via PumpPortal) : ${token.initial_virtual_sol_reserves}`);
  console.log(`  is_mayhem_mode connu (à la création, via raw_new_token_event) : ${rawIsMayhemMode === null ? 'n/a' : rawIsMayhemMode}`);
  console.log('='.repeat(72));

  const mintPubkey = new PublicKey(token.mint);
  const [bondingCurvePda, bump] = PublicKey.findProgramAddressSync(
    [Buffer.from('bonding-curve'), mintPubkey.toBuffer()],
    PUMP_FUN_PROGRAM_ID
  );
  console.log(`\nPDA bonding curve dérivée (calcul local, pas d'appel réseau) : ${bondingCurvePda.toBase58()} (bump ${bump})`);

  console.log("\n--- Test 1/3 : getAccountInfo (état ACTUEL du compte, PAS l'historique) ---");
  try {
    const accountInfo = await rpcCall('getAccountInfo', [bondingCurvePda.toBase58(), { encoding: 'base64' }]);
    if (!accountInfo || !accountInfo.value) {
      console.log('  Compte introuvable — soit la PDA dérivée est fausse, soit le compte a été fermé après migration.');
    } else {
      console.log(`  Compte trouvé. owner=${accountInfo.value.owner}`);
      const decoded = decodeBondingCurve(accountInfo.value.data[0]);
      console.log('  Décodé (layout 151 octets, voir en-tête du fichier) :');
      console.log('   ', JSON.stringify(decoded));
      if (decoded.error) {
        console.log('  -> ECHEC DE DECODAGE : layout encore faux, à revoir avant d\'aller plus loin.');
      } else {
        console.log(decoded.complete === true
          ? '  -> complete=true, cohérent avec un token déjà migré : bon signe pour le layout.'
          : '  -> complete=false alors que le token est marqué migré côté PumpPortal : incohérence à investiguer.');

        if (rawIsMayhemMode !== null) {
          console.log(decoded.is_mayhem_mode === rawIsMayhemMode
            ? `  -> is_mayhem_mode cohérent : ${decoded.is_mayhem_mode} (compte on-chain) === ${rawIsMayhemMode} (vu à la création).`
            : `  -> INCOHÉRENCE is_mayhem_mode : ${decoded.is_mayhem_mode} (compte on-chain) !== ${rawIsMayhemMode} (vu à la création).`);
        } else {
          console.log('  -> is_mayhem_mode non disponible dans raw_new_token_event pour ce token, comparaison impossible.');
        }

        if (token.initial_virtual_sol_reserves != null) {
          const initialSol = Number(token.initial_virtual_sol_reserves) / 1e9;
          console.log(`  -> virtual_quote_reserves_sol actuel = ${decoded.virtual_quote_reserves_sol} SOL vs initial_virtual_sol_reserves à la création = ${initialSol} SOL (écart attendu : la réserve évolue avec les échanges, mais doit rester du même ordre de grandeur).`);
        }

        const supplyPer1e6 = Number(BigInt(decoded.token_total_supply) / 1000000n);
        console.log(`  -> token_total_supply = ${decoded.token_total_supply} base units, soit ${supplyPer1e6.toLocaleString()} tokens à 6 décimales (attendu ~1 000 000 000 pour un supply pump.fun standard).`);
      }
    }
  } catch (err) {
    console.log(`  ECHEC : ${err.message}`);
  }

  console.log('\n--- Test 2/3 : getSignaturesForAddress (historique de transactions du compte) ---');
  let signatures = [];
  try {
    signatures = await rpcCall('getSignaturesForAddress', [bondingCurvePda.toBase58(), { limit: 50 }]);
    console.log(`  ${signatures.length} signature(s) trouvée(s) (limite demandée 50).`);
    if (signatures.length) {
      const newest = signatures[0];
      const oldest = signatures[signatures.length - 1];
      console.log(`  Plus récente : ${newest.signature.slice(0, 20)}... (slot ${newest.slot}, ${newest.blockTime ? new Date(newest.blockTime * 1000).toISOString() : 'blockTime n/a'})`);
      console.log(`  Plus ancienne (parmi les ${signatures.length}) : ${oldest.signature.slice(0, 20)}... (slot ${oldest.slot}, ${oldest.blockTime ? new Date(oldest.blockTime * 1000).toISOString() : 'blockTime n/a'})`);
      console.log('  -> Si ces horodatages couvrent bien la fenêtre création->migration du token, la reconstruction historique via transactions est envisageable.');
    }
  } catch (err) {
    console.log(`  ECHEC : ${err.message}`);
  }

  console.log('\n--- Test 3/3 : getTransaction sur la signature la plus ancienne (reconstruction historique) ---');
  if (signatures.length) {
    try {
      const oldestSig = signatures[signatures.length - 1].signature;
      const tx = await rpcCall('getTransaction', [oldestSig, { encoding: 'json', maxSupportedTransactionVersion: 0 }]);
      if (!tx) {
        console.log('  Transaction introuvable (peut-être élaguée par ce noeud RPC public gratuit).');
      } else {
        const logs = (tx.meta && tx.meta.logMessages) || [];
        console.log(`  Transaction trouvée, ${logs.length} ligne(s) de log au total.`);
        const relevantLogs = logs.filter((l) => /program log|pump/i.test(l));
        console.log(`  Lignes contenant "Program log" ou "pump" (${relevantLogs.length}) :`);
        for (const l of relevantLogs.slice(0, 12)) console.log(`    ${l}`);
        if (relevantLogs.length) {
          console.log('  -> Des logs exploitables existent : reconstruire la trajectoire depuis les transactions est plausible, à approfondir plus tard si besoin.');
        } else {
          console.log('  -> Aucun log directement exploitable trouvé dans cet extrait — pas une preuve que c\'est impossible, juste que ça demande plus de travail de parsing.');
        }
      }
    } catch (err) {
      console.log(`  ECHEC : ${err.message}`);
    }
  } else {
    console.log('  (aucune signature à inspecter — test 2/3 a échoué ou compte vide)');
  }

  console.log('\n' + '='.repeat(72));
  console.log("Fin du test. Verdict à tirer manuellement de ce qui a fonctionné/échoué ci-dessus — pas de décision automatique ici.");
}

main().catch((err) => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});

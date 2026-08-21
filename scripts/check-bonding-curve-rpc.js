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
// Hypothèses NON vérifiées avant ce test (c'est justement lui qui doit
// les confirmer ou les infirmer) :
// - Program ID pump.fun : 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
// - PDA de la bonding curve dérivée des seeds ["bonding-curve", mint]
// - Layout du compte : discriminator(8) + virtualTokenReserves(u64) +
//   virtualSolReserves(u64) + realTokenReserves(u64) + realSolReserves(u64)
//   + tokenTotalSupply(u64) + complete(bool)
// Si le décodage produit des valeurs incohérentes (ex. très éloignées de
// initial_virtual_sol_reserves connu à la création), une de ces
// hypothèses est fausse — le test le montrera, pas la peine de deviner.
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

// Décode le layout supposé (voir hypothèses en tête de fichier). Retourne
// un objet {error} plutôt que de planter si la taille ne correspond pas —
// c'est en soi un résultat de test (hypothèse de layout fausse).
function decodeBondingCurve(base64Data) {
  const buf = Buffer.from(base64Data, 'base64');
  const expectedMin = 8 + 8 * 5 + 1;
  if (buf.length < expectedMin) {
    return { error: `taille inattendue: ${buf.length} octets (attendu au moins ${expectedMin})`, rawByteLength: buf.length };
  }
  let offset = 8; // discriminator Anchor supposé
  const readU64 = () => {
    const v = buf.readBigUInt64LE(offset);
    offset += 8;
    return v;
  };
  const virtualTokenReserves = readU64();
  const virtualSolReserves = readU64();
  const realTokenReserves = readU64();
  const realSolReserves = readU64();
  const tokenTotalSupply = readU64();
  const complete = buf.readUInt8(offset) === 1;
  return {
    virtualTokenReserves: virtualTokenReserves.toString(),
    virtualSolReservesSol: Number(virtualSolReserves) / 1e9,
    realTokenReserves: realTokenReserves.toString(),
    realSolReservesSol: Number(realSolReserves) / 1e9,
    tokenTotalSupply: tokenTotalSupply.toString(),
    complete,
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
    .select('mint, symbol, created_at, migrated_at, time_to_migration_seconds, initial_virtual_sol_reserves')
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

  console.log('='.repeat(72));
  console.log(`Token choisi (groupe B, le plus lent observé) : ${token.symbol || '(sans symbole)'} — ${token.mint}`);
  console.log(`  créé ${token.created_at}, migré ${token.migrated_at} (délai ${token.time_to_migration_seconds}s)`);
  console.log(`  initial_virtual_sol_reserves connu (à la création, via PumpPortal) : ${token.initial_virtual_sol_reserves}`);
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
      console.log('  Décodé (layout supposé, voir en-tête du fichier) :');
      console.log('   ', JSON.stringify(decoded));
      if (decoded.complete === true) {
        console.log('  -> complete=true, cohérent avec un token déjà migré : bon signe pour le layout supposé.');
      } else if (decoded.error) {
        console.log('  -> ECHEC DE DECODAGE : layout probablement faux, à revoir avant d\'aller plus loin.');
      } else {
        console.log('  -> complete=false alors que le token est marqué migré côté PumpPortal : incohérence à investiguer (layout ou PDA probablement faux).');
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

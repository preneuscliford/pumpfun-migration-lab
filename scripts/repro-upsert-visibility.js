#!/usr/bin/env node
'use strict';

// Repro isolé (écrit une seule ligne de test, lecture seule ensuite) pour
// la panne du 2026-08-26 : upsertNewTokens() ne renvoie pas d'erreur mais
// la ligne semble absente de `tokens` quand insertSnapshot() la cherche
// quelques secondes plus tard (violation FK à 100% depuis le reset).
//
// Ce script reproduit EXACTEMENT le even chemin utilisé par le listener :
// upsert(rows, {onConflict:'mint'}) suivi d'une lecture immédiate, pour
// voir si le problème est répétable en dehors du listener (élimine toute
// hypothèse liée à la logique de cascade/scheduling).
//
// Écrit UNE seule ligne de test (mint synthétique, préfixé TESTPROBE_,
// facile à identifier et à nettoyer), puis la supprime à la fin. Usage :
// node scripts/repro-upsert-visibility.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  const testMint = `TESTPROBE_${Date.now()}`;
  const row = {
    mint: testMint,
    symbol: 'TEST',
    name: 'repro-upsert-visibility',
    created_at: new Date().toISOString(),
    initial_virtual_sol_reserves: 30,
    initial_virtual_token_reserves: 1e9,
    raw_new_token_event: { test: true },
  };

  console.log(`Mint de test : ${testMint}`);

  console.log('\n1) Upsert...');
  const t0 = Date.now();
  const { error: upsertError, status: upsertStatus, statusText: upsertStatusText } = await supabase
    .from('tokens')
    .upsert([row], { onConflict: 'mint' });
  console.log(`   status=${upsertStatus} ${upsertStatusText}  error=${upsertError ? upsertError.message : 'aucune'}  (${Date.now() - t0}ms)`);

  console.log('\n2) Lecture immédiate (0ms d\'attente)...');
  const { data: read0, error: read0Error } = await supabase.from('tokens').select('mint, created_at').eq('mint', testMint).maybeSingle();
  console.log(`   trouvé=${!!read0}  error=${read0Error ? read0Error.message : 'aucune'}`);

  console.log('\n3) Lecture après 1s...');
  await new Promise((r) => setTimeout(r, 1000));
  const { data: read1, error: read1Error } = await supabase.from('tokens').select('mint, created_at').eq('mint', testMint).maybeSingle();
  console.log(`   trouvé=${!!read1}  error=${read1Error ? read1Error.message : 'aucune'}`);

  console.log('\n4) Tentative d\'insertion d\'un token_snapshot pour ce mint (comme insertSnapshot le fait)...');
  const { error: snapError } = await supabase.from('token_snapshots').insert({
    mint: testMint,
    age_seconds: 2,
    nominal_delay_s: 2,
    virtual_sol_reserves: 30,
    virtual_token_reserves: 1e15,
    raw_event: {},
  });
  console.log(`   insertSnapshot ${snapError ? `ÉCHEC: ${snapError.message}` : 'RÉUSSI'}`);

  console.log('\n5) Nettoyage (suppression de la ligne de test)...');
  await supabase.from('token_snapshots').delete().eq('mint', testMint);
  const { error: deleteError } = await supabase.from('tokens').delete().eq('mint', testMint);
  console.log(`   suppression ${deleteError ? `échec: ${deleteError.message}` : 'ok'}`);
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

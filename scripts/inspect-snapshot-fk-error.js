#!/usr/bin/env node
'use strict';

// Diagnostic ponctuel (lecture seule) : après le reset du 2026-08-26,
// insertSnapshot échoue à 100% avec une violation de clé étrangère
// token_snapshots_mint_fkey, y compris pour des tokens vieux de 2s.
// Objectif : vérifier si le mint concerné existe réellement dans
// `tokens` au moment de l'inspection, pour distinguer "le mint n'a
// jamais été inséré" de "course lecture-après-écriture".
//
// Usage : MINT=<mint> node scripts/inspect-snapshot-fk-error.js
// (ou sans MINT : prend le mint du dernier bonding_curve_snapshot_error)

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  let mint = process.env.MINT;
  if (!mint) {
    const { data: logs, error: logErr } = await supabase
      .from('ingestion_log')
      .select('detail, at')
      .eq('event_type', 'bonding_curve_snapshot_error')
      .order('at', { ascending: false })
      .limit(1);
    if (logErr) throw new Error(`lecture ingestion_log: ${logErr.message}`);
    if (!logs || !logs.length) {
      console.log('Aucune erreur bonding_curve_snapshot_error trouvée.');
      return;
    }
    const m = logs[0].detail.match(/^(\S+) @/);
    mint = m ? m[1] : null;
    console.log(`Mint extrait de la dernière erreur (${logs[0].at}) : ${mint}`);
  }
  if (!mint) throw new Error('Impossible de déterminer un mint à inspecter.');

  const { data: tokenRow, error: tokenErr } = await supabase
    .from('tokens')
    .select('mint, created_at, raw_new_token_event')
    .eq('mint', mint)
    .maybeSingle();
  if (tokenErr) throw new Error(`lecture tokens: ${tokenErr.message}`);

  console.log(`\nExiste dans tokens ? ${tokenRow ? 'OUI' : 'NON'}`);
  if (tokenRow) {
    console.log(`  created_at: ${tokenRow.created_at}`);
    console.log(`  raw_new_token_event présent : ${!!tokenRow.raw_new_token_event}`);
  }

  // Toutes les erreurs récentes pour ce mint précis, pour voir combien de
  // tentatives ont été faites et sur quelle plage de temps.
  const { data: allErrorsForMint, error: allErrErr } = await supabase
    .from('ingestion_log')
    .select('at, detail')
    .eq('event_type', 'bonding_curve_snapshot_error')
    .ilike('detail', `${mint}%`)
    .order('at', { ascending: true });
  if (allErrErr) throw new Error(`lecture ingestion_log (filtré): ${allErrErr.message}`);
  console.log(`\nNombre d'erreurs enregistrées pour ce mint : ${(allErrorsForMint || []).length}`);
  for (const e of (allErrorsForMint || []).slice(0, 10)) console.log(`  [${e.at}] ${e.detail}`);

  // Vérification directe token_snapshots (2026-08-27) : requête minimale,
  // sans passer par fetchSnapshotsForMints/.in(), pour écarter un bug côté
  // requête des scripts de simulation.
  const { data: directSnapshots, error: directErr } = await supabase
    .from('token_snapshots')
    .select('id, age_seconds, nominal_delay_s, captured_at')
    .eq('mint', mint)
    .order('id', { ascending: true });
  if (directErr) throw new Error(`lecture token_snapshots (direct): ${directErr.message}`);
  console.log(`\nSnapshots trouvés directement dans token_snapshots pour ce mint : ${(directSnapshots || []).length}`);
  for (const s of (directSnapshots || []).slice(0, 10)) console.log(`  id=${s.id} age_seconds=${s.age_seconds} nominal_delay_s=${s.nominal_delay_s} captured_at=${s.captured_at}`);
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

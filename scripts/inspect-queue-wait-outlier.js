#!/usr/bin/env node
'use strict';

// Diagnostic ponctuel (lecture seule) : le queue_wait_ms max observé
// (~14 280 895ms, ~3.97h) apparaît identique que la fenêtre de mesure
// remonte au déploiement du 2e correctif (20:46:32Z) ou seulement au
// process courant (relancé 02:36:42Z, ~79min avant la mesure) — ce qui
// est mathématiquement impossible si ce process n'a que 79min d'âge.
// Ce script affiche la ligne brute correspondante pour trancher : vrai
// bug de calcul, ou artefact de requête laissant passer une ligne
// ancienne.
//
// Usage : node scripts/inspect-queue-wait-outlier.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(n) {
  const v = process.env[n];
  if (!v) throw new Error(`env manquant: ${n}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  const { data, error } = await supabase
    .from('token_snapshots')
    .select('id, mint, captured_at, nominal_delay_s, scheduled_at, queued_at, started_at, completed_at, queue_wait_ms, rpc_call_ms')
    .order('queue_wait_ms', { ascending: false })
    .limit(10);
  if (error) throw new Error(`lecture: ${error.message}`);

  console.log('10 plus grands queue_wait_ms toutes périodes confondues :');
  for (const row of data) {
    console.log(JSON.stringify(row, null, 2));
  }
}

main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

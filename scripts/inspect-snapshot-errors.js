#!/usr/bin/env node
'use strict';

// Diagnostic read-only : le rapport a montré 80 bonding_curve_snapshot_error
// dans ingestion_log peu après l'activation de la capture de trajectoire —
// ce script affiche le détail (message d'erreur réel) de quelques-unes
// pour comprendre la cause (RPC bloqué/rate-limité depuis les runners
// GitHub Actions, bug de décodage, PDA introuvable, etc.) avant de décider
// quoi corriger. Ne touche à rien.
//
// Usage : node scripts/inspect-snapshot-errors.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  const { data, error } = await supabase
    .from('ingestion_log')
    .select('at, detail')
    .eq('event_type', 'bonding_curve_snapshot_error')
    .order('at', { ascending: false })
    .limit(20);
  if (error) throw new Error(`lecture ingestion_log: ${error.message}`);

  console.log(`${data.length} erreur(s) la/les plus récente(s) :`);
  for (const row of data) {
    console.log(`  [${row.at}] ${row.detail}`);
  }

  const { count: snapshotCount, error: snapError } = await supabase
    .from('token_snapshots')
    .select('id', { count: 'exact', head: true });
  if (snapError) throw new Error(`lecture token_snapshots: ${snapError.message}`);
  console.log(`\nTotal token_snapshots réussis : ${snapshotCount}`);
}

main().catch((err) => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});

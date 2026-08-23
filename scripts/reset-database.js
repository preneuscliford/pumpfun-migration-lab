#!/usr/bin/env node
'use strict';

// Reset complet — usage unique, demandé explicitement (2026-08-23) avant
// d'implémenter le gate d'activité à deux niveaux (V2) : on repart d'une
// base vide plutôt que de mélanger les lignes collectées avec l'ancien
// schéma de cascade (4 snapshots fixes, holders inconditionnels) et les
// futures lignes V2 (gate + holders gatés + colonnes dérivées).
//
// Vide tokens, token_snapshots ET ingestion_log — les trois tables.
// token_snapshots a "on delete cascade" sur tokens(mint), donc supprimer
// tokens suffirait techniquement, mais on vide chaque table explicitement
// pour avoir des comptages avant/après clairs par table.
//
// Destructif et volontairement à usage unique : n'est pas conçu pour être
// relancé. À supprimer du repo une fois exécuté avec succès (même
// pratique que scripts/purge-pretrajectory-tokens.js, déjà retiré).
//
// Garde-fou : exige CONFIRM=yes-reset-entire-database.
//
// Usage : CONFIRM=yes-reset-entire-database node scripts/reset-database.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function countRows(supabase, table) {
  const { count, error } = await supabase.from(table).select('*', { count: 'exact', head: true });
  if (error) throw new Error(`comptage ${table}: ${error.message}`);
  return count ?? 0;
}

async function main() {
  const confirm = requireEnv('CONFIRM');
  if (confirm !== 'yes-reset-entire-database') {
    throw new Error("CONFIRM doit valoir exactement 'yes-reset-entire-database' — abandon par sécurité.");
  }

  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  const TABLES = [
    { name: 'token_snapshots', idColumn: 'id', filter: (q) => q.gte('id', 0) },
    { name: 'tokens', idColumn: 'mint', filter: (q) => q.not('mint', 'is', null) },
    { name: 'ingestion_log', idColumn: 'id', filter: (q) => q.gte('id', 0) },
  ];

  for (const t of TABLES) {
    const before = await countRows(supabase, t.name);
    console.log(`${t.name} : ${before} ligne(s) avant suppression`);
  }

  console.log('\nSuppression en cours...');
  for (const t of TABLES) {
    let query = supabase.from(t.name).delete({ count: 'exact' });
    query = t.filter(query);
    const { error, count } = await query;
    if (error) throw new Error(`suppression ${t.name}: ${error.message}`);
    console.log(`  ${t.name} : ${count} ligne(s) supprimée(s)`);
  }

  console.log('\nVérification après suppression :');
  for (const t of TABLES) {
    const after = await countRows(supabase, t.name);
    console.log(`  ${t.name} : ${after} ligne(s) restante(s)`);
  }
}

main().catch((err) => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});

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

// Suppression par lots : un DELETE portant sur ~60 000 lignes avec des
// colonnes jsonb volumineuses (raw_new_token_event) dépasse le statement
// timeout de Supabase (repéré le 2026-08-23, table tokens). On sélectionne
// un lot de clés, on les supprime, on répète jusqu'à ce qu'il n'en reste
// plus — chaque requête individuelle reste petite.
const BATCH_SIZE = 2000;

async function deleteAllRows(supabase, table, keyColumn) {
  let totalDeleted = 0;
  for (;;) {
    const { data: batch, error: selectError } = await supabase.from(table).select(keyColumn).limit(BATCH_SIZE);
    if (selectError) throw new Error(`lecture lot ${table}: ${selectError.message}`);
    if (!batch.length) break;
    const keys = batch.map((r) => r[keyColumn]);
    const { error: deleteError, count } = await supabase.from(table).delete({ count: 'exact' }).in(keyColumn, keys);
    if (deleteError) throw new Error(`suppression lot ${table}: ${deleteError.message}`);
    totalDeleted += count ?? 0;
    console.log(`  ${table} : lot de ${keys.length} supprimé (total ${totalDeleted})`);
  }
  return totalDeleted;
}

async function main() {
  const confirm = requireEnv('CONFIRM');
  if (confirm !== 'yes-reset-entire-database') {
    throw new Error("CONFIRM doit valoir exactement 'yes-reset-entire-database' — abandon par sécurité.");
  }

  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  // token_snapshots d'abord (FK vers tokens), même si "on delete cascade"
  // la viderait aussi indirectement — explicite plutôt qu'implicite.
  const TABLES = [
    { name: 'token_snapshots', keyColumn: 'id' },
    { name: 'tokens', keyColumn: 'mint' },
    { name: 'ingestion_log', keyColumn: 'id' },
  ];

  for (const t of TABLES) {
    const before = await countRows(supabase, t.name);
    console.log(`${t.name} : ${before} ligne(s) avant suppression`);
  }

  console.log('\nSuppression en cours (par lots de ' + BATCH_SIZE + ')...');
  for (const t of TABLES) {
    await deleteAllRows(supabase, t.name, t.keyColumn);
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

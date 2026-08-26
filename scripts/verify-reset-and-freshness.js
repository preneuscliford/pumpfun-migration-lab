#!/usr/bin/env node
'use strict';

// Diagnostic (lecture seule) demandé le 2026-08-26 juste après le reset de
// la base (truncate token_snapshots, tokens, ingestion_log), pour
// confirmer : (1) les anciennes données ont bien été effacées, (2) le
// collecteur tourne toujours et écrit de nouvelles lignes fraîches.
//
// Ne modifie rien. Usage : node scripts/verify-reset-and-freshness.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function countRows(supabase, table) {
  const { count, error } = await supabase.from(table).select('*', { count: 'exact', head: true });
  if (error) throw new Error(`count ${table}: ${error.message}`);
  return count;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  console.log('='.repeat(78));
  console.log(`Vérification reset + fraîcheur des données — ${new Date().toISOString()}`);
  console.log('='.repeat(78));

  for (const table of ['tokens', 'token_snapshots', 'ingestion_log']) {
    const n = await countRows(supabase, table);
    console.log(`  ${table.padEnd(20)} : ${n} lignes`);
  }

  // .not('created_at', 'is', null) : en DESC, Postgres met les NULL en
  // premier par défaut, ce qui fausserait "les plus récents" (même piège
  // que inspect-queue-wait-outlier.js plus tôt dans la session).
  const { data: newest, error: newestErr } = await supabase
    .from('tokens')
    .select('mint, created_at')
    .not('created_at', 'is', null)
    .order('created_at', { ascending: false })
    .limit(5);
  if (newestErr) throw new Error(`lecture tokens (newest): ${newestErr.message}`);

  const { data: oldest, error: oldestErr } = await supabase
    .from('tokens')
    .select('mint, created_at')
    .not('created_at', 'is', null)
    .order('created_at', { ascending: true })
    .limit(5);
  if (oldestErr) throw new Error(`lecture tokens (oldest): ${oldestErr.message}`);

  console.log('\n-- 5 tokens les plus récents --');
  for (const t of newest || []) console.log(`  ${t.mint}  created_at=${t.created_at}`);

  console.log('\n-- 5 tokens les plus anciens (devrait être juste après le reset) --');
  for (const t of oldest || []) console.log(`  ${t.mint}  created_at=${t.created_at}`);

  // token_snapshots à 0 alors que des tokens vieux de >1min existent est
  // anormal (le gate universel 2/5/10s devrait déjà avoir produit des
  // lignes) — on regarde ingestion_log pour voir s'il y a des erreurs
  // 'bonding_curve_snapshot_error' ou autre chose d'inhabituel.
  const { data: recentLog, error: logErr } = await supabase
    .from('ingestion_log')
    .select('event_type, at, detail')
    .order('at', { ascending: false })
    .limit(30);
  if (logErr) throw new Error(`lecture ingestion_log: ${logErr.message}`);

  console.log('\n-- 30 dernières lignes ingestion_log (event_type) --');
  const counts = {};
  for (const l of recentLog || []) counts[l.event_type] = (counts[l.event_type] || 0) + 1;
  for (const [type, n] of Object.entries(counts)) console.log(`  ${type.padEnd(30)} : ${n}`);

  const errors = (recentLog || []).filter((l) => l.event_type.includes('error'));
  if (errors.length) {
    console.log('\n-- Détail des erreurs récentes --');
    for (const e of errors.slice(0, 5)) console.log(`  [${e.at}] ${e.event_type}: ${e.detail}`);
  }

  if (newest && newest.length) {
    const ageMs = Date.now() - new Date(newest[0].created_at).getTime();
    console.log(`\nÂge du token le plus récent : ${(ageMs / 1000).toFixed(0)}s`);
    console.log(ageMs < 5 * 60 * 1000 ? '=> Le collecteur écrit bien des données fraîches.' : '=> ATTENTION : le token le plus récent date de plus de 5 minutes, à vérifier.');
  } else {
    console.log('\nAucun token en base pour le moment (normal juste après le reset, le collecteur doit encore recevoir un événement).');
  }

  console.log('='.repeat(78));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

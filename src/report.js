#!/usr/bin/env node
'use strict';

// Rapport d'observation — lecture seule, ne modifie rien. Donne un premier
// coup d'œil sur ce qui a été collecté : taux de migration, délai de
// migration, comparaison des features statiques entre migrés et
// non-migrés, santé de la collecte (trous de connexion). Avec le peu de
// données accumulées au début, ce n'est PAS une conclusion statistique —
// juste un smoke-test du pipeline et un premier repère.
//
// Usage : node src/report.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

function mean(arr) {
  return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : null;
}

function median(arr) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function fmt(v, digits = 4) {
  return v === null || v === undefined || Number.isNaN(v) ? 'n/a' : v.toFixed(digits);
}

function compareFeature(name, migrated, nonMigrated, extractor) {
  const mVals = migrated.map(extractor).filter((v) => v !== null && v !== undefined);
  const nVals = nonMigrated.map(extractor).filter((v) => v !== null && v !== undefined);
  console.log(
    `  ${name.padEnd(32)} migrés: moy=${fmt(mean(mVals))} méd=${fmt(median(mVals))} (n=${mVals.length})` +
      `  |  non-migrés: moy=${fmt(mean(nVals))} méd=${fmt(median(nVals))} (n=${nVals.length})`
  );
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  const { data: tokens, error } = await supabase.from('tokens').select('*');
  if (error) throw new Error(`lecture tokens: ${error.message}`);

  const total = tokens.length;
  const migrated = tokens.filter((t) => t.migrated);
  const nonMigrated = tokens.filter((t) => !t.migrated);
  const creationMissed = tokens.filter((t) => !t.raw_new_token_event);
  const observationClosed = nonMigrated.filter((t) => t.observation_closed_at);
  const stillOpen = nonMigrated.filter((t) => !t.observation_closed_at);

  console.log('='.repeat(72));
  console.log(`Rapport pumpfun-migration-lab — ${new Date().toISOString()}`);
  console.log('='.repeat(72));
  console.log(`Tokens observés au total : ${total}`);
  console.log(`  migrés            : ${migrated.length}${total ? ` (${((migrated.length / total) * 100).toFixed(2)}%)` : ''}`);
  console.log(`  non migrés        : ${nonMigrated.length} (dont ${stillOpen.length} encore dans leur fenêtre de 6h, ${observationClosed.length} fenêtre fermée)`);
  console.log(`  création manquée  : ${creationMissed.length} (vus seulement via leur événement de migration)`);

  const ttms = migrated.map((t) => t.time_to_migration_seconds).filter((v) => v !== null && v !== undefined);
  if (ttms.length) {
    console.log(
      `\nDélai de migration (n=${ttms.length}, exclut les créations manquées) : ` +
        `moyenne ${fmt(mean(ttms), 0)}s | médiane ${fmt(median(ttms), 0)}s | min ${Math.min(...ttms)}s | max ${Math.max(...ttms)}s`
    );
  } else {
    console.log('\nAucune migration avec délai connu pour le moment.');
  }

  console.log('\nComparaison migrés vs non-migrés (features statiques à la création) :');
  console.log('  (échantillon encore petit — repère, pas une conclusion statistique)');
  compareFeature('initial_market_cap_sol', migrated, nonMigrated, (t) => t.initial_market_cap_sol);
  compareFeature('initial_virtual_sol_reserves', migrated, nonMigrated, (t) => t.initial_virtual_sol_reserves);
  compareFeature('initial_virtual_token_reserves', migrated, nonMigrated, (t) => t.initial_virtual_token_reserves);
  console.log('  (has_twitter/has_telegram/has_website : non renseigné en V1, pas encore comparable)');

  const { data: log, error: logError } = await supabase.from('ingestion_log').select('event_type, at').order('at', { ascending: true });
  if (logError) throw new Error(`lecture ingestion_log: ${logError.message}`);

  const counts = {};
  for (const l of log || []) counts[l.event_type] = (counts[l.event_type] || 0) + 1;
  console.log('\nJournal d\'ingestion :', JSON.stringify(counts));
  if (log && log.length) {
    const first = new Date(log[0].at);
    const last = new Date(log[log.length - 1].at);
    console.log(`Période couverte : ${first.toISOString()} -> ${last.toISOString()} (${((last - first) / 60000).toFixed(1)} min)`);
  }
  console.log('='.repeat(72));
}

main().catch((err) => {
  console.error('Erreur:', err.message);
  process.exit(1);
});

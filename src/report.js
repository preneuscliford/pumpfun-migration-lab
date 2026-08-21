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

function percentile(arr, p) {
  if (!arr.length) return null;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

// Bornes de temps rondes (pas choisies à partir de la distribution
// observée) — uniquement pour rendre l'histogramme lisible, pas une
// hypothèse sur où se situe un seuil intéressant.
const HISTOGRAM_BINS_SECONDS = [30, 60, 120, 300, 600, 1800, 3600, 10800, 21600]; // 30s,1min,2min,5min,10min,30min,1h,3h,6h

function formatSeconds(s) {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${(s / 60).toFixed(s % 60 === 0 ? 0 : 1)}min`;
  return `${(s / 3600).toFixed(1)}h`;
}

function printHistogram(values, bins) {
  const counts = new Array(bins.length + 1).fill(0);
  for (const v of values) {
    const i = bins.findIndex((b) => v <= b);
    counts[i === -1 ? bins.length : i] += 1;
  }
  const maxCount = Math.max(...counts, 1);
  const barWidth = 40;
  for (let i = 0; i < counts.length; i++) {
    const label = i === 0 ? `≤${formatSeconds(bins[0])}` : i === bins.length ? `>${formatSeconds(bins[bins.length - 1])}` : `${formatSeconds(bins[i - 1])}-${formatSeconds(bins[i])}`;
    const bar = '#'.repeat(Math.round((counts[i] / maxCount) * barWidth));
    console.log(`    ${label.padEnd(12)} ${bar.padEnd(barWidth)} ${counts[i]}`);
  }
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

  const { data: allTokens, error } = await supabase.from('tokens').select('*');
  if (error) throw new Error(`lecture tokens: ${error.message}`);

  // On ne garde que les tokens dont on a réellement vu l'événement de
  // création : sans lui, on ne connaît ni son âge réel ni ses features de
  // départ (market cap initial, réserves...), et on ne peut même pas
  // savoir s'il existait déjà avant qu'on commence à écouter. Les inclure
  // fausserait le taux de migration et toute comparaison de features.
  const creationMissedCount = allTokens.filter((t) => !t.raw_new_token_event).length;
  const tokens = allTokens.filter((t) => t.raw_new_token_event);

  const total = tokens.length;
  const migrated = tokens.filter((t) => t.migrated);
  const nonMigrated = tokens.filter((t) => !t.migrated);
  const observationClosed = nonMigrated.filter((t) => t.observation_closed_at);
  const stillOpen = nonMigrated.filter((t) => !t.observation_closed_at);

  console.log('='.repeat(72));
  console.log(`Rapport pumpfun-migration-lab — ${new Date().toISOString()}`);
  console.log('='.repeat(72));
  console.log(`Tokens avec création observée : ${total} (${creationMissedCount} exclu(s) — création jamais vue, pas d'âge/features fiables)`);
  console.log(`  migrés            : ${migrated.length}${total ? ` (${((migrated.length / total) * 100).toFixed(2)}%)` : ''}`);
  console.log(`  non migrés        : ${nonMigrated.length} (dont ${stillOpen.length} encore dans leur fenêtre de 6h, ${observationClosed.length} fenêtre fermée)`);

  const ttms = migrated.map((t) => t.time_to_migration_seconds).filter((v) => v !== null && v !== undefined);
  if (ttms.length) {
    console.log(
      `\nDélai de migration (n=${ttms.length}) : ` +
        `moyenne ${fmt(mean(ttms), 0)}s | médiane ${fmt(median(ttms), 0)}s | min ${Math.min(...ttms)}s | max ${Math.max(...ttms)}s`
    );
    console.log(
      '  percentiles : ' +
        [10, 25, 50, 75, 90, 95, 99]
          .map((p) => `P${p}=${fmt(percentile(ttms, p), 0)}s`)
          .join(' | ')
    );
    console.log('  histogramme :');
    printHistogram(ttms, HISTOGRAM_BINS_SECONDS);
  } else {
    console.log('\nAucune migration avec délai connu pour le moment.');
  }

  // Taux de migration conditionnel par tranche de capital initial :
  // délibérément PAS implémenté ici pour l'instant. Choisir les seuils de
  // tranche à partir de la distribution qu'on est en train d'observer
  // reviendrait à fabriquer un signal après coup plutôt qu'à le tester —
  // on attend un échantillon bien plus grand (100+ migrations) avant de
  // figer des seuils, pour ne pas contaminer la collecte avec nos
  // premières impressions.

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

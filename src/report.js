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

// Tranches de délai de migration — bornes fournies explicitement (pas
// dérivées de la distribution observée), (min, max].
const DELAY_BUCKETS = [
  { label: '≤10s', min: -Infinity, max: 10 },
  { label: '10s-1min', min: 10, max: 60 },
  { label: '1-5min', min: 60, max: 300 },
  { label: '5-15min', min: 300, max: 900 },
  { label: '>15min', min: 900, max: Infinity },
];

function bucketFor(ttmSeconds) {
  return DELAY_BUCKETS.find((b) => ttmSeconds > b.min && ttmSeconds <= b.max);
}

function distStats(values) {
  const clean = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  return { n: clean.length, mean: mean(clean), median: median(clean), p25: percentile(clean, 25), p75: percentile(clean, 75) };
}

function fmtDistStats(s) {
  if (!s.n) return 'n=0';
  return `n=${String(s.n).padStart(3)}  méd=${fmt(s.median, 2).padStart(10)}  (P25=${fmt(s.p25, 2)} P75=${fmt(s.p75, 2)})  moy=${fmt(s.mean, 2)}`;
}

// Extrait un champ numérique directement du JSON brut de l'événement de
// création — pas besoin de l'avoir prévu comme colonne dédiée à
// l'ingestion, c'est exactement le rôle de raw_new_token_event (voir
// README). solAmount/initialBuy ne sont PAS des colonnes du schéma.
function rawNumField(t, field) {
  const raw = t.raw_new_token_event;
  if (!raw || raw[field] === undefined || raw[field] === null) return null;
  const n = Number(raw[field]);
  return Number.isNaN(n) ? null : n;
}

// Compare la distribution (pas seulement la moyenne) d'une feature entre
// chaque tranche de délai de migration ET les non-migrés — c'est la vraie
// question posée : les migrations ultra-rapides étaient-elles déjà
// statistiquement différentes à la création, séparément des migrations
// lentes et des non-migrés ?
function printFeatureDistribution(name, groups) {
  console.log(`\n  ${name} :`);
  for (const g of groups) {
    console.log(`    ${g.label.padEnd(12)} ${fmtDistStats(distStats(g.values))}`);
  }
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
  //
  // Pareil pour toute règle de décision ("capital > X -> intéressant") :
  // hors scope tant qu'on n'a pas de quoi faire une vraie validation
  // hors échantillon (découverte sur ~80% des données, test sur les 20%
  // restants, idéalement en split CHRONOLOGIQUE — jours 1..N pour
  // l'hypothèse, jours N+1..N+2 pour la tester). Avec 21 migrations,
  // n'importe quel seuil "trouvé" serait de l'overfitting pur. On vise au
  // moins quelques centaines de migrations avant d'envisager ce split.

  console.log('\nDistribution des features à la création, par tranche de délai de migration + non-migrés :');
  console.log('  (échantillon encore petit, surtout par tranche — repère, pas une conclusion statistique)');

  const bucketGroups = DELAY_BUCKETS.map((b) => ({
    label: b.label,
    tokens: migrated.filter((t) => t.time_to_migration_seconds !== null && t.time_to_migration_seconds !== undefined && bucketFor(t.time_to_migration_seconds) === b),
  }));
  const allGroups = [...bucketGroups, { label: 'non-migrés', tokens: nonMigrated }];

  printFeatureDistribution(
    'initial_market_cap_sol',
    allGroups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.initial_market_cap_sol) }))
  );
  printFeatureDistribution(
    'initial_virtual_sol_reserves',
    allGroups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.initial_virtual_sol_reserves) }))
  );
  printFeatureDistribution(
    'initial_virtual_token_reserves',
    allGroups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.initial_virtual_token_reserves) }))
  );
  // Pas des colonnes du schéma — extraites du JSON brut à la volée (voir
  // rawNumField). C'est exactement le genre de champ qu'on n'avait pas
  // prévu d'extraire à l'ingestion mais que raw_new_token_event permet de
  // récupérer après coup.
  printFeatureDistribution(
    'solAmount (achat initial du créateur, en SOL)',
    allGroups.map((g) => ({ label: g.label, values: g.tokens.map((t) => rawNumField(t, 'solAmount')) }))
  );
  printFeatureDistribution(
    'initialBuy (achat initial du créateur, en tokens)',
    allGroups.map((g) => ({ label: g.label, values: g.tokens.map((t) => rawNumField(t, 'initialBuy')) }))
  );

  console.log('\n  (has_twitter/has_telegram/has_website : non renseigné en V1, pas encore comparable)');

  // is_mayhem_mode : flag booléen vu dans le JSON brut de certains
  // événements de création (jamais documenté explicitement par nous —
  // encore un exemple de ce que raw_new_token_event permet de creuser
  // après coup). Comparaison en TAUX de migration, pas en distribution
  // (c'est une variable catégorielle, pas continue).
  const mayhemGroups = { true: [], false: [], 'n/a': [] };
  for (const t of tokens) {
    const raw = t.raw_new_token_event;
    const key = raw && typeof raw.is_mayhem_mode === 'boolean' ? String(raw.is_mayhem_mode) : 'n/a';
    mayhemGroups[key].push(t);
  }
  console.log('\n  is_mayhem_mode (taux de migration par valeur) :');
  for (const key of ['true', 'false', 'n/a']) {
    const group = mayhemGroups[key];
    const migratedInGroup = group.filter((t) => t.migrated).length;
    const rate = group.length ? ((migratedInGroup / group.length) * 100).toFixed(2) : 'n/a';
    console.log(`    ${key.padEnd(12)} n=${String(group.length).padStart(4)}  migrés=${String(migratedInGroup).padStart(3)}  (${rate}%)`);
  }

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

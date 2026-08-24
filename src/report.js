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

// PostgREST plafonne chaque requête à 1000 lignes par défaut — sans
// pagination, tokens/ingestion_log se retrouvaient silencieusement
// tronqués dès qu'on dépassait 1000 lignes (repéré le 2026-08-21 : le
// rapport annonçait ~1000 tokens alors que la collecte en avait déjà
// plusieurs milliers). orderColumn doit être une colonne unique (clé
// primaire) pour que .range() donne des pages stables sans doublon ni
// trou.
const PAGE_SIZE = 1000;

async function fetchAllRows(supabase, table, select, orderColumn, applyFilter) {
  const rows = [];
  let from = 0;
  for (;;) {
    let query = supabase.from(table).select(select).order(orderColumn, { ascending: true }).range(from, from + PAGE_SIZE - 1);
    if (applyFilter) query = applyFilter(query);
    const { data, error } = await query;
    if (error) throw new Error(`lecture ${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows;
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

// Trois groupes de résultat. A (immédiate) est explicitement EXCLU de la
// population cible : trop proche d'un événement mécanique (l'achat
// initial du créateur qui franchit seul le seuil de la bonding curve), et
// ça ne correspond de toute façon pas à la philosophie du projet — on veut
// le temps d'observer un token, pas courir après un événement déjà
// terminé à la création. Gardé dans le rapport uniquement pour
// surveillance (vérifier que le motif reste stable), jamais comme
// candidat à un signal exploitable. La vraie comparaison de recherche est
// B (progressive) vs C (non-migré, témoin). Seuil des 10s fourni
// explicitement, pas dérivé de la distribution.
const OUTCOME_GROUPS = [
  { label: 'A. immédiate ≤10s [HORS CIBLE, surveillance uniquement]', filter: (t) => t.migrated && t.time_to_migration_seconds !== null && t.time_to_migration_seconds !== undefined && t.time_to_migration_seconds <= 10 },
  { label: 'B. progressive >10s [POPULATION CIBLE]', filter: (t) => t.migrated && t.time_to_migration_seconds !== null && t.time_to_migration_seconds !== undefined && t.time_to_migration_seconds > 10 },
  { label: 'C. non-migré [témoin]', filter: (t) => !t.migrated },
];

function groupTokens(tokens) {
  return OUTCOME_GROUPS.map((g) => ({ label: g.label, tokens: tokens.filter(g.filter) }));
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
    console.log(`    ${g.label.padEnd(24)} ${fmtDistStats(distStats(g.values))}`);
  }
}

// Variable catégorielle (pas continue) : répartition en compte + % au
// sein de CHAQUE groupe A/B/C, pas un taux de migration par valeur —
// c'est la comparaison symétrique aux features continues ci-dessus.
function printCategoricalDistribution(name, groups, extractor) {
  console.log(`\n  ${name} :`);
  for (const g of groups) {
    const counts = {};
    for (const t of g.tokens) {
      const key = extractor(t);
      counts[key] = (counts[key] || 0) + 1;
    }
    const total = g.tokens.length;
    const parts = Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .map(([k, c]) => `${k}=${c} (${total ? ((c / total) * 100).toFixed(1) : '0.0'}%)`)
      .join(', ');
    console.log(`    ${g.label.padEnd(24)} n=${String(total).padStart(4)}  ${parts}`);
  }
}

// Profil de référence : plages P10-P90 d'une feature, calculées sur UN
// SEUL groupe (typiquement B, les migrés progressifs). Différent de
// printFeatureDistribution (qui compare A/B/C côte à côte avec seulement
// méd/P25/P75) — ici on veut la plage la plus large possible d'un seul
// groupe, la matière première pour tout ce qu'on construira dessus. Pas
// un seuil, pas un score : juste ce à quoi ressemblent, empiriquement,
// les paramètres des tokens qui ont réellement migré.
function printProfileStats(name, values) {
  const clean = values.filter((v) => v !== null && v !== undefined && !Number.isNaN(v));
  if (!clean.length) {
    console.log(`  ${name.padEnd(38)} n=0`);
    return;
  }
  const [p10, p25, p50, p75, p90] = [10, 25, 50, 75, 90].map((p) => fmt(percentile(clean, p), 2));
  console.log(`  ${name.padEnd(38)} n=${String(clean.length).padStart(4)}  P10=${p10} P25=${p25} P50=${p50} P75=${p75} P90=${p90}`);
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  const allTokens = await fetchAllRows(supabase, 'tokens', '*', 'mint');

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
  const groups = groupTokens(tokens);
  const immediateCount = groups[0].tokens.length; // A — hors population cible
  const targetCount = groups[1].tokens.length; // B — progressive, population cible

  console.log('='.repeat(72));
  console.log(`Rapport pumpfun-migration-lab — ${new Date().toISOString()}`);
  console.log('='.repeat(72));
  console.log(`Tokens avec création observée : ${total} (${creationMissedCount} exclu(s) — création jamais vue, pas d'âge/features fiables)`);
  console.log(`  migrés (brut)     : ${migrated.length}${total ? ` (${((migrated.length / total) * 100).toFixed(2)}%)` : ''} — dont ${immediateCount} immédiate(s) [hors cible] + ${targetCount} progressive(s) [cible]`);
  console.log(`  migrés CIBLE      : ${targetCount}${total ? ` (${((targetCount / total) * 100).toFixed(2)}%)` : ''} — taux de migration hors migrations mécaniques (≤10s exclues)`);
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

  console.log('\nDistribution des features à la création, par groupe de résultat (A hors cible / B cible / C témoin) :');
  console.log('  (échantillon encore petit, surtout pour A et B — repère, pas une conclusion statistique)');

  printFeatureDistribution(
    'initial_market_cap_sol',
    groups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.initial_market_cap_sol) }))
  );
  printFeatureDistribution(
    'initial_virtual_sol_reserves',
    groups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.initial_virtual_sol_reserves) }))
  );
  printFeatureDistribution(
    'initial_virtual_token_reserves',
    groups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.initial_virtual_token_reserves) }))
  );
  // Pas des colonnes du schéma — extraites du JSON brut à la volée (voir
  // rawNumField). C'est exactement le genre de champ qu'on n'avait pas
  // prévu d'extraire à l'ingestion mais que raw_new_token_event permet de
  // récupérer après coup.
  printFeatureDistribution(
    'solAmount (achat initial du créateur, en SOL)',
    groups.map((g) => ({ label: g.label, values: g.tokens.map((t) => rawNumField(t, 'solAmount')) }))
  );
  printFeatureDistribution(
    'initialBuy (achat initial du créateur, en tokens)',
    groups.map((g) => ({ label: g.label, values: g.tokens.map((t) => rawNumField(t, 'initialBuy')) }))
  );

  // is_mayhem_mode : flag booléen vu dans le JSON brut de certains
  // événements de création (jamais documenté explicitement par nous —
  // encore un exemple de ce que raw_new_token_event permet de creuser
  // après coup). Variable catégorielle : répartition par groupe, pas taux
  // de migration.
  printCategoricalDistribution('is_mayhem_mode', groups, (t) => {
    const raw = t.raw_new_token_event;
    return raw && typeof raw.is_mayhem_mode === 'boolean' ? String(raw.is_mayhem_mode) : 'n/a';
  });

  console.log('\n  métadonnées (has_twitter/has_telegram/has_website) : non renseigné en V1, pas encore comparable');

  // Profil de référence groupe B seul (pas une comparaison A/B/C) : la
  // matière première pour tout système futur basé sur les paramètres des
  // tokens réellement migrés progressivement. Toujours aucun seuil, aucun
  // score, aucune décision ici — juste caractériser ce groupe.
  console.log('\n' + '='.repeat(72));
  console.log(`PROFIL DE RÉFÉRENCE — GROUPE B seul, migration progressive (n=${targetCount})`);
  console.log('  Plages P10-P90 sur les tokens RÉELLEMENT migrés progressivement');
  console.log('  uniquement — pas un seuil, pas un score, pas une comparaison ici.');
  console.log('='.repeat(72));

  const bTokens = groups[1].tokens;
  console.log('\nÀ la création :');
  printProfileStats('initial_market_cap_sol', bTokens.map((t) => t.initial_market_cap_sol));
  printProfileStats('initial_virtual_sol_reserves', bTokens.map((t) => t.initial_virtual_sol_reserves));
  printProfileStats('initial_virtual_token_reserves', bTokens.map((t) => t.initial_virtual_token_reserves));
  printProfileStats('solAmount (achat créateur, SOL)', bTokens.map((t) => rawNumField(t, 'solAmount')));
  printProfileStats('initialBuy (achat créateur, tokens)', bTokens.map((t) => rawNumField(t, 'initialBuy')));
  const bMayhemKnown = bTokens.filter((t) => t.raw_new_token_event && typeof t.raw_new_token_event.is_mayhem_mode === 'boolean');
  const bMayhemTrue = bMayhemKnown.filter((t) => t.raw_new_token_event.is_mayhem_mode === true).length;
  console.log(
    `  ${'is_mayhem_mode=true'.padEnd(38)} ${bMayhemKnown.length ? `${bMayhemTrue}/${bMayhemKnown.length} (${((bMayhemTrue / bMayhemKnown.length) * 100).toFixed(1)}%)` : 'n=0'}`
  );

  // Question de recherche V2 (voir sql/schema.sql, en-tête) : à T+5s/10s/
  // 20s/30s, quelles caractéristiques de la bonding curve distinguent déjà
  // B de C ? bc_ratio_tXs/bc_first_active_at_s/bc_peak_ratio sont écrits EN
  // DIRECT par le listener au fil de la cascade — pas besoin de relire
  // token_snapshots ici, contrairement à la V1.
  //
  // Restreint aux tokens créés APRÈS le hotfix du 2026-08-23 21:21 UTC
  // (course upsert/cascade corrigée, voir listener.js) : les tokens créés
  // avant peuvent avoir bc_ratio_t5s/t10s manquants à cause du bug, pas
  // comparables au reste. Fenêtre surchargeable via V2_ANALYSIS_SINCE
  // pour rejouer cette section sans republier le code.
  const v2Since = new Date(process.env.V2_ANALYSIS_SINCE || '2026-08-23T21:21:20Z');
  const v2Tokens = tokens.filter((t) => t.created_at && new Date(t.created_at) >= v2Since);
  const v2Groups = groupTokens(v2Tokens);
  const v2B = v2Groups[1].tokens;
  const v2C = v2Groups[2].tokens;

  console.log('\n' + '='.repeat(72));
  console.log(`CASCADE V2 — B vs C depuis ${v2Since.toISOString()} (B n=${v2B.length}, C n=${v2C.length})`);
  console.log('  bc_ratio_tXs = virtual_sol_reserves observé / initial, à la lecture la');
  console.log('  plus proche de ce délai nominal. NULL = pas de lecture à ce point (la');
  console.log('  cascade s\'est arrêtée avant — gate jugé inactif, ou déjà résolu).');
  console.log('='.repeat(72));

  if (!v2B.length && !v2C.length) {
    console.log('\nAucun token créé depuis ce hotfix pour le moment — trop tôt.');
  } else {
    const bcGroups = [
      { label: 'B. progressive [CIBLE]', tokens: v2B },
      { label: 'C. non-migré [témoin]', tokens: v2C },
    ];

    // Piège identifié en observant les premiers résultats V2 (2026-08-24) :
    // le compte de bonding curve est VIDÉ après la migration (0/0, voir
    // scripts/check-bonding-curve-rpc.js). Un token B dont
    // time_to_migration_seconds <= le délai nominal du checkpoint a donc
    // DÉJÀ migré au moment de cette lecture — bc_ratio_tXs y mesure "déjà
    // vidé", pas "pression d'achat avant la migration". Mélanger les deux
    // dans B fait mécaniquement chuter la médiane vers 0 et fabrique une
    // séparation B/C qui ne dit rien sur ce qu'on cherche : un signal
    // observable AVANT que la migration soit connue. On ne garde donc, pour
    // B, que les lectures dont le token n'avait pas encore migré à ce
    // délai — la vraie comparaison pré-migration B vs C.
    const NOMINAL_DELAY_BY_COLUMN = { bc_ratio_t5s: 5, bc_ratio_t10s: 10, bc_ratio_t20s: 20, bc_ratio_t30s: 30 };
    for (const col of ['bc_ratio_t5s', 'bc_ratio_t10s', 'bc_ratio_t20s', 'bc_ratio_t30s']) {
      const delay = NOMINAL_DELAY_BY_COLUMN[col];
      const bPreMigration = v2B.filter((t) => t.time_to_migration_seconds !== null && t.time_to_migration_seconds !== undefined && t.time_to_migration_seconds > delay);
      const bAlreadyMigrated = v2B.length - bPreMigration.length;
      console.log(`\n  [${col}] groupe B : ${bPreMigration.length} pas encore migré(s) à T+${delay}s (gardés) / ${bAlreadyMigrated} déjà migré(s) à ce moment (exclus, compte vidé)`);
      printFeatureDistribution(
        `${col}, PRÉ-migration seulement pour B (couverture entre parenthèses = part des tokens éligibles ayant une lecture à ce point)`,
        [
          { label: bcGroups[0].label, tokens: bPreMigration },
          bcGroups[1],
        ].map((g) => {
          const withValue = g.tokens.filter((t) => t[col] !== null && t[col] !== undefined);
          const coverage = g.tokens.length ? ((withValue.length / g.tokens.length) * 100).toFixed(1) : '0.0';
          return { label: `${g.label} (${coverage}%)`, values: g.tokens.map((t) => t[col]) };
        })
      );
    }
    // Même piège que ci-dessus : pour B, une détection "active" causée par
    // le vidage du compte APRÈS migration (bc_first_active_at_s >=
    // time_to_migration_seconds) n'est pas un signal précoce, c'est la
    // migration elle-même qui se voit. On sépare donc "détecté actif AVANT
    // sa propre migration" (le vrai signal) de "détecté seulement au
    // moment/après" (artefact). C n'a pas ce problème (jamais migré).
    console.log('\n  bc_first_active_at_s (âge en secondes de la 1re lecture jugée active — NULL = jamais détecté actif) :');
    for (const g of bcGroups) {
      const active = g.tokens.filter((t) => t.bc_first_active_at_s !== null && t.bc_first_active_at_s !== undefined);
      const pct = g.tokens.length ? ((active.length / g.tokens.length) * 100).toFixed(1) : '0.0';
      console.log(`    ${g.label.padEnd(24)} actifs=${active.length}/${g.tokens.length} (${pct}%)  ${fmtDistStats(distStats(active.map((t) => t.bc_first_active_at_s)))}`);
      if (g === bcGroups[0]) {
        const preMig = active.filter((t) => t.bc_first_active_at_s < t.time_to_migration_seconds);
        const atOrAfterMig = active.length - preMig.length;
        console.log(`      dont détecté AVANT sa propre migration (signal réel) : ${preMig.length}/${active.length}  ${fmtDistStats(distStats(preMig.map((t) => t.bc_first_active_at_s)))}`);
        console.log(`      dont détecté au moment/après sa migration (artefact du vidage) : ${atOrAfterMig}/${active.length}`);
      }
    }
    // bc_peak_ratio n'est PAS décomposé pré/post-migration : on ne garde en
    // base que la valeur agrégée (le ratio le plus extrême toutes lectures
    // confondues), pas quel checkpoint l'a produite — impossible de savoir
    // après coup si ce pic vient d'avant ou après la migration pour B. À
    // lire comme un indicateur brut, pas encore nettoyé du même artefact.
    printFeatureDistribution(
      'bc_peak_ratio (écart max observé par rapport à 1, toutes lectures confondues — PAS nettoyé du vidage post-migration pour B)',
      bcGroups.map((g) => ({ label: g.label, values: g.tokens.map((t) => t.bc_peak_ratio) }))
    );
  }

  const log = await fetchAllRows(supabase, 'ingestion_log', 'event_type, at, id', 'id');

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

-- Schéma — laboratoire pump.fun.
--
-- Toujours pas de subscribeTokenTrade ni de polling continu.
--
-- V2 (2026-08-23) — gate d'activité à deux niveaux, remplace la cascade
-- fixe à 4 points du 2026-08-21 :
--   1. Gate universel (TOUS les tokens) : lectures à T+2s/T+5s/T+10s.
--   2. Si au moins une lecture dépasse le seuil de bruit calibré sur les
--      données réelles (écart relatif > 1e-4 vs initial_virtual_sol_reserves
--      — voir scripts/calibrate-activity-threshold.js, run du 2026-08-23 :
--      le bruit flottant pur reste sous 1e-8, la masse réelle démarre
--      autour de 1e-4), cascade étendue T+20s/30s/45s/60s.
--   3. Toujours actif à la fin de la cascade étendue -> longue traîne
--      espacée T+2min/5min/10min/20min/30min, arrêtée immédiatement dès
--      qu'une migration est détectée (subscribeMigration, temps réel) ou
--      qu'une lecture montre complete=true/réserve à 0.
-- Les holders (src/holders.js, ~20 appels RPC) suivent le même gate — ne
-- sont capturés qu'au premier point de la cascade étendue (T+20s), donc
-- seulement pour les tokens jugés actifs, plus le fixe "30s pour tous".
--
-- Rétention à deux niveaux : token_snapshots (détail brut) purgée après
-- SNAPSHOT_RETENTION_MS (~4 jours par défaut, voir src/listener.js) — les
-- métriques utiles sont déjà recopiées sur tokens en direct au fil de la
-- cascade, la ligne brute n'est qu'un filet de sécurité temporaire.
-- raw_new_token_event/raw_migration_event sur tokens sont mis à NULL après
-- RAW_JSON_RETENTION_MS (~7 jours par défaut) — même logique, fenêtre plus
-- longue car ce JSON sert de rattrapage si un champ non prévu s'avère
-- utile après coup. Le reste de la ligne tokens (résumé, groupe A/B/C,
-- métriques dérivées bc_*) n'est JAMAIS purgé.

create table if not exists tokens (
  mint text primary key,
  symbol text,
  name text,
  creator text,
  -- PAS de "not null default now()" : created_at ne doit être posé que
  -- quand on observe réellement l'événement de création (buildTokenRow
  -- le fait explicitement dans src/listener.js). Un token dont on n'a vu
  -- que la migration (création manquée, ex. trou de connexion) doit avoir
  -- created_at NULL, pas une valeur bidon proche de migrated_at — sinon
  -- time_to_migration_seconds sort un chiffre proche de 0 voire négatif
  -- au lieu de NULL (bug constaté en prod : -9s, corrigé le 2026-08-21).
  created_at timestamptz,

  -- État initial de la bonding curve au moment de la création — les
  -- "features statiques" sur lesquelles porte l'expérience V1.
  initial_virtual_sol_reserves numeric,
  initial_virtual_token_reserves numeric,
  initial_market_cap_sol numeric,
  metadata_uri text,

  -- Dérivé de metadata_uri en best-effort si on choisit de le résoudre
  -- plus tard (pas fait sur le chemin d'ingestion en V1, pour rester
  -- économe en requêtes — voir README). Laissé nullable en attendant.
  has_twitter boolean,
  has_telegram boolean,
  has_website boolean,

  migrated boolean not null default false,
  migrated_at timestamptz,
  migration_pool text,
  -- Calculé par Postgres à partir de created_at/migrated_at plutôt que par
  -- l'application : évite tout bug de calcul si la création et la
  -- migration d'un même token sont vues par deux runs différents (relais
  -- GitHub Actions) qui ne partagent pas de mémoire.
  time_to_migration_seconds integer generated always as (
    case when migrated_at is not null and created_at is not null
      then (extract(epoch from (migrated_at - created_at)))::int
    end
  ) stored,

  -- Fenêtre d'observation de 6h fermée par cleanup.js pour les non-migrés.
  -- NULL = encore dans sa fenêtre ou déjà migré (jamais fermée dans ce cas).
  observation_closed_at timestamptz,

  -- Assurance contre nos propres angles morts de schéma : l'événement brut
  -- complet, tel que reçu de PumpPortal. Voir README. NULLisé après
  -- RAW_JSON_RETENTION_MS (voir en-tête du fichier) — pas gardé pour
  -- toujours, contrairement au reste de la ligne.
  raw_new_token_event jsonb,
  raw_migration_event jsonb,

  -- Métriques dérivées de la cascade bonding curve V2 (voir en-tête du
  -- fichier), mises à jour EN DIRECT par le listener au fil des lectures —
  -- jamais recalculées après coup à partir de token_snapshots, qui ne
  -- garde qu'une fenêtre glissante. bc_ratio_tXs = virtual_sol_reserves
  -- observé / initial_virtual_sol_reserves à la lecture la plus proche de
  -- ce délai nominal ; NULL = pas de lecture à ce point (cascade arrêtée
  -- avant, ou déjà migré). bc_first_active_at_s = âge en secondes de la
  -- première lecture ayant dépassé le seuil d'activité ; NULL = jamais
  -- détecté actif dans la fenêtre suivie. bc_peak_ratio = ratio le plus
  -- éloigné de 1 observé parmi toutes les lectures de ce token.
  bc_ratio_t5s numeric,
  bc_ratio_t10s numeric,
  bc_ratio_t20s numeric,
  bc_ratio_t30s numeric,
  bc_first_active_at_s integer,
  bc_peak_ratio numeric,
  bc_cascade_reads integer,

  ingested_at timestamptz not null default now()
);

create index if not exists idx_tokens_migrated_created on tokens (migrated, created_at);
create index if not exists idx_tokens_observation_open on tokens (observation_closed_at) where observation_closed_at is null and migrated = false;
-- Sert la purge du JSON brut par ancienneté (created_at < cutoff, encore
-- non nullisé) — trouve rapidement les candidats sans scanner toute la table.
create index if not exists idx_tokens_raw_json_pending on tokens (created_at) where raw_new_token_event is not null;

-- Trajectoire de la bonding curve — cascade V2 à gate d'activité (voir en-
-- tête du fichier). Fenêtre glissante : purgée après SNAPSHOT_RETENTION_MS,
-- voir src/listener.js — les métriques utiles sont déjà sur tokens.bc_*,
-- cette table n'est qu'un détail temporaire. market_cap_sol reste NULL :
-- pas de lecture du supply côté RPC pour ces snapshots (décision
-- volontaire, voir listener.js), donc pas de quoi le calculer proprement.
create table if not exists token_snapshots (
  id bigint generated always as identity primary key,
  mint text not null references tokens(mint) on delete cascade,
  captured_at timestamptz not null default now(),
  age_seconds integer not null,
  -- Délai VISÉ par la cascade (2/5/10/20/30/45/60/120/300/600/1200/1800),
  -- pas l'âge réel ci-dessus qui varie légèrement selon la latence RPC —
  -- remplace le rapprochement approximatif "délai le plus proche" utilisé
  -- dans la V1 de report.js.
  nominal_delay_s integer,
  market_cap_sol numeric,
  virtual_sol_reserves numeric,
  virtual_token_reserves numeric,
  raw_event jsonb,

  -- Concentration des détenteurs (2026-08-21, gate d'activité depuis le
  -- 2026-08-23) : NULL sur la plupart des lignes, rempli une seule fois
  -- par token — au premier point de la cascade étendue (T+20s), donc
  -- seulement pour les tokens jugés actifs au gate. Coûte ~20 appels RPC
  -- par lecture (src/holders.js), d'où le gate plutôt qu'un point fixe
  -- inconditionnel comme avant.
  total_supply numeric,
  curve_held_amount numeric,
  top_holders_count integer,
  top_holders_pct_of_supply numeric,
  holders_error text,
  holders_raw jsonb
);

create index if not exists idx_snapshots_mint_time on token_snapshots (mint, captured_at);
-- Sert la purge par ancienneté (captured_at < cutoff, tous mints
-- confondus) — l'index composite ci-dessus n'aide pas pour ce filtre.
create index if not exists idx_snapshots_captured_at on token_snapshots (captured_at);

-- Journal de connexion/déconnexion WS + relais entre runs GitHub Actions.
-- Sert à savoir, au moment de l'analyse, quelles périodes ont été
-- réellement écoutées (un "calme plat" peut être un vrai calme ou une
-- déconnexion silencieuse — sans ce journal on ne peut pas distinguer
-- les deux).
create table if not exists ingestion_log (
  id bigint generated always as identity primary key,
  event_type text not null, -- 'connected' | 'disconnected' | 'reconnect_attempt' | 'relay_handoff' | 'unknown_event' | 'cleanup_run' | 'snapshots_purged' | 'raw_json_purged' | 'bonding_curve_snapshot_error'
  at timestamptz not null default now(),
  detail text
);

create index if not exists idx_ingestion_log_time on ingestion_log (at);

-- Migration V2 (2026-08-23) — à exécuter une fois dans l'éditeur SQL
-- Supabase si tokens/token_snapshots existent déjà : "create table if not
-- exists" ne touche pas aux tables déjà là, il faut ajouter les nouvelles
-- colonnes explicitement. Sûr à réexécuter (IF NOT EXISTS partout).
alter table tokens add column if not exists bc_ratio_t5s numeric;
alter table tokens add column if not exists bc_ratio_t10s numeric;
alter table tokens add column if not exists bc_ratio_t20s numeric;
alter table tokens add column if not exists bc_ratio_t30s numeric;
alter table tokens add column if not exists bc_first_active_at_s integer;
alter table tokens add column if not exists bc_peak_ratio numeric;
alter table tokens add column if not exists bc_cascade_reads integer;
alter table token_snapshots add column if not exists nominal_delay_s integer;
create index if not exists idx_tokens_raw_json_pending on tokens (created_at) where raw_new_token_event is not null;
create index if not exists idx_snapshots_captured_at on token_snapshots (captured_at);

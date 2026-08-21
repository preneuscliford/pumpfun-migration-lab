-- Schéma V1 — laboratoire pump.fun.
--
-- Décision V1 (voir README) : subscribeNewToken + subscribeMigration
-- uniquement, pas de subscribeTokenTrade ni de polling. Il n'y a donc PAS
-- de flux de snapshots intermédiaires en V1 — la table token_snapshots est
-- créée par avance pour l'option B (snapshots périodiques), mais reste
-- vide tant qu'on ne l'active pas. Ne pas s'étonner qu'elle soit vide au
-- début : c'est voulu, pas un bug de collecte.
--
-- Principe central : on garde un résumé pour TOUS les tokens (migrés ou
-- non) — c'est le groupe témoin nécessaire pour comparer les deux
-- populations. Seul le détail (token_snapshots, quand il existera) est
-- purgé pour les non-migrés après la fenêtre d'observation.

create table if not exists tokens (
  mint text primary key,
  symbol text,
  name text,
  creator text,
  created_at timestamptz not null default now(),

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
    case when migrated_at is not null
      then (extract(epoch from (migrated_at - created_at)))::int
    end
  ) stored,

  -- Fenêtre d'observation de 6h fermée par cleanup.js pour les non-migrés.
  -- NULL = encore dans sa fenêtre ou déjà migré (jamais fermée dans ce cas).
  observation_closed_at timestamptz,

  -- Assurance contre nos propres angles morts de schéma : l'événement brut
  -- complet, tel que reçu de PumpPortal. Voir README.
  raw_new_token_event jsonb,
  raw_migration_event jsonb,

  ingested_at timestamptz not null default now()
);

create index if not exists idx_tokens_migrated_created on tokens (migrated, created_at);
create index if not exists idx_tokens_observation_open on tokens (observation_closed_at) where observation_closed_at is null and migrated = false;

-- Prêt pour l'option B (snapshots périodiques) — vide en V1, voir plus haut.
create table if not exists token_snapshots (
  id bigint generated always as identity primary key,
  mint text not null references tokens(mint) on delete cascade,
  captured_at timestamptz not null default now(),
  age_seconds integer not null,
  market_cap_sol numeric,
  virtual_sol_reserves numeric,
  virtual_token_reserves numeric,
  raw_event jsonb
);

create index if not exists idx_snapshots_mint_time on token_snapshots (mint, captured_at);

-- Journal de connexion/déconnexion WS + relais entre runs GitHub Actions.
-- Sert à savoir, au moment de l'analyse, quelles périodes ont été
-- réellement écoutées (un "calme plat" peut être un vrai calme ou une
-- déconnexion silencieuse — sans ce journal on ne peut pas distinguer
-- les deux).
create table if not exists ingestion_log (
  id bigint generated always as identity primary key,
  event_type text not null, -- 'connected' | 'disconnected' | 'reconnect_attempt' | 'relay_handoff' | 'unknown_event' | 'cleanup_run'
  at timestamptz not null default now(),
  detail text
);

create index if not exists idx_ingestion_log_time on ingestion_log (at);

# pumpfun-migration-lab

Laboratoire de recherche, **sans capital et sans trading**, sur les tokens
Pump.fun/Solana. Hypothèse : plutôt que de rivaliser de vitesse avec les
bots, on étudie quels tokens survivent assez longtemps pour migrer (passer
de la bonding curve à un AMM), et si des caractéristiques disponibles dès
la création permettent de le prédire statistiquement.

Aucun ordre n'est jamais envoyé. Ce projet ne fait que lire un flux public
et écrire dans une base de données.

## Portée

- `subscribeNewToken` + `subscribeMigration` uniquement — **pas** de
  `subscribeTokenTrade`, pas de polling de prix en continu.
- Pour chaque token créé, une fenêtre d'observation de 6h : s'il migre
  dans cette fenêtre, on garde tout ; sinon, on ferme la fenêtre.
- Question V1 (répondue) : les caractéristiques statiques disponibles à
  la création (créateur, réserves initiales, métadonnées...) ne
  suffisaient pas seules à distinguer les tokens qui migrent — d'où la
  couche dynamique V2 ci-dessous.
- Question V2 (en cours) : **à T+5s, T+10s, T+20s ou T+30s, quelles
  caractéristiques de la trajectoire de la bonding curve distinguent déjà
  une migration progressive (groupe B) d'un token qui ne migrera pas
  (groupe C) ?**

## V2 — surveillance en cascade à gate d'activité

Les 4 snapshots fixes de la V1 (30s/60s/180s/300s) arrivaient trop tard :
la médiane de migration du groupe B est ~64s, et beaucoup de comptes
étaient déjà à 0/0 au premier snapshot. La V2 (2026-08-23) observe la
trajectoire beaucoup plus tôt, sans suivre tous les tokens à haute
fréquence par défaut :

1. **Gate universel** (tous les tokens) : lectures à T+2s / T+5s / T+10s.
2. Si au moins une de ces lectures dépasse le seuil de bruit calibré sur
   des données réelles (écart relatif > `1e-4` vs
   `initial_virtual_sol_reserves` — voir
   `scripts/calibrate-activity-threshold.js` ; le bruit flottant pur
   reste sous `1e-8`, la masse réelle démarre autour de `1e-4`) : cascade
   **étendue** T+20s / 30s / 45s / 60s.
3. Toujours actif à la fin de la cascade étendue : **longue traîne**
   espacée T+2min / 5min / 10min / 20min / 30min, arrêtée immédiatement
   dès qu'une migration est détectée en temps réel (`subscribeMigration`)
   ou qu'une lecture montre `complete=true` / réserve à 0.

Les **holders** (`src/holders.js`, ~20 appels RPC, lent — voir "Ce qui
n'est pas vérifié" plus bas) suivent le même gate : capturés une seule
fois, au premier point de la cascade étendue (T+20s) — donc seulement
pour les tokens jugés actifs, jamais inconditionnellement.

Tous les délais (`GATE_DELAYS_S`, `EXTENDED_DELAYS_S`,
`LONG_TAIL_DELAYS_S`) et le seuil (`ACTIVITY_REL_DEV_THRESHOLD`) sont
surchargeables par variable d'environnement — voir `src/listener.js`.

## Deux niveaux de conservation

Supprimer entièrement les tokens qui ne migrent pas détruirait le groupe
témoin nécessaire à la comparaison statistique. On garde donc :

- un **dataset permanent léger** (table `tokens`, jamais purgé) — pour
  **tous** les tokens, migrés ou non : créateur, état initial de la
  bonding curve, résultat migré/non migré, délai de migration (calculé
  par Postgres, pas par l'application — voir `sql/schema.sql`), et les
  métriques dérivées de la cascade (`bc_ratio_t5s/t10s/t20s/t30s`,
  `bc_first_active_at_s`, `bc_peak_ratio`, `bc_cascade_reads`), écrites
  **en direct** au fil de la cascade — jamais recalculées après coup à
  partir des snapshots bruts.
- un **détail temporaire** (table `token_snapshots`) — la trajectoire
  brute complète de la bonding curve, en fenêtre glissante
  (`SNAPSHOT_RETENTION_MS`, ~4 jours par défaut). Purgée par lots
  périodiques par le listener lui-même (`purgeOldSnapshots`) — ce n'est
  qu'un filet de sécurité temporaire, les métriques utiles ont déjà été
  recopiées sur `tokens.bc_*`.
- le **JSON brut** des deux événements (`raw_new_token_event` /
  `raw_migration_event`) est mis à `NULL` après `RAW_JSON_RETENTION_MS`
  (~7 jours par défaut, `purgeOldRawJson`) — fenêtre plus longue que les
  snapshots car il sert de rattrapage si un champ non prévu s'avère utile
  après coup, mais n'est pas gardé indéfiniment non plus.

```
                     TOUS LES TOKENS
                           │
             ┌─────────────┴─────────────┐
             ↓                           ↓
        NON MIGRÉS                    MIGRÉS
             │                           │
   résumé + bc_* conservés      résumé + bc_* conservés
   (pour toujours)              (pour toujours)
   snapshots bruts purgés       snapshots bruts purgés
   après ~4j, JSON brut         après ~4j, JSON brut
   après ~7j                    après ~7j
```

## Mise en place

1. **Supabase** : créer un projet gratuit sur supabase.com, puis coller le
   contenu de `sql/schema.sql` dans leur éditeur SQL et l'exécuter. Sur un
   projet déjà créé avant la V2 (tables `tokens`/`token_snapshots`
   existantes), le bloc `create table if not exists` ne touche pas aux
   tables déjà là : exécuter en plus la section "Migration V2" en bas du
   fichier (`alter table ... add column if not exists ...`, sûre à
   réexécuter).
2. **Secrets du repo** (Settings → Secrets and variables → Actions) :
   - `SUPABASE_URL` — l'URL du projet (`https://xxxx.supabase.co`)
   - `SUPABASE_SERVICE_KEY` — la clé **service_role** (pas la clé
     publique `anon`, elle seule peut écrire sans RLS configurée)
3. Déclencher manuellement le workflow **Listener** (Actions → Listener →
   Run workflow) pour démarrer la collecte.

Aucun secret n'est nécessaire en plus de ceux-là — le token utilisé pour
que le listener se relance lui-même est le `GITHUB_TOKEN` automatique
fourni par GitHub Actions à chaque run (voir plus bas).

## Pourquoi deux workflows

GitHub impose une limite dure de 6h par job — impossible de garder une
connexion WebSocket ouverte "24/7" dans un seul run. Architecture de
contournement, **expérimentale, pas définitive** :

- **`listener.yml`** (`workflow_dispatch` uniquement) tourne ~5h55, puis
  se ferme proprement et déclenche un nouveau run de lui-même via l'API
  GitHub Actions juste avant la limite. Chaîne continue avec un trou de
  reconnexion de l'ordre de quelques dizaines de secondes à chaque relais
  (le temps que le nouveau runner démarre) — les tokens créés pendant ce
  trou sont perdus, c'est un biais d'échantillonnage accepté et loggé
  (`ingestion_log`), pas éliminé.
- **`watchdog.yml`** (toutes les 20 min) vérifie qu'un run de `listener`
  est bien en cours ; sinon il en relance un. Filet de sécurité si la
  relance automatique échoue (crash avant d'y arriver, erreur réseau
  ponctuelle sur l'appel API).

Repo **public** délibérément : sur un repo privé, le budget Actions
gratuit (~2000 min/mois) serait épuisé en moins de deux jours pour un
écouteur qui tourne en continu. Sur un repo public, les minutes Actions
sont gratuites et illimitées. Aucun secret, clé, ou donnée personnelle
n'est committé dans le code — uniquement dans les secrets GitHub Actions.

Si l'expérience montre un intérêt, la collecte devra migrer vers une
infrastructure réellement persistante (un petit VPS, par exemple) plutôt
que de continuer sur ce montage en relais.

## Ce qui n'est pas vérifié

La forme exacte des messages PumpPortal (noms de champs, format) n'a pas
pu être vérifiée en direct pendant le développement (réseau sortant
restreint dans cet environnement). Le classement des événements
(`classifyEvent` dans `src/listener.js`) est écrit en best-effort à partir
de la documentation publique, avec repli heuristique si les champs
attendus sont absents. Un événement qu'on n'arrive pas à classer est
loggé (`ingestion_log`, `event_type='unknown_event'`) avec son JSON brut
plutôt que silencieusement perdu — à inspecter après le premier run réel
pour ajuster la classification si besoin.

Idem pour l'appel API qui relance le workflow (`triggerNextRun`) : la
capacité du `GITHUB_TOKEN` automatique à déclencher un `workflow_dispatch`
sur le même workflow est un usage documenté et courant, mais à confirmer
au premier relais réel plutôt qu'à supposer garanti.

## Tests

```
npm install
node test/integration.js
```

Fait tourner `src/listener.js` comme un vrai process contre un faux
serveur WebSocket et un faux serveur HTTP, tous deux locaux — aucun appel
vers pumpportal.fun ou supabase.co. Vérifie la connexion, l'abonnement,
la classification (y compris un événement inconnu), le buffering/flush,
la reconnexion après coupure, le déclenchement du relais de fin de run,
et la cascade V2 (gate + étendue + holders une seule fois, via des délais
raccourcis passés par variables d'environnement — voir le haut du
fichier).

## Explicitement hors scope pour l'instant

Pas de trading, pas de scoring, pas de modèle prédictif, pas de
`subscribeTokenTrade`. L'objectif de cette V1 est uniquement de
constituer un dataset propre — l'analyse (quelles variables diffèrent
significativement entre migrés et non-migrés, à partir de quel moment une
prédiction devient exploitable) vient après, une fois qu'il y a de la
matière.

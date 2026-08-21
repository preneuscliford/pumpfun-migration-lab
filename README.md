# pumpfun-migration-lab

Laboratoire de recherche, **sans capital et sans trading**, sur les tokens
Pump.fun/Solana. Hypothèse : plutôt que de rivaliser de vitesse avec les
bots, on étudie quels tokens survivent assez longtemps pour migrer (passer
de la bonding curve à un AMM), et si des caractéristiques disponibles dès
la création permettent de le prédire statistiquement.

Aucun ordre n'est jamais envoyé. Ce projet ne fait que lire un flux public
et écrire dans une base de données.

## Portée V1

- `subscribeNewToken` + `subscribeMigration` uniquement — **pas** de
  `subscribeTokenTrade`, pas de polling de prix en cours de fenêtre.
- Pour chaque token créé, une fenêtre d'observation de 6h : s'il migre
  dans cette fenêtre, on garde tout ; sinon, on ferme la fenêtre et on
  purge le détail (voir "Deux niveaux de conservation" plus bas).
- Question posée par cette V1 : **les caractéristiques statiques
  disponibles à la création (créateur, réserves initiales de la bonding
  curve, présence de métadonnées, forme du nom/symbole...) suffisent-elles
  déjà à distinguer les tokens qui migrent des autres ?** Si oui, on aura
  une raison d'ajouter une couche dynamique (option B, plus coûteuse) :
  quelques snapshots périodiques pendant la fenêtre. Si non, pas la peine
  de payer ce coût-là pour rien.

## Deux niveaux de conservation

Supprimer entièrement les tokens qui ne migrent pas détruirait le groupe
témoin nécessaire à la comparaison statistique. On garde donc :

- un **résumé** (table `tokens`) — pour **tous** les tokens, migrés ou
  non : créateur, état initial de la bonding curve, résultat migré/non
  migré, délai de migration (calculé par Postgres, pas par l'application —
  voir `sql/schema.sql`), et le JSON brut des deux événements.
- un **détail** (table `token_snapshots`) — prévue pour l'option B
  (snapshots périodiques), **vide en V1** puisqu'on ne collecte pas encore
  cette couche. Ne pas s'inquiéter qu'elle soit vide au début : c'est le
  scope V1, pas un bug de collecte.

```
                     TOUS LES TOKENS
                           │
             ┌─────────────┴─────────────┐
             ↓                           ↓
        NON MIGRÉS                    MIGRÉS
             │                           │
       résumé conservé            résumé conservé
       snapshots supprimés        snapshots conservés
                                  (à partir de l'option B)
```

Le JSON brut (`raw_new_token_event`/`raw_migration_event`) est toujours
conservé : assurance contre un champ qu'on n'aurait pas pensé à extraire
au moment d'écrire le parseur — on pourra toujours revenir le chercher
plus tard sans avoir eu besoin de le prévoir à l'avance.

## Mise en place

1. **Supabase** : créer un projet gratuit sur supabase.com, puis coller le
   contenu de `sql/schema.sql` dans leur éditeur SQL et l'exécuter.
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
la reconnexion après coupure, et le déclenchement du relais de fin de
run.

## Explicitement hors scope pour l'instant

Pas de trading, pas de scoring, pas de modèle prédictif, pas de
`subscribeTokenTrade`. L'objectif de cette V1 est uniquement de
constituer un dataset propre — l'analyse (quelles variables diffèrent
significativement entre migrés et non-migrés, à partir de quel moment une
prédiction devient exploitable) vient après, une fois qu'il y a de la
matière.

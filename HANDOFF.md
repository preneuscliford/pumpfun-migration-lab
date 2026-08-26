# Passation — pumpfun-migration-lab (2026-08-26)

Ce document résume l'état du projet pour une reprise en main par une
instance Claude Code locale (accès filesystem/shell/navigateur direct),
suite à une longue session de recherche menée depuis Claude Code sur le
web (sans navigateur, uniquement via GitHub Actions pour interroger
Supabase).

## Contexte du projet

Laboratoire de recherche : on observe des tokens pump.fun à la création
pour voir si la trajectoire de leur bonding curve (juste après création)
permet de distinguer, tôt, les tokens qui vont finir par migrer
(compléter leur bonding curve) de ceux qui ne migreront jamais.

**Contraintes du projet, à respecter aussi en local :**
- Aucun trading, aucune clé de wallet, aucun `subscribeTokenTrade` (WS
  PumpPortal). Le projet lit un flux public + quelques RPC Solana en
  lecture seule.
- Aucun score, seuil de décision ou stratégie n'a été construit à ce
  jour — uniquement des mesures et comparaisons de distributions. C'est
  une contrainte explicite et répétée de l'utilisateur tout au long de
  la session, pas une limite technique.
- Toujours valider hors ligne (données synthétiques) avant de déployer
  un changement au collecteur (`src/listener.js`).

## État actuel de l'architecture (déployée, stable)

- **Écouteur** (`src/listener.js`, tourne en continu via GitHub Actions,
  se relance automatiquement toutes les ~5h50) : `subscribeNewToken` +
  `subscribeMigration` PumpPortal, cascade de lectures RPC bonding curve
  à des délais croissants (gate 2/5/10s, étendue 20/30/45/60s si actif,
  longue traîne jusqu'à 30min si toujours actif).
- **File RPC bonding curve : espacement FIXE à 300ms** (`createRpcThrottle`).
  Un throttle adaptatif (accélère/ralentit selon les signaux de 429) a été
  tenté puis **abandonné** après avoir causé une vraie divergence de file
  (lectures perdues silencieusement, jusqu'à ~4h de retard réel avant
  qu'une auto-relance du process les abandonne). Le code adaptatif est
  conservé dans `listener.js` (post-mortem documenté juste au-dessus de
  `createAdaptiveBondingCurveThrottle`) mais **non branché** — ne pas le
  réactiver sans re-designer la garantie de débit minimum.
- **Holders (concentration des détenteurs) : cassé à 100%** (HTTP 429
  systématique du RPC public gratuit sur ces appels, ~20 requêtes par
  capture). File séparée de la bonding curve, budget quotidien limité,
  mais le problème lui-même (429 permanent) n'a jamais été creusé plus
  loin — reste ouvert si quelqu'un veut s'y attaquer (RPC payant ?
  fournisseur différent pour ces appels précisément ?).
- **Deux horloges de complétion, jamais confondues** :
  - `tokens.migrated_at` — notification `subscribeMigration` PumpPortal.
    Mesuré comme **non fiable en temps réel** (retard médian ~414s,
    jusqu'à ~35min observés).
  - `tokens.curve_completed_at` — posée par le listener dès qu'une
    lecture RPC montre elle-même `complete=true` ou réserves vidées.
    C'est la véritable source de vérité pour "quand la curve a fini".
  - `tokens.curve_completion_lag_seconds` (colonne calculée par
    Postgres) = migrated_at - curve_completed_at.
- **Instrumentation de timing** sur `token_snapshots` :
  `scheduled_at`/`queued_at`/`started_at`/`completed_at` (horodatages
  absolus) + `queue_wait_ms`/`rpc_call_ms` (durées dérivées). **Seules
  les lignes collectées depuis le 2026-08-25 ont `started_at` rempli** —
  avant cette date, seul `age_seconds` (âge visé, capturé AVANT
  l'attente en file RPC, donc potentiellement périmé) est disponible.
  Toujours préférer `started_at` quand il est présent pour calculer un
  âge réel de snapshot.

## Découverte principale de la session

**La séparation entre tokens qui migrent (B, curve complétée >10s après
création) et ceux qui ne migrent jamais dans la fenêtre observée (C) est
déjà nette dès la toute première lecture possible, T+2s après création**
— pas une émergence progressive.

Exemple (écart relatif `|Δ réserve| / réserve_initiale`, vSol, sur
échantillon élargi) :

| T+s | B : % avec variation >50% | C : % avec variation >50% |
|---|---|---|
| 2  | 53,8% | 7,2% |
| 30 | 78,9% | 17,4% |

Confirmé indépendamment sur `virtual_token_reserves` (même direction,
même ampleur), et résiste à l'exclusion des lectures proches de la
completion et des lectures à `queue_wait_ms` élevé.

**Explication trouvée en creusant plus loin (la partie la plus utile
pour la suite locale)** : le message WS `create` de PumpPortal contient
`solAmount` et `initialBuy` — le montant que le **créateur** met dans le
même mouvement que la création du token. Or `initial_virtual_sol_reserves`
stocké en base **inclut déjà cet achat** (`initial_virtual_sol_reserves
≈ 30 + solAmount`, vérifié presque exactement sur l'échantillon). Et les
tokens B ont un `solAmount` créateur ~9x plus élevé en médiane que les C
(2,67 SOL vs 0,30 SOL), ~29x au P75 (85,0 SOL vs 2,96 SOL).

**Donc le signal n'est pas "de l'activité organique apparaît en 2
secondes" — c'est que les tokens qui vont migrer ont, dès l'instant de
création (avant toute lecture RPC), un créateur qui a mis significativement
plus au pot.** C'est un signal disponible à t=0, dans le message WS
lui-même, sans latence RPC.

**Bémol honnête** : sur l'échantillon T+2s utilisé pour ce dernier
résultat, seule une minorité de lignes (~20%) avait `started_at` réel —
le reste retombe sur `age_seconds` (potentiellement périmé pour des
lignes anciennes). Ça n'affecte PAS la découverte solAmount/initialBuy
(elle vient du message de création, aucune lecture RPC impliquée), mais
tempère la précision fine des valeurs de réserves à "T+2s exactement"
pour une partie de l'échantillon.

## Ce que l'utilisateur demande maintenant

Reprendre la main en local (accès navigateur) pour **ouvrir de vrais
tokens B et C dans le navigateur** et observer qualitativement leur
comportement — valider "à l'œil" ce que les statistiques ont montré,
sur des exemples concrets plutôt que des distributions agrégées.

### Comment récupérer de vrais mints B et C

Toutes les requêtes de cette session sont passées par des scripts Node
lancés via GitHub Actions (cet environnement web n'a pas d'accès réseau
direct à Supabase). **En local, plus simple : appeler Supabase
directement** avec `SUPABASE_URL`/`SUPABASE_SERVICE_KEY` (à récupérer
auprès de l'utilisateur — ne jamais commiter ces valeurs).

Requêtes SQL prêtes à l'emploi (éditeur SQL Supabase, ou via le client
JS comme dans `scripts/*.js`) :

```sql
-- B : tokens dont la bonding curve a fini par se compléter, plus de 10s
-- après la création (le groupe "va migrer").
select mint, created_at, curve_completed_at, migrated_at,
       extract(epoch from (curve_completed_at - created_at)) as completion_gap_s,
       initial_virtual_sol_reserves, initial_virtual_token_reserves,
       raw_new_token_event->>'solAmount' as creator_sol_amount,
       raw_new_token_event->>'initialBuy' as creator_initial_buy
from tokens
where curve_completed_at is not null
  and extract(epoch from (curve_completed_at - created_at)) > 10
order by (raw_new_token_event->>'solAmount')::numeric desc nulls last
limit 20;

-- C : jamais observés comme complétés dans nos lectures (≠ "n'a jamais
-- migré" au sens absolu — voir limites plus bas).
select mint, created_at, migrated_at,
       initial_virtual_sol_reserves, initial_virtual_token_reserves,
       raw_new_token_event->>'solAmount' as creator_sol_amount
from tokens
where curve_completed_at is null
order by random()
limit 20;
```

Un mint peut s'ouvrir directement sur `https://pump.fun/coin/<mint>` (si
encore listé) ou sur un explorateur Solana (Solscan, Solana Explorer)
pour voir l'historique réel des transactions — ce que ce projet n'a
volontairement jamais collecté lui-même (pas de `subscribeTokenTrade`).

### Scripts existants utiles comme référence

Tous dans `scripts/`, lecture seule, tous validés hors ligne avant
déploiement — bon point de départ pour toute requête similaire en local
(mêmes patterns de pagination PostgREST à 1000 lignes, batching de
`.in('mint', ...)` à 150 mints max pour rester sous la limite d'URL) :

- `curve-completion-analysis.js` — classement A/B/C basé sur l'horloge RPC.
- `validate-bc-trajectory-gap.js` — validation par âge réel
  (`started_at`), exclusions de robustesse, vSol/vToken séparément.
- `analyze-bc-early-detection.js` — apparition du signal par checkpoint
  (2/5/10/20/30s), taux de dépassement de seuils descriptifs.
- `describe-bc-t2s-observations.js` — la découverte solAmount/initialBuy
  ci-dessus, classification via `tokens.curve_completed_at` (rapide, pas
  besoin de rebalayer tous les snapshots).

### Limites à garder en tête

- "C" = "notre RPC n'a jamais observé la curve comme terminée dans les
  lectures retenues" — pas une preuve que le token n'a jamais migré (la
  cascade peut s'être arrêtée avant, ou la fenêtre d'observation être
  encore trop courte pour les tokens récents).
- Échantillons B toujours petits en absolu (~60-1300 selon la fenêtre et
  le filtre), même si le signal est net.
- Aucune donnée de transaction/volume dans ce projet — c'est exactement
  ce qu'un coup d'œil navigateur sur un explorateur peut apporter que la
  base ne contient pas.

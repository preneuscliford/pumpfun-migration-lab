#!/usr/bin/env node
'use strict';

// Script de nettoyage UNE FOIS, demandé explicitement (2026-08-21) : les
// tokens ingérés avant l'activation de la capture de trajectoire de
// bonding curve (voir src/listener.js) n'auront jamais de snapshots — leur
// fenêtre 30s/1min/3min/5min est déjà passée, et la reconstruction
// historique via transactions est explicitement exclue du projet. Plutôt
// que de les garder comme un résidu incomplet dans la population, on les
// supprime entièrement (résumé création/migration inclus, pas seulement
// les snapshots — décision confirmée explicitement, malgré la perte de
// l'historique de délais déjà collecté sur ces ~1000 tokens).
//
// Destructif et volontairement à usage unique : n'est pas conçu pour être
// relancé. À supprimer du repo une fois exécuté avec succès.
//
// Critère : tokens.ingested_at < CUTOFF_ISO (pas created_at, qui peut être
// NULL pour un token dont la création a été manquée — ingested_at est
// toujours renseigné et capture correctement "collecté avant ce point").
//
// Garde-fou : exige CONFIRM=yes-delete-pretrajectory-tokens en plus des
// identifiants Supabase, pour éviter un déclenchement accidentel.
//
// Usage : CUTOFF_ISO=... CONFIRM=yes-delete-pretrajectory-tokens node scripts/purge-pretrajectory-tokens.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function main() {
  const cutoffIso = requireEnv('CUTOFF_ISO');
  const confirm = requireEnv('CONFIRM');
  if (confirm !== 'yes-delete-pretrajectory-tokens') {
    throw new Error("CONFIRM doit valoir exactement 'yes-delete-pretrajectory-tokens' — abandon par sécurité.");
  }
  if (Number.isNaN(Date.parse(cutoffIso))) {
    throw new Error(`CUTOFF_ISO invalide: ${cutoffIso}`);
  }

  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  console.log(`Seuil de suppression : ingested_at < ${cutoffIso}`);

  const { count: beforeCount, error: countError } = await supabase
    .from('tokens')
    .select('mint', { count: 'exact', head: true })
    .lt('ingested_at', cutoffIso);
  if (countError) throw new Error(`comptage avant suppression: ${countError.message}`);
  console.log(`Tokens concernés (ingested_at < seuil) : ${beforeCount}`);

  const { count: totalCount, error: totalError } = await supabase
    .from('tokens')
    .select('mint', { count: 'exact', head: true });
  if (totalError) throw new Error(`comptage total: ${totalError.message}`);
  console.log(`Total tokens avant suppression : ${totalCount}`);

  if (!beforeCount) {
    console.log('Rien à supprimer.');
    return;
  }

  const { error: deleteError, count: deletedCount } = await supabase
    .from('tokens')
    .delete({ count: 'exact' })
    .lt('ingested_at', cutoffIso);
  if (deleteError) throw new Error(`suppression: ${deleteError.message}`);
  console.log(`Lignes supprimées : ${deletedCount}`);

  const { count: afterTotal, error: afterError } = await supabase
    .from('tokens')
    .select('mint', { count: 'exact', head: true });
  if (afterError) throw new Error(`comptage après suppression: ${afterError.message}`);
  console.log(`Total tokens après suppression : ${afterTotal}`);
}

main().catch((err) => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});

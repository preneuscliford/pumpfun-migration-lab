#!/usr/bin/env node
'use strict';

// Diagnostic read-only : affiche la trajectoire réelle (vQuote/vToken par
// âge en secondes) de quelques tokens ayant déjà plusieurs snapshots, pour
// un premier coup d'œil visuel avant de coder la comparaison groupe B vs
// groupe C dans report.js. Ne touche à rien.
//
// Usage : node scripts/inspect-trajectories.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Variable d'environnement manquante: ${name}`);
  return v;
}

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), {
    auth: { persistSession: false },
  });

  const { data: snapshots, error } = await supabase
    .from('token_snapshots')
    .select('mint, age_seconds, virtual_sol_reserves, virtual_token_reserves, captured_at')
    .order('captured_at', { ascending: false })
    .limit(500);
  if (error) throw new Error(`lecture token_snapshots: ${error.message}`);
  console.log(`${snapshots.length} snapshot(s) inspecté(s) (les 500 plus récents).`);

  const byMint = new Map();
  for (const s of snapshots) {
    if (!byMint.has(s.mint)) byMint.set(s.mint, []);
    byMint.get(s.mint).push(s);
  }

  // Priorité aux tokens avec le plus de points de mesure (trajectoire la
  // plus complète à regarder).
  const mints = [...byMint.entries()].sort((a, b) => b[1].length - a[1].length).slice(0, 8);

  const { data: tokens, error: tokError } = await supabase
    .from('tokens')
    .select('mint, symbol, migrated, migrated_at, time_to_migration_seconds, created_at')
    .in('mint', mints.map(([mint]) => mint));
  if (tokError) throw new Error(`lecture tokens: ${tokError.message}`);
  const tokenByMint = new Map(tokens.map((t) => [t.mint, t]));

  console.log('\n' + '='.repeat(72));
  console.log(`Trajectoires des ${mints.length} tokens avec le plus de snapshots :`);
  console.log('='.repeat(72));

  for (const [mint, points] of mints) {
    const t = tokenByMint.get(mint);
    const sorted = points.slice().sort((a, b) => a.age_seconds - b.age_seconds);
    const label = t
      ? `${t.symbol || '(sans symbole)'} — migré=${t.migrated}${t.migrated ? ` (délai ${t.time_to_migration_seconds}s)` : ''}`
      : '(token non trouvé dans tokens)';
    console.log(`\n${mint} — ${label}`);
    for (const p of sorted) {
      console.log(`  age=${String(p.age_seconds).padStart(4)}s  vSol=${p.virtual_sol_reserves}  vToken=${p.virtual_token_reserves}`);
    }
  }

  const { count: totalSnapshots } = await supabase.from('token_snapshots').select('id', { count: 'exact', head: true });
  const { count: totalTokens } = await supabase.from('tokens').select('mint', { count: 'exact', head: true });
  console.log('\n' + '='.repeat(72));
  console.log(`Total : ${totalTokens} tokens, ${totalSnapshots} snapshots.`);
}

main().catch((err) => {
  console.error('Erreur fatale:', err.message);
  process.exit(1);
});

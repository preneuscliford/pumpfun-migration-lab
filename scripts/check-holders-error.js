#!/usr/bin/env node
'use strict';
const { createClient } = require('@supabase/supabase-js');
function requireEnv(n) { const v = process.env[n]; if (!v) throw new Error(`env manquant: ${n}`); return v; }

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });
  const { data, error } = await supabase
    .from('token_snapshots')
    .select('mint, nominal_delay_s, captured_at, holders_error, total_supply')
    .eq('nominal_delay_s', 20)
    .order('captured_at', { ascending: false })
    .limit(20);
  if (error) throw new Error(error.message);
  console.log(`Échantillon de ${data.length} lectures à T+20s (déclencheur holders) :`);
  for (const r of data) {
    console.log(`  ${r.mint} @${r.captured_at} total_supply=${r.total_supply} holders_error=${r.holders_error}`);
  }
  const { count: totalAt20, error: e1 } = await supabase.from('token_snapshots').select('*', { count: 'exact', head: true }).eq('nominal_delay_s', 20);
  if (e1) throw new Error(e1.message);
  const { count: withError, error: e2 } = await supabase.from('token_snapshots').select('*', { count: 'exact', head: true }).eq('nominal_delay_s', 20).not('holders_error', 'is', null);
  if (e2) throw new Error(e2.message);
  const { count: withSupply, error: e3 } = await supabase.from('token_snapshots').select('*', { count: 'exact', head: true }).eq('nominal_delay_s', 20).not('total_supply', 'is', null);
  if (e3) throw new Error(e3.message);
  console.log(`\nTotal lectures T+20s : ${totalAt20}`);
  console.log(`  avec holders_error non-null : ${withError}`);
  console.log(`  avec total_supply non-null (succès) : ${withSupply}`);
  console.log(`  ni erreur ni succès (holders jamais tenté ?) : ${totalAt20 - withError - withSupply}`);
}
main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

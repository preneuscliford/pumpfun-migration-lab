#!/usr/bin/env node
'use strict';

// Mesure ciblée (lecture seule), demandée le 2026-08-24, avant de toucher au
// throttle bonding curve : quantifier le taux de 429 RÉEL sur les lectures
// bonding curve (pas holders, qui reste à part). Sert de base chiffrée à la
// proposition de throttling adaptatif — ne construit rien, ne modifie rien.
//
// rpcCall() (src/bondingCurve.js) retente en interne sur 429 (backoff
// 500ms/1s/2s, 3 tentatives), donc un 429 qui finit par réussir n'apparaît
// JAMAIS comme erreur dans ingestion_log — seul un 429 qui survit aux 3
// retries devient un bonding_curve_snapshot_error. Pour capter aussi les
// 429 "absorbés" par le retry, on utilise rpc_call_ms comme indice indirect
// (un appel normal est ~30-110ms d'après measure-rpc-queue-delay.js ;
// un appel ayant attendu au moins un backoff de 500ms/1s/2s se voit dans
// des paliers autour de 500+/1500+/3500+ms). C'est une ESTIMATION par
// palier, explicitement annoncée comme telle — pas un comptage exact de
// tentatives, faute d'instrumentation par tentative aujourd'hui.
//
// Usage : node scripts/measure-bonding-curve-429-rate.js

const { createClient } = require('@supabase/supabase-js');

function requireEnv(n) {
  const v = process.env[n];
  if (!v) throw new Error(`env manquant: ${n}`);
  return v;
}

const PAGE_SIZE = 1000;
async function fetchAllRows(supabase, table, select, orderColumn, applyFilter) {
  const rows = [];
  let from = 0;
  for (;;) {
    let q = supabase.from(table).select(select).order(orderColumn, { ascending: true }).range(from, from + PAGE_SIZE - 1);
    if (applyFilter) q = applyFilter(q);
    const { data, error } = await q;
    if (error) throw new Error(`${table}: ${error.message}`);
    rows.push(...data);
    if (data.length < PAGE_SIZE) break;
    from += PAGE_SIZE;
  }
  return rows;
}

const DEPLOY_AT = new Date(process.env.DEPLOY_AT || '2026-08-24T05:23:15Z');
const SCHEMA_FIXED_AT = new Date(process.env.SCHEMA_FIXED_AT || '2026-08-24T05:26:00Z');

async function main() {
  const supabase = createClient(requireEnv('SUPABASE_URL'), requireEnv('SUPABASE_SERVICE_KEY'), { auth: { persistSession: false } });

  // Échecs définitifs (429 ayant survécu aux 3 retries, ou autre erreur).
  const errors = await fetchAllRows(supabase, 'ingestion_log', 'at, detail', 'at', (q) =>
    q.eq('event_type', 'bonding_curve_snapshot_error').gte('at', DEPLOY_AT.toISOString())
  );
  const final429 = errors.filter((e) => /HTTP 429/.test(e.detail));
  const otherErrors = errors.length - final429.length;

  // Lectures réussies, avec rpc_call_ms (fiable seulement depuis le
  // rechargement du cache de schéma, voir SCHEMA_FIXED_AT).
  const successes = await fetchAllRows(supabase, 'token_snapshots', 'captured_at, rpc_call_ms', 'id', (q) =>
    q.gte('captured_at', SCHEMA_FIXED_AT.toISOString())
  );

  const bucket0 = successes.filter((s) => s.rpc_call_ms !== null && s.rpc_call_ms < 250).length;
  const bucket1 = successes.filter((s) => s.rpc_call_ms !== null && s.rpc_call_ms >= 250 && s.rpc_call_ms < 1300).length;
  const bucket2 = successes.filter((s) => s.rpc_call_ms !== null && s.rpc_call_ms >= 1300 && s.rpc_call_ms < 2900).length;
  const bucket3 = successes.filter((s) => s.rpc_call_ms !== null && s.rpc_call_ms >= 2900).length;
  const successesWithRetrySignature = bucket1 + bucket2 + bucket3;

  const totalAttempts = successes.length + errors.length;
  const estimated429Encounters = successesWithRetrySignature + final429.length;

  console.log('='.repeat(78));
  console.log(`TAUX DE 429 — bonding curve uniquement, depuis ${DEPLOY_AT.toISOString()}`);
  console.log('='.repeat(78));
  console.log(`  Tentatives totales (succès + échecs)         : ${totalAttempts}`);
  console.log(`    ... succès (token_snapshots)                : ${successes.length}`);
  console.log(`    ... échecs définitifs (ingestion_log)        : ${errors.length}`);
  console.log(`        dont "HTTP 429" (429 survivant aux 3 retries) : ${final429.length}  (${errors.length ? ((final429.length / errors.length) * 100).toFixed(1) : 'n/a'}% des échecs)`);
  console.log(`        dont autre cause                             : ${otherErrors}`);
  console.log('');
  console.log(`  Taux d'échec DÉFINITIF (429 non rattrapé) / tentatives totales : ${totalAttempts ? ((final429.length / totalAttempts) * 100).toFixed(2) : 'n/a'}%`);
  console.log('');
  console.log(`  ESTIMATION par palier de rpc_call_ms (succès, n=${successes.length}, depuis ${SCHEMA_FIXED_AT.toISOString()}) :`);
  console.log(`    <250ms   (0 retry probable)          : ${bucket0}  (${successes.length ? ((bucket0 / successes.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`    250-1300ms (≥1 retry probable)       : ${bucket1}  (${successes.length ? ((bucket1 / successes.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`    1300-2900ms (≥2 retries probables)   : ${bucket2}  (${successes.length ? ((bucket2 / successes.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log(`    >=2900ms (3 retries, dernier essai OK): ${bucket3}  (${successes.length ? ((bucket3 / successes.length) * 100).toFixed(1) : 'n/a'}%)`);
  console.log('');
  console.log(`  ESTIMATION taux de 429 total (rattrapés + définitifs) / tentatives totales : ${totalAttempts ? ((estimated429Encounters / totalAttempts) * 100).toFixed(2) : 'n/a'}%`);
  console.log('='.repeat(78));
  console.log('Limites : les paliers rpc_call_ms sont un indice indirect (latence réseau');
  console.log('normale variable, pas de comptage exact de tentatives) — traiter comme');
  console.log('ordre de grandeur, pas comme un chiffre exact tant qu\'aucune instrumentation');
  console.log('par tentative n\'existe.');
}

main().catch((e) => { console.error('Erreur:', e.message); process.exit(1); });

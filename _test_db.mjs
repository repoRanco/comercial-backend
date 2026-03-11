import pg from 'pg'
const { Pool } = pg
const p = new Pool({
  host: '192.168.1.106', port: 5432,
  database: 'etl_comercial', user: 'postgres', password: 'f1ca030e',
  connectionTimeoutMillis: 5000,
})
try {
  const r = await p.query('SELECT NOW()')
  console.log('✅ PostgreSQL OK:', r.rows[0].now)
} catch(e) {
  console.error('❌ ERROR:', e.message)
} finally {
  await p.end()
}

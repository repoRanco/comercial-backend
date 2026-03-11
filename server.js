// server.js
import express from 'express'
import cors from 'cors'
import fetch from 'node-fetch'
import JSONStream from 'JSONStream'
import multer   from 'multer'
import { spawn } from 'child_process'
import path     from 'path'
import fs       from 'fs'
import { fileURLToPath } from 'url'
import pg from 'pg'
import XLSX from 'xlsx'

const { Pool } = pg

const app = express()
const PORT = process.env.PORT || 8081

// ─── PostgreSQL ───────────────────────────────────────────────────────────────
const pool = new Pool({
  host:     process.env.PG_HOST     || '192.168.1.106',
  port:     parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DB       || 'etl_comercial',
  user:     process.env.PG_USER     || 'postgres',
  password: process.env.PG_PASS     || 'f1ca030e',
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
})

async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.etl_transformaciones (
        id              SERIAL PRIMARY KEY,
        cliente         VARCHAR(100)  NOT NULL,
        modo            VARCHAR(10)   NOT NULL,
        nombre_archivo  VARCHAR(500)  NOT NULL,
        fecha_proceso   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        filas_precios   INTEGER       NOT NULL DEFAULT 0,
        filas_gastos    INTEGER       NOT NULL DEFAULT 0,
        archivo_excel   BYTEA         NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_etl_cliente_modo ON public.etl_transformaciones (cliente, modo);
      CREATE INDEX IF NOT EXISTS idx_etl_fecha ON public.etl_transformaciones (fecha_proceso DESC);
    `)
    console.log('✅ Tabla etl_transformaciones lista')
  } catch (e) {
    console.error('❌ Error inicializando BD:', e.message)
  }
}

// ─── Upstream / Cache ─────────────────────────────────────────────────────────
const UPSTREAM_URL = 'https://apicloudfruit.ranco.cl/produccion/cb/CB_GenerarDataReportes/InformeGestionCuentasCorrientes/20169/%20/%20/%20/json/10112/0'
const UP_USER = process.env.UP_USER || 'Pdiaz@ranco.cl'
const UP_PASS = process.env.UP_PASS || 'Pato2023'
const UPSTREAM_TIMEOUT_MS = 900_000
const CACHE_TTL_MS = 24 * 60 * 60 * 1000

const CAMPOS = [
  'IdFactEmbarque', 'ClaveEmbarque', 'EstadoLiquidacion',
  'CodigoRecibidor', 'Recibidor', 'CodigoTipoNave', 'TipoNave',
  'IdEspecie', 'CodigoEspecie', 'Especie', 'Moneda',
  'FOBChileAjustado', 'TotalFOBChile', 'Contenedor',
  'TiCaCursoLegal', 'FechaLiquidacion', 'FechaDespacho',
  'Naviera', 'Destino', 'CodigoTemporada',
]

const __dirname = path.dirname(fileURLToPath(import.meta.url))

app.use(cors({
  origin: ['http://localhost:5173', 'http://localhost:3000'],
  methods: ['GET', 'POST', 'DELETE'],
}))
app.use(express.json({ limit: '50mb' }))

// ─── Cache ────────────────────────────────────────────────────────────────────
const cache = new Map()
let isDownloading = false
let downloadPromise = null
let lastUpdated = null

function getCache(key) {
  const v = cache.get(key)
  if (!v) return null
  if (Date.now() > v.expiresAt) { cache.delete(key); return null }
  return v.value
}
function setCache(key, value) {
  cache.set(key, { expiresAt: Date.now() + CACHE_TTL_MS, value })
}
function clearAllCache() {
  cache.clear()
  console.log('🗑️  Cache limpiado completamente')
}

// ─── Progreso SSE ─────────────────────────────────────────────────────────────
let currentProgress = { count: 0, mb: 0, status: 'idle', lastUpdated: null }

app.get('/api/progress', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream')
  res.setHeader('Cache-Control', 'no-cache')
  res.setHeader('Connection', 'keep-alive')
  const sendProgress = () => res.write(`data: ${JSON.stringify(currentProgress)}\n\n`)
  sendProgress()
  const interval = setInterval(sendProgress, 1000)
  req.on('close', () => clearInterval(interval))
})

// ─── fetchUpstream ────────────────────────────────────────────────────────────
async function fetchUpstream() {
  const cached = getCache('upstream:slim')
  if (cached) return cached
  if (isDownloading) { console.log('⏳ Ya hay una descarga en curso, esperando...'); return downloadPromise }

  isDownloading = true
  downloadPromise = (async () => {
    currentProgress = { count: 0, mb: 0, status: 'descargando', lastUpdated: null }
    const auth = Buffer.from(`${UP_USER}:${UP_PASS}`).toString('base64')
    const startedAt = Date.now()
    console.log('🔄 Iniciando descarga desde Ranco Cloud...')
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT_MS)
    try {
      const res = await fetch(UPSTREAM_URL, {
        headers: { Authorization: `Basic ${auth}`, Accept: 'application/json' },
        signal: controller.signal,
      })
      if (!res.ok) throw new Error(`Upstream HTTP ${res.status}`)
      const rows = await new Promise((resolve, reject) => {
        const collected = []
        let totalBytes = 0
        const jsonStream = JSONStream.parse('Data.*')
        jsonStream.on('data', (raw) => {
          const obj = {}
          for (const f of CAMPOS) { if (raw[f] !== undefined) obj[f] = raw[f] }
          collected.push(obj)
          currentProgress.count++
          if (currentProgress.count % 10000 === 0) {
            currentProgress.mb = Math.round(totalBytes / 1024 / 1024)
            console.log(`📥 Progreso: ${currentProgress.count} filas | ${currentProgress.mb} MB`)
          }
        })
        jsonStream.on('end', () => {
          const tookMs = Date.now() - startedAt
          lastUpdated = new Date().toISOString()
          console.log(`✅ Descarga finalizada: ${currentProgress.count} filas en ${(tookMs / 1000).toFixed(1)}s`)
          currentProgress.status = 'completado'
          currentProgress.lastUpdated = lastUpdated
          resolve(collected)
        })
        jsonStream.on('error', reject)
        res.body.on('data', (chunk) => { totalBytes += chunk.length; jsonStream.write(chunk) })
        res.body.on('end', () => jsonStream.end())
        res.body.on('error', reject)
      })
      setCache('upstream:slim', rows)
      return rows
    } catch (e) {
      currentProgress.status = 'error'
      console.error('❌ Error en descarga:', e.message)
      throw e
    } finally {
      clearTimeout(timer)
      isDownloading = false
      downloadPromise = null
    }
  })()
  return downloadPromise
}

// ─── Normalización ────────────────────────────────────────────────────────────
function normalizeRow(r) {
  return {
    idFactEmbarque: r.IdFactEmbarque,
    claveEmbarque: r.ClaveEmbarque || '',
    estado: r.EstadoLiquidacion || '',
    recibidor: r.CodigoRecibidor || '',
    recibidorNombre: r.Recibidor || '',
    tipoNave: r.CodigoTipoNave || '',
    tipoNaveNombre: r.TipoNave || '',
    especie: r.CodigoEspecie || '',
    especieNombre: r.Especie || '',
    moneda: r.Moneda || '',
    totalFobNum: Number(r.FOBChileAjustado ?? r.TotalFOBChile ?? 0),
    contenedor: r.Contenedor || '',
    tipoCambio: r.TiCaCursoLegal ?? null,
    fechaLiquidacion: r.FechaLiquidacion ?? null,
    fechaDespacho: r.FechaDespacho ?? null,
    naviera: r.Naviera || '',
    destino: r.Destino || '',
    temporada: r.CodigoTemporada || '',
  }
}

function aggregateByEmbarque(rows) {
  const map = new Map()
  for (const r of rows) {
    const norm = normalizeRow(r)
    if (!norm.claveEmbarque) continue
    if (!map.has(norm.claveEmbarque)) map.set(norm.claveEmbarque, { ...norm, totalFobNum: 0 })
    const entry = map.get(norm.claveEmbarque)
    entry.totalFobNum += Number.isFinite(norm.totalFobNum) ? norm.totalFobNum : 0
  }
  return Array.from(map.values()).map(x => ({
    ...x,
    id: x.claveEmbarque,
    totalFob: x.totalFobNum.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
  })).sort((a, b) => a.claveEmbarque.localeCompare(b.claveEmbarque))
}

// ─── Rutas API Liquidaciones ──────────────────────────────────────────────────
app.get('/api/liquidaciones', async (req, res) => {
  const { recibidor = '', claveEmbarque = '', estado = '', especie = '', tipoNave = '' } = req.query
  const cacheKey = `liq:${recibidor}|${claveEmbarque}|${estado}|${especie}|${tipoNave}`
  const cached = getCache(cacheKey)
  if (cached) return res.json({ data: cached, cached: true, lastUpdated })
  try {
    const allRows = await fetchUpstream()
    const fRec = String(recibidor).trim().toUpperCase()
    const fCla = String(claveEmbarque).trim().toUpperCase()
    const fEst = String(estado).trim().toUpperCase()
    const fEsp = String(especie).trim().toUpperCase()
    const fNav = String(tipoNave).trim().toUpperCase()
    const filtered = allRows.filter(r => {
      const rr = String(r?.CodigoRecibidor || '').toUpperCase()
      const cc = String(r?.ClaveEmbarque || '').toUpperCase()
      const ee = String(r?.EstadoLiquidacion || '').toUpperCase()
      const es = String(r?.CodigoEspecie || '').toUpperCase()
      const tn = String(r?.CodigoTipoNave || '').toUpperCase()
      return ((!fRec || rr.includes(fRec)) && (!fCla || cc.includes(fCla)) && (!fEst || ee.includes(fEst)) && (!fEsp || es.includes(fEsp)) && (!fNav || tn.includes(fNav)))
    })
    const data = aggregateByEmbarque(filtered)
    setCache(cacheKey, data)
    res.json({ data, total: data.length, cached: false, lastUpdated })
  } catch (e) {
    res.status(502).json({ error: 'Error al obtener datos de Ranco Cloud' })
  }
})

app.get('/api/liquidaciones/opciones', async (_req, res) => {
  const cached = getCache('opciones')
  if (cached) return res.json({ ...cached, lastUpdated })
  try {
    const allRows = await fetchUpstream()
    const result = {
      recibidores: [...new Set(allRows.map(r => r.CodigoRecibidor).filter(Boolean))].sort(),
      estados: [...new Set(allRows.map(r => r.EstadoLiquidacion).filter(Boolean))].sort(),
      claves: [...new Set(allRows.map(r => r.ClaveEmbarque).filter(Boolean))].sort(),
      especies: [...new Set(allRows.map(r => r.CodigoEspecie).filter(Boolean))].sort(),
      tipoNaves: [...new Set(allRows.map(r => r.CodigoTipoNave).filter(Boolean))].sort(),
    }
    setCache('opciones', result)
    res.json({ ...result, lastUpdated })
  } catch (e) {
    res.status(502).json({ error: 'Error al obtener opciones' })
  }
})

app.get('/api/liquidaciones/estado', (_req, res) => {
  res.json({ isDownloading, lastUpdated, cacheActivo: cache.has('upstream:slim'), progreso: currentProgress })
})

app.post('/api/liquidaciones/refresh', (req, res) => {
  if (isDownloading) return res.json({ message: 'Ya hay una descarga en curso', isDownloading: true, lastUpdated })
  clearAllCache()
  fetchUpstream().catch(e => console.error('❌ Error en refresh manual:', e.message))
  res.json({ message: 'Actualización iniciada. Los datos estarán disponibles en ~10 minutos.', isDownloading: true, lastUpdated })
})

// ─── ETL ──────────────────────────────────────────────────────────────────────
const upload = multer({ dest: path.join(__dirname, 'tmp_uploads') })

const SCRIPTS = {
  'M:Qupai': '001-M-Qupai.py',        'A:Qupai': '001-A-Qupai.py',
  'M:Sanyong': '002-M-Sanyong.py',    'A:Sanyong': '002-A-Sanyong.py',
  'M:RiverKing': '003-M-RiverKing.py','A:RiverKing': '003-A-RiverKing.py',
  'M:Wonong': '004-M-Wonong.py',       'A:Wonong': '004-A-Wonong.py',
  'M:SunVirtue': '005-M-SunVirtue.py','A:SunVirtue': '005-A-SunVirtue.py',
  'M:CCMax': '006-M-CCMax.py',         'A:CCMax': '006-A-CCMax.py',
  'M:Kingo': '007-M-Kingo.py',         'A:Kingo': '007-A-Kingo.py',
  'M:Xianfeng': '008-M-Xianfeng.py',  'A:Xianfeng': '008-A-Xianfeng.py',
  'M:HingLee': '009-M-HingLee.py',    'A:HingLee': '009-A-HingLee.py',
  'M:Qinguo': '010-M-Qinguo.py',      'A:Qinguo': '010-A-Qinguo.py',
  'M:FrutaCloud': '011-M-FrutaCloud.py','A:FrutaCloud': '011-A-FrutaCloud.py',
}

// Helper: ejecutar un script Python ETL y devolver { excelBuffer, filasPrecios, filasGastos }
async function runETL(scriptName, inputPath) {
  const scriptPath = path.join(__dirname, scriptName)
  const outputPath = inputPath + '_out.xlsx'
  let stdout = ''
  try {
    await new Promise((resolve, reject) => {
      const py = spawn('python', [scriptPath, inputPath, outputPath])
      let stderr = ''
      py.stdout.on('data', d => stdout += d.toString())
      py.stderr.on('data', d => stderr += d.toString())
      py.on('close', code => code !== 0 ? reject(new Error(stderr || 'Error en script Python')) : resolve())
    })
    let filasPrecios = 0
    for (const line of stdout.split('\n')) {
      const m = line.match(/^FILAS:(\d+)/)
      if (m) filasPrecios = parseInt(m[1])
    }
    const excelBuffer = fs.readFileSync(outputPath)
    return { excelBuffer, filasPrecios, filasGastos: 0 }
  } finally {
    if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath)
  }
}

// Helper: leer hojas de un Excel buffer y devolver JSON para preview
function excelToPreview(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer' })
  const sheets = {}
  for (const sheetName of wb.SheetNames) {
    const ws = wb.Sheets[sheetName]
    sheets[sheetName] = XLSX.utils.sheet_to_json(ws, { defval: null })
  }
  return sheets
}

// ─── POST /api/etl/preview ────────────────────────────────────────────────────
// Transforma 1 o más archivos y devuelve JSON con preview (NO guarda en BD)
app.post('/api/etl/preview', upload.array('archivos', 20), async (req, res) => {
  const files = req.files || []
  const { modo = '', cliente = '' } = req.body

  if (files.length === 0) return res.status(400).json({ error: 'No se recibieron archivos' })

  const scriptName = SCRIPTS[`${modo}:${cliente}`]
  if (!scriptName) {
    files.forEach(f => fs.existsSync(f.path) && fs.unlinkSync(f.path))
    return res.status(400).json({ error: 'Configuración ETL no válida' })
  }

  const resultados = []
  const errores = []

  for (const file of files) {
    try {
      const { excelBuffer, filasPrecios, filasGastos } = await runETL(scriptName, file.path)
      const preview = excelToPreview(excelBuffer)
      resultados.push({
        nombreOriginal: file.originalname,
        filasPrecios,
        filasGastos,
        preview,                                          // { Precios: [...], Gastos: [...] }
        excelBase64: excelBuffer.toString('base64'),      // para descarga/guardado posterior
        tamanoBytes: excelBuffer.length,
      })
    } catch (e) {
      errores.push({ nombreOriginal: file.originalname, error: e.message })
      console.error(`❌ ETL Error [${file.originalname}]:`, e.message)
    } finally {
      if (fs.existsSync(file.path)) fs.unlinkSync(file.path)
    }
  }

  res.json({ resultados, errores, modo, cliente })
})

// ─── POST /api/etl/verificar-duplicados ──────────────────────────────────────
// Verifica si alguno de los nombres de archivo ya existe en BD para cliente/modo
app.post('/api/etl/verificar-duplicados', async (req, res) => {
  const { nombres, modo, cliente } = req.body
  if (!Array.isArray(nombres) || nombres.length === 0) {
    return res.json({ duplicados: [] })
  }
  try {
    const result = await pool.query(
      `SELECT nombre_archivo, fecha_proceso, id
       FROM public.etl_transformaciones
       WHERE cliente = $1 AND modo = $2 AND nombre_archivo = ANY($3::text[])
       ORDER BY fecha_proceso DESC`,
      [cliente, modo, nombres]
    )
    // Agrupar por nombre_archivo → devolver el más reciente
    const map = new Map()
    for (const row of result.rows) {
      if (!map.has(row.nombre_archivo)) map.set(row.nombre_archivo, row)
    }
    res.json({ duplicados: Array.from(map.values()) })
  } catch (e) {
    console.error('❌ Error verificando duplicados:', e.message)
    res.status(500).json({ error: e.message })
  }
})

// ─── POST /api/etl/confirmar ──────────────────────────────────────────────────
// Guarda en BD los archivos que el usuario validó (recibe base64)
app.post('/api/etl/confirmar', async (req, res) => {
  const { archivos, modo, cliente } = req.body
  // archivos: [{ nombreOriginal, excelBase64, filasPrecios, filasGastos, forzar }]

  if (!Array.isArray(archivos) || archivos.length === 0) {
    return res.status(400).json({ error: 'No hay archivos para confirmar' })
  }

  const guardados = []
  const errores = []
  const duplicados = []

  for (const arch of archivos) {
    try {
      // Verificar duplicado si no se forzó
      if (!arch.forzar) {
        const dup = await pool.query(
          `SELECT id FROM public.etl_transformaciones
           WHERE cliente = $1 AND modo = $2 AND nombre_archivo = $3
           LIMIT 1`,
          [cliente, modo, arch.nombreOriginal]
        )
        if (dup.rows.length > 0) {
          duplicados.push({ nombreOriginal: arch.nombreOriginal, idExistente: dup.rows[0].id })
          continue
        }
      }

      const buffer = Buffer.from(arch.excelBase64, 'base64')
      const result = await pool.query(
        `INSERT INTO public.etl_transformaciones
           (cliente, modo, nombre_archivo, filas_precios, filas_gastos, archivo_excel)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id`,
        [cliente, modo, arch.nombreOriginal, arch.filasPrecios || 0, arch.filasGastos || 0, buffer]
      )
      guardados.push({ id: result.rows[0].id, nombreOriginal: arch.nombreOriginal })
      console.log(`✅ Guardado en BD: id=${result.rows[0].id} [${arch.nombreOriginal}]`)
    } catch (e) {
      errores.push({ nombreOriginal: arch.nombreOriginal, error: e.message })
      console.error(`❌ Error guardando [${arch.nombreOriginal}]:`, e.message)
    }
  }

  res.json({ guardados, errores, duplicados })
})

// ─── POST /api/etl/transformar (legacy - descarga directa sin preview) ────────
app.post('/api/etl/transformar', upload.single('archivo'), async (req, res) => {
  const inputPath = req.file?.path
  const { modo = '', cliente = '' } = req.body
  const nombreOriginal = req.file?.originalname || `${cliente}_${modo}.xlsx`

  if (!inputPath) return res.status(400).json({ error: 'No se recibió archivo' })

  const scriptName = SCRIPTS[`${modo}:${cliente}`]
  if (!scriptName) {
    fs.unlinkSync(inputPath)
    return res.status(400).json({ error: 'Configuración ETL no válida' })
  }

  try {
    const { excelBuffer, filasPrecios, filasGastos } = await runETL(scriptName, inputPath)

    let registroId = null
    try {
      const result = await pool.query(
        `INSERT INTO public.etl_transformaciones
           (cliente, modo, nombre_archivo, filas_precios, filas_gastos, archivo_excel)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id`,
        [cliente, modo, nombreOriginal, filasPrecios, filasGastos, excelBuffer]
      )
      registroId = result.rows[0].id
      console.log(`✅ ETL guardado en BD: id=${registroId} cliente=${cliente} modo=${modo}`)
    } catch (dbErr) {
      console.error('⚠️  No se pudo guardar en BD:', dbErr.message)
    }

    const nombreDescarga = `${cliente}_${modo}_${Date.now()}.xlsx`
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    res.setHeader('Content-Disposition', `attachment; filename="${nombreDescarga}"`)
    if (registroId) res.setHeader('X-ETL-ID', String(registroId))
    res.send(excelBuffer)
  } catch (e) {
    console.error('❌ ETL Error:', e.message)
    res.status(500).json({ error: e.message })
  } finally {
    if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath)
  }
})

// ─── GET /api/etl/historial ───────────────────────────────────────────────────
app.get('/api/etl/historial', async (req, res) => {
  const { cliente = '', modo = '', limit = '50', offset = '0' } = req.query
  try {
    let where = 'WHERE 1=1'
    const params = []
    if (cliente) { params.push(cliente); where += ` AND cliente = $${params.length}` }
    if (modo)    { params.push(modo);    where += ` AND modo = $${params.length}` }
    params.push(parseInt(limit))
    params.push(parseInt(offset))

    const result = await pool.query(
      `SELECT id, cliente, modo, nombre_archivo, fecha_proceso, filas_precios, filas_gastos,
              length(archivo_excel) AS tamano_bytes
       FROM public.etl_transformaciones
       ${where}
       ORDER BY fecha_proceso DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params
    )
    const countResult = await pool.query(
      `SELECT COUNT(*) FROM public.etl_transformaciones ${where}`,
      params.slice(0, -2)
    )
    res.json({
      data: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset),
    })
  } catch (e) {
    console.error('❌ Error historial:', e.message)
    res.status(500).json({ error: 'Error al obtener historial' })
  }
})

// ─── GET /api/etl/historial/:id/descargar ────────────────────────────────────
app.get('/api/etl/historial/:id/descargar', async (req, res) => {
  const { id } = req.params
  try {
    const result = await pool.query(
      `SELECT cliente, modo, nombre_archivo, archivo_excel FROM public.etl_transformaciones WHERE id = $1`,
      [parseInt(id)]
    )
    if (result.rows.length === 0) return res.status(404).json({ error: 'Registro no encontrado' })
    const row = result.rows[0]
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    res.setHeader('Content-Disposition', `attachment; filename="${row.cliente}_${row.modo}_${row.nombre_archivo}"`)
    res.send(row.archivo_excel)
  } catch (e) {
    console.error('❌ Error descarga historial:', e.message)
    res.status(500).json({ error: 'Error al descargar archivo' })
  }
})

// ─── DELETE /api/etl/historial/:id ───────────────────────────────────────────
app.delete('/api/etl/historial/:id', async (req, res) => {
  const { id } = req.params
  try {
    const result = await pool.query(
      'DELETE FROM public.etl_transformaciones WHERE id = $1 RETURNING id',
      [parseInt(id)]
    )
    if (result.rows.length === 0) return res.status(404).json({ error: 'Registro no encontrado' })
    res.json({ ok: true, id: parseInt(id) })
  } catch (e) {
    console.error('❌ Error eliminando registro:', e.message)
    res.status(500).json({ error: 'Error al eliminar registro' })
  }
})

// ─── GET /api/etl/clientes ───────────────────────────────────────────────────
app.get('/api/etl/clientes', (_req, res) => {
  const clientes = [...new Set(Object.keys(SCRIPTS).map(k => k.split(':')[1]))].sort()
  res.json({ clientes })
})

// ─── Health ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({
  ok: true, ts: new Date().toISOString(),
  cacheActivo: cache.has('upstream:slim'), lastUpdated, isDownloading,
}))

// ─── Inicio ───────────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`✅ Backend en http://localhost:${PORT}`)
  console.log(`ℹ️  Cache: 24h | Timeout: ${UPSTREAM_TIMEOUT_MS / 1000}s`)
  await initDB()
})

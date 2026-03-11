-- ============================================================
-- Schema ETL Comercial
-- Base de datos: etl_comercial
-- Servidor: 192.168.1.106:5432
-- Usuario: postgres
-- ============================================================

-- Tabla principal de transformaciones ETL
CREATE TABLE IF NOT EXISTS public.etl_transformaciones (
    id              SERIAL PRIMARY KEY,
    cliente         VARCHAR(100)  NOT NULL,
    modo            VARCHAR(10)   NOT NULL,   -- 'M' = Marítimo, 'A' = Aéreo
    nombre_archivo  VARCHAR(500)  NOT NULL,
    fecha_proceso   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    filas_precios   INTEGER       NOT NULL DEFAULT 0,
    filas_gastos    INTEGER       NOT NULL DEFAULT 0,
    archivo_excel   BYTEA         NOT NULL    -- Excel transformado almacenado en BD
);

-- Índices para búsquedas frecuentes
CREATE INDEX IF NOT EXISTS idx_etl_cliente_modo ON public.etl_transformaciones (cliente, modo);
CREATE INDEX IF NOT EXISTS idx_etl_fecha        ON public.etl_transformaciones (fecha_proceso DESC);
CREATE INDEX IF NOT EXISTS idx_etl_cliente      ON public.etl_transformaciones (cliente);

-- ============================================================
-- Consultas útiles
-- ============================================================

-- Ver historial completo
-- SELECT id, cliente, modo, nombre_archivo, fecha_proceso, filas_precios,
--        pg_size_pretty(length(archivo_excel)::bigint) AS tamano
-- FROM public.etl_transformaciones
-- ORDER BY fecha_proceso DESC;

-- Descargar un archivo específico (desde psql o pgAdmin)
-- SELECT archivo_excel FROM public.etl_transformaciones WHERE id = 1;

-- Eliminar registros antiguos (más de 90 días)
-- DELETE FROM public.etl_transformaciones
-- WHERE fecha_proceso < NOW() - INTERVAL '90 days';

"""
001-M-Qupai.py
Transformación ETL - Marítimo - Qupai
Genera un .xlsx con 2 hojas: Precios y Gastos
Uso: python 001-M-Qupai.py <input.xlsx> <output.xlsx>
"""

import sys
import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# MAPEOS
# ============================================================================
MAPEO_COLUMNAS = {
    'Variety':         'Variedad',
    'Size':            'Calibre',
    'Brand':           'Marca',
    'PACKAGE':         'Envop',
    'Quantity':        'Cantidad',
    'Unit price(CNY)': 'PrecioUnitario'
}

# Mapa de costos → (CodigoItem, CodigoSubItem)
# CodigoItem 40 = gastos locales/varios, 41 = flete internacional
MAPEO_COSTOS = {
    "Tariff & VAT (CNY)":                   (40, "01"),
    "RE-SALE MARGIN (CNY)":                 (40, "02"),
    "Clearance charge & Port Charge (CNY)": (40, "04"),
    "Entrance fee":                         (40, "05"),
    "Entrance Fee":                         (40, "05"),
    "Cooling charge":                       (40, "06"),
    "Loading Fee":                          (40, "07"),
    "Container_Charges":                    (40, "08"),
    "Repack":                               (40, "09"),
    "Land Freight":                         (40, "10"),
    "Landing Freight":                      (40, "10"),
    "Inspection fee":                       (40, "11"),
    "International freight fee (CNY)":      (41, "01"),
}

# Todos los SubItems que deben aparecer aunque sean 0
SUBITEMS_40 = ["01","02","04","05","06","07","08","09","10","11"]
SUBITEMS_41 = ["01"]

PALABRAS_FIN_DATOS = ['Total sales amount', 'Charges', 'Total charge']
COLUMNAS_REQUERIDAS = ['Variedad', 'Calibre', 'Marca', 'Envop', 'Cantidad', 'PrecioUnitario']

# Mapeo de Variedad (nombre largo → código corto)
MAPEO_VARIEDAD = {
    'SANTINA':      'SN',
    'ROYAL DAWN':   'RD',
    'MEDA REX':     'MR',
    'SWEET ARYANA': 'SR',
    'NIMBA':        'NB',
    # Agrega más según necesites
}

# Mapeo de Envop (descripción → código)
MAPEO_ENVOP = {
    'CARTON2.5KG*2#': '5CAS2M',
    # Agrega más según necesites
}

# Etiqueta fija para este cliente
ETIQUETA = 'DL'
ESPECIE  = 'CE'
CATEGORIA = 'CAT1'

# ============================================================================
# UTILIDADES
# ============================================================================
def encontrar_fila_header(ruta: str) -> int:
    df_temp = pd.read_excel(ruta, header=None, nrows=10)
    for idx, row in df_temp.iterrows():
        row_str = ' '.join([str(c) for c in row if pd.notna(c)])
        if 'Variety' in row_str and 'Size' in row_str and 'Quantity' in row_str:
            logger.info(f"Header en fila {idx}")
            return idx
    logger.warning("Header no encontrado, usando fila 3")
    return 3


def buscar_valor_numerico(row: pd.Series, desde_col: int) -> float:
    for i in range(desde_col, len(row)):
        try:
            valor = row.iloc[i]
            if pd.isna(valor):
                continue
            if isinstance(valor, (int, float)) and valor >= 0:
                return float(valor)
            s = str(valor).replace('￥','').replace(',','').replace(' ','').strip()
            if s and s != 'nan' and not any(c.isalpha() for c in s.replace('.','').replace('-','')):
                v = float(s)
                if v >= 0:
                    return v
        except Exception:
            continue
    return 0.0


def limpiar_nombres_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str)
    mapping = {}
    for col in df.columns:
        for patron, std in MAPEO_COLUMNAS.items():
            if patron in str(col).strip():
                mapping[col] = std
                break
    df = df.rename(columns=mapping)
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes:
        raise Exception(f"Columnas faltantes: {faltantes}")
    return df


def validar_fila(row: pd.Series) -> bool:
    marca = str(row.get('Marca', ''))
    if any(p in marca for p in PALABRAS_FIN_DATOS):
        return False
    for col in ['Variedad', 'Envop', 'Cantidad', 'PrecioUnitario']:
        if pd.isna(row.get(col)):
            return False
    try:
        if float(row['Cantidad']) <= 0:
            return False
        if float(row['PrecioUnitario']) < 0:
            return False
    except Exception:
        return False
    return True


def codificar_variedad(nombre: str) -> str:
    nombre_upper = nombre.strip().upper()
    return MAPEO_VARIEDAD.get(nombre_upper, nombre_upper[:2])


def codificar_envop(descripcion: str) -> str:
    desc_upper = descripcion.strip().upper()
    return MAPEO_ENVOP.get(desc_upper, desc_upper[:6])


# ============================================================================
# EXTRACCIÓN DE COSTOS
# ============================================================================
def obtener_costos(ruta: str) -> dict:
    """Retorna dict: (CodigoItem, CodigoSubItem) → valor"""
    df = pd.read_excel(ruta, header=None)
    resultado = {}
    procesados = set()

    for _, row in df.iterrows():
        for ci in range(len(row)):
            cell = str(row.iloc[ci]).strip() if pd.notna(row.iloc[ci]) else ""
            if len(cell) < 3:
                continue
            for texto, (item, subitem) in MAPEO_COSTOS.items():
                if texto == cell or texto in cell:
                    val = buscar_valor_numerico(row, ci + 1)
                    key = f"{texto}_{val}"
                    if val > 0 and key not in procesados:
                        k = (item, subitem)
                        resultado[k] = resultado.get(k, 0.0) + val
                        procesados.add(key)
                        logger.info(f"✓ {texto}: {val} → Item {item} SubItem {subitem}")
                    break
    return resultado


# ============================================================================
# GENERACIÓN HOJA PRECIOS
# ============================================================================
def generar_precios(df_filas: list, total_cajas: int) -> pd.DataFrame:
    """
    Agrupa por (CodigoVariedad, CodigoCalibre) y calcula precio promedio ponderado.
    """
    df = pd.DataFrame(df_filas)

    # Agrupar: suma de cajas y suma de (precio * cajas) para promedio ponderado
    df['_monto'] = df['Cantidad'] * df['PrecioUnitario']
    grp = df.groupby(['CodigoVariedad', 'CodigoCalibre', 'CodigoEnvop'], sort=False).agg(
        Cajas=('Cantidad', 'sum'),
        _monto_total=('_monto', 'sum')
    ).reset_index()

    grp['PrecioLiq'] = grp['_monto_total'] / grp['Cajas']

    # Construir tabla final
    rows = []
    for i, r in grp.iterrows():
        rows.append({
            'Fila':            i + 1,
            'CodigEspecie':    ESPECIE,
            'CodigoVariedad':  r['CodigoVariedad'],
            'CodigoEnvop':     r['CodigoEnvop'],
            'CodigoCalibre':   r['CodigoCalibre'],
            'CodigoEtiqueta':  ETIQUETA,
            'CodigoCategoria': CATEGORIA,
            'Cajas':           int(r['Cajas']),
            'PrecioLiq':       round(r['PrecioLiq'], 7),
        })

    return pd.DataFrame(rows)


# ============================================================================
# GENERACIÓN HOJA GASTOS
# ============================================================================
def generar_gastos(costos: dict, total_cajas: int) -> pd.DataFrame:
    rows = []

    # Item 40 - todos los subitems
    for sub in SUBITEMS_40:
        val = costos.get((40, sub), 0.0)
        rows.append({
            'Especie':       ESPECIE,
            'CodigoItem':    40,
            'CodigoSubItem': sub,
            'Cajas':         total_cajas,
            'Valor':         round(val, 2),
        })

    # Item 41 - flete internacional
    for sub in SUBITEMS_41:
        val = costos.get((41, sub), 0.0)
        rows.append({
            'Especie':       ESPECIE,
            'CodigoItem':    41,
            'CodigoSubItem': sub,
            'Cajas':         total_cajas,
            'Valor':         round(val, 2),
        })

    return pd.DataFrame(rows)


# ============================================================================
# MAIN
# ============================================================================
def main():
    if len(sys.argv) < 3:
        print("Uso: python 001-M-Qupai.py <input.xlsx> <output.xlsx>", file=sys.stderr)
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2]

    if not os.path.exists(input_path):
        print(f"Archivo no encontrado: {input_path}", file=sys.stderr)
        sys.exit(1)

    logger.info(f"Procesando: {input_path}")

    # 1. Leer Excel y limpiar columnas
    fila_header = encontrar_fila_header(input_path)
    df = pd.read_excel(input_path, header=fila_header)
    df = limpiar_nombres_columnas(df)

    # 2. Filtrar y transformar filas válidas
    filas = []
    for _, row in df.iterrows():
        if pd.isna(row.get('Envop')) or str(row.get('Envop')).strip() == "":
            row = row.copy()
            row['Envop'] = 'Sin Envop'
        if validar_fila(row):
            filas.append({
                'CodigoVariedad': codificar_variedad(str(row['Variedad'])),
                'CodigoEnvop':    codificar_envop(str(row['Envop'])),
                'CodigoCalibre':  str(row['Calibre']).strip().upper(),
                'Cantidad':       float(row['Cantidad']),
                'PrecioUnitario': float(row['PrecioUnitario']),
            })

    if not filas:
        print("ERROR: No se encontraron filas válidas", file=sys.stderr)
        sys.exit(1)

    total_cajas = int(sum(f['Cantidad'] for f in filas))
    logger.info(f"Filas válidas: {len(filas)}, Total cajas: {total_cajas}")

    # 3. Extraer costos
    costos = obtener_costos(input_path)

    # 4. Generar hojas
    df_precios = generar_precios(filas, total_cajas)
    df_gastos  = generar_gastos(costos, total_cajas)

    # 5. Escribir Excel con 2 hojas
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_precios.to_excel(writer, sheet_name='Precios', index=False)
        df_gastos.to_excel(writer, sheet_name='Gastos',  index=False)

    logger.info(f"✅ Excel generado: {output_path}")
    logger.info(f"   Precios: {len(df_precios)} filas | Gastos: {len(df_gastos)} filas")

    # Stats para Node.js
    print(f"FILAS:{len(df_precios)}")
    print(f"COLUMNAS:{len(df_precios.columns)}")


if __name__ == "__main__":
    main()
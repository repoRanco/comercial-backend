"""
002-A-Sanyong.py
Transformación ETL - Aéreo - Sanyong
Genera un .xlsx con 2 hojas: Precios y Gastos
Uso: python 002-A-Sanyong.py <input.xlsx> <output.xlsx>
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
    '(Variety)': 'Variedad', '(Size)': 'Calibre', '(Description)': 'Marca',
    '( Remarks )': 'Envop', '(Quantity)': 'Cantidad', '(Average Price )': 'PrecioUnitario'
}

MAPEO_COSTOS = {
    "增值税/Vat": (40, "01"),
    "客户佣金/Customer's Commission (6% of SELLING PRICE)": (40, "02"),
    "物流服务费/Forwarding charge": (40, "04"),
    "海关查验（中检查验装卸）/Fee for Custom clearance process": (40, "04"),
    "代理费/agent charge": (40, "04"),
    "单证费/Documents charge": (40, "04"),
    "入场费/Enter charge": (40, "05"),
    "场地费/Place rent": (40, "05"),
    "打冷费/cooling charge": (40, "06"),
    "冷处理费/Cold treatment": (40, "06"),
    "冷库费/Cold storage charge": (40, "06"),
    "装卸费/handling charges": (40, "07"),
    "叉车费/forklift truck charge": (40, "07"),
    "海关查验/Customs inspection": (40, "11"),
    "转运费/Change freight(SH-CZ)": (40, "10"),
    "转运费/Change freight": (40, "10"),
    "押车费/Rider stooped fee": (40, "10"),
    "海运费用/Ocean freight": (41, "01"),
    "空运费用/Air freight": (41, "01"),
}

SUBITEMS_40 = ["01","02","04","05","06","07","08","09","10","11"]
SUBITEMS_41 = ["01"]
PALABRAS_FIN_DATOS = ['合计/TOTAL SALES AMOUNT']
COLUMNAS_REQUERIDAS = ['Variedad', 'Calibre', 'Marca', 'Envop', 'Cantidad', 'PrecioUnitario']
ETIQUETA = 'DL'; ESPECIE = 'CE'; CATEGORIA = 'CAT1'

# ============================================================================
# UTILIDADES
# ============================================================================
def encontrar_fila_header(ruta: str) -> int:
    df_temp = pd.read_excel(ruta, header=None, nrows=10)
    for idx, row in df_temp.iterrows():
        row_str = ' '.join([str(c) for c in row if pd.notna(c)])
        if 'Variety' in row_str and 'Size' in row_str and 'Quantity' in row_str:
            return idx
    return 3

def buscar_valor_numerico(row: pd.Series, desde_col: int) -> float:
    for i in range(desde_col, len(row)):
        try:
            valor = row.iloc[i]
            if pd.isna(valor): continue
            if isinstance(valor, (int, float)) and valor >= 0: return float(valor)
            s = str(valor).replace('￥','').replace(',','').replace(' ','').strip()
            if s and s != 'nan' and not any(c.isalpha() for c in s.replace('.','').replace('-','')):
                v = float(s)
                if v >= 0: return v
        except Exception: continue
    return 0.0

def limpiar_nombres_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str)
    mapping = {}
    for col in df.columns:
        for patron, std in MAPEO_COLUMNAS.items():
            if patron in str(col).strip():
                mapping[col] = std; break
    df = df.rename(columns=mapping)
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes: raise Exception(f"Columnas faltantes: {faltantes}")
    return df

def validar_fila(row: pd.Series) -> bool:
    if any(p in str(row.get('Marca','')) for p in PALABRAS_FIN_DATOS): return False
    for col in ['Variedad','Envop','Cantidad','PrecioUnitario']:
        if pd.isna(row.get(col)): return False
    try:
        if float(row['Cantidad']) <= 0: return False
        if float(row['PrecioUnitario']) < 0: return False
    except: return False
    return True

def obtener_costos(ruta: str) -> dict:
    df = pd.read_excel(ruta, header=None)
    resultado = {}; procesados = set()
    for _, row in df.iterrows():
        for ci in range(len(row)):
            cell = str(row.iloc[ci]).strip() if pd.notna(row.iloc[ci]) else ""
            if len(cell) < 3: continue
            for texto, (item, subitem) in MAPEO_COSTOS.items():
                if texto == cell or texto in cell:
                    val = buscar_valor_numerico(row, ci + 1)
                    key = f"{texto}_{val}"
                    if val > 0 and key not in procesados:
                        k = (item, subitem); resultado[k] = resultado.get(k, 0.0) + val; procesados.add(key)
                    break
    return resultado

def generar_precios(filas: list) -> pd.DataFrame:
    df = pd.DataFrame(filas); df['_monto'] = df['Cantidad'] * df['PrecioUnitario']
    grp = df.groupby(['CodigoVariedad','CodigoCalibre','CodigoEnvop'], sort=False).agg(
        Cajas=('Cantidad','sum'), _monto_total=('_monto','sum')).reset_index()
    grp['PrecioLiq'] = grp['_monto_total'] / grp['Cajas']
    return pd.DataFrame([{'Fila':i+1,'CodigEspecie':ESPECIE,'CodigoVariedad':r['CodigoVariedad'],
        'CodigoEnvop':r['CodigoEnvop'],'CodigoCalibre':r['CodigoCalibre'],'CodigoEtiqueta':ETIQUETA,
        'CodigoCategoria':CATEGORIA,'Cajas':int(r['Cajas']),'PrecioLiq':round(r['PrecioLiq'],7)}
        for i,r in grp.iterrows()])

def generar_gastos(costos: dict, total_cajas: int) -> pd.DataFrame:
    rows = []
    for sub in SUBITEMS_40:
        rows.append({'Especie':ESPECIE,'CodigoItem':40,'CodigoSubItem':sub,'Cajas':total_cajas,'Valor':round(costos.get((40,sub),0.0),2)})
    for sub in SUBITEMS_41:
        rows.append({'Especie':ESPECIE,'CodigoItem':41,'CodigoSubItem':sub,'Cajas':total_cajas,'Valor':round(costos.get((41,sub),0.0),2)})
    return pd.DataFrame(rows)

def main():
    if len(sys.argv) < 3: print("Uso: python 002-A-Sanyong.py <input.xlsx> <output.xlsx>", file=sys.stderr); sys.exit(1)
    input_path = sys.argv[1]; output_path = sys.argv[2]
    if not os.path.exists(input_path): print(f"Archivo no encontrado: {input_path}", file=sys.stderr); sys.exit(1)
    fila_header = encontrar_fila_header(input_path)
    df = pd.read_excel(input_path, header=fila_header)
    df = limpiar_nombres_columnas(df)
    filas = []
    for _, row in df.iterrows():
        if pd.isna(row.get('Envop')) or str(row.get('Envop')).strip() == "":
            row = row.copy(); row['Envop'] = 'Sin Envop'
        if validar_fila(row):
            filas.append({'CodigoVariedad':str(row['Variedad']).strip().upper(),
                'CodigoEnvop':str(row['Envop']).strip().upper(),'CodigoCalibre':str(row['Calibre']).strip().upper(),
                'Cantidad':float(row['Cantidad']),'PrecioUnitario':float(row['PrecioUnitario'])})
    if not filas: print("ERROR: No se encontraron filas válidas", file=sys.stderr); sys.exit(1)
    total_cajas = int(sum(f['Cantidad'] for f in filas))
    costos = obtener_costos(input_path)
    df_precios = generar_precios(filas); df_gastos = generar_gastos(costos, total_cajas)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_precios.to_excel(writer, sheet_name='Precios', index=False)
        df_gastos.to_excel(writer, sheet_name='Gastos', index=False)
    print(f"FILAS:{len(df_precios)}"); print(f"COLUMNAS:{len(df_precios.columns)}")

if __name__ == "__main__":
    main()

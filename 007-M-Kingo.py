"""007-M-Kingo.py - ETL Maritimo Kingo. Uso: python 007-M-Kingo.py <input.xlsx> <output.xlsx>"""
import sys,os,logging,pandas as pd
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s")
logger=logging.getLogger(__name__)

# Mapeo ampliado: clave=fragmento a buscar (case-insensitive), valor=nombre normalizado
MAPEO_COLUMNAS={
    # Variedad
    'variety':'Variedad','variedad':'Variedad','producto':'Variedad','product':'Variedad',
    'descripcion':'Variedad','description':'Variedad','item':'Variedad','fruta':'Variedad','fruit':'Variedad',
    # Calibre
    'size':'Calibre','calibre':'Calibre','talla':'Calibre','grade':'Calibre','count':'Calibre',
    # Marca
    'brand':'Marca','marca':'Marca','label':'Marca','etiqueta':'Marca',
    # Envop / Packing
    'packing':'Envop','envop':'Envop','envase':'Envop','pack':'Envop','packaging':'Envop','embalaje':'Envop','caja':'Envop','box':'Envop',
    # Cantidad
    'quantity':'Cantidad','cantidad':'Cantidad','qty':'Cantidad','cajas':'Cantidad','boxes':'Cantidad','units':'Cantidad','unidades':'Cantidad','ctns':'Cantidad','cartons':'Cantidad',
    # Precio unitario
    'unit price':'PrecioUnitario','unitprice':'PrecioUnitario','precio unitario':'PrecioUnitario','precio':'PrecioUnitario','price':'PrecioUnitario','unit cost':'PrecioUnitario','costo unitario':'PrecioUnitario','p.unit':'PrecioUnitario','p/unit':'PrecioUnitario',
}

MAPEO_COSTOS={
    "VAT":(40,"01"),"Import VAT":(40,"01"),"Commission":(40,"02"),"Service Charge":(40,"02"),
    "Forwarding Charges":(40,"04"),"Customs Clearance":(40,"04"),"Clearance Fee":(40,"04"),
    "Market Charges":(40,"05"),"Market Entry Fee":(40,"05"),"Entrance Fee":(40,"05"),
    "Fruit Storage":(40,"06"),"Cold Storage":(40,"06"),"Cooling Fee":(40,"06"),
    "Handling":(40,"07"),"Loading Fee":(40,"07"),"Container Charges":(40,"08"),
    "Other Charges":(40,"11"),"Other Expenses":(40,"11"),"Truck Charges":(40,"10"),"Trucking":(40,"10"),
    "Repack":(40,"09"),"Ocean Freight":(41,"01"),"Air Freight":(41,"01"),
}
SUBITEMS_40=["01","02","04","05","06","07","08","09","10","11"];SUBITEMS_41=["01"]
PALABRAS_FIN_DATOS=["TOTAL","Total","total"]
COLUMNAS_REQUERIDAS=["Variedad","Cantidad","PrecioUnitario"]  # Mínimas obligatorias
ETIQUETA="DL";ESPECIE="CE";CATEGORIA="CAT1"

def encontrar_fila_header(ruta):
    """Busca la fila que contiene los encabezados de columna."""
    df_temp=pd.read_excel(ruta,header=None,nrows=20)
    keywords=['variety','variedad','quantity','cantidad','price','precio','size','calibre','packing','envop','brand','marca','product','producto']
    for idx,row in df_temp.iterrows():
        s=" ".join([str(c).lower() for c in row if pd.notna(c)])
        hits=sum(1 for k in keywords if k in s)
        if hits>=2:return idx
    return 3

def buscar_valor_numerico(row,desde_col):
    for i in range(desde_col,len(row)):
        try:
            v=row.iloc[i]
            if pd.isna(v):continue
            if isinstance(v,(int,float)) and v>=0:return float(v)
            s=str(v).replace("\uff65","").replace("￥","").replace("¥","").replace("$","").replace(",","").replace(" ","").strip()
            if s and s!="nan" and not any(c.isalpha() for c in s.replace(".","").replace("-","")):
                f=float(s)
                if f>=0:return f
        except:continue
    return 0.0

def limpiar_nombres_columnas(df):
    """Mapea columnas usando coincidencia parcial case-insensitive."""
    df.columns=df.columns.astype(str)
    m={}
    for col in df.columns:
        col_lower=str(col).strip().lower()
        for patron,nombre in MAPEO_COLUMNAS.items():
            if patron in col_lower and nombre not in m.values():
                m[col]=nombre
                break
    df=df.rename(columns=m)
    # Verificar columnas mínimas obligatorias
    f=[c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if f:
        logger.error(f"Columnas disponibles en el Excel: {list(df.columns)}")
        raise Exception(f"Columnas faltantes: {f}. Columnas encontradas: {list(df.columns)}")
    # Rellenar opcionales con default
    if 'Calibre' not in df.columns:df['Calibre']='S/C'
    if 'Marca' not in df.columns:df['Marca']='S/M'
    if 'Envop' not in df.columns:df['Envop']='Sin Envop'
    return df

def validar_fila(row):
    marca=str(row.get("Marca",""))
    if any(p in marca for p in PALABRAS_FIN_DATOS):return False
    variedad=str(row.get("Variedad","")).strip()
    if not variedad or variedad.lower() in ['nan','none','']:return False
    for col in ["Cantidad","PrecioUnitario"]:
        if pd.isna(row.get(col)):return False
    try:
        if float(row["Cantidad"])<=0:return False
        if float(row["PrecioUnitario"])<0:return False
    except:return False
    return True

def obtener_costos(ruta):
    df=pd.read_excel(ruta,header=None);resultado={};procesados=set()
    for _,row in df.iterrows():
        for ci in range(len(row)):
            cell=str(row.iloc[ci]).strip() if pd.notna(row.iloc[ci]) else ""
            if len(cell)<3:continue
            for texto,(item,subitem) in MAPEO_COSTOS.items():
                if texto==cell or texto in cell:
                    val=buscar_valor_numerico(row,ci+1);key=f"{texto}_{val}"
                    if val>0 and key not in procesados:
                        k=(item,subitem);resultado[k]=resultado.get(k,0.0)+val;procesados.add(key)
                    break
    return resultado

def generar_precios(filas):
    df=pd.DataFrame(filas);df["_m"]=df["Cantidad"]*df["PrecioUnitario"]
    grp=df.groupby(["CodigoVariedad","CodigoCalibre","CodigoEnvop"],sort=False).agg(Cajas=("Cantidad","sum"),_mt=("_m","sum")).reset_index()
    grp["PrecioLiq"]=grp["_mt"]/grp["Cajas"]
    return pd.DataFrame([{"Fila":i+1,"CodigEspecie":ESPECIE,"CodigoVariedad":r["CodigoVariedad"],"CodigoEnvop":r["CodigoEnvop"],"CodigoCalibre":r["CodigoCalibre"],"CodigoEtiqueta":ETIQUETA,"CodigoCategoria":CATEGORIA,"Cajas":int(r["Cajas"]),"PrecioLiq":round(r["PrecioLiq"],7)} for i,r in grp.iterrows()])

def generar_gastos(costos,total_cajas):
    rows=[]
    for sub in SUBITEMS_40:rows.append({"Especie":ESPECIE,"CodigoItem":40,"CodigoSubItem":sub,"Cajas":total_cajas,"Valor":round(costos.get((40,sub),0.0),2)})
    for sub in SUBITEMS_41:rows.append({"Especie":ESPECIE,"CodigoItem":41,"CodigoSubItem":sub,"Cajas":total_cajas,"Valor":round(costos.get((41,sub),0.0),2)})
    return pd.DataFrame(rows)

def main():
    if len(sys.argv)<3:print("Uso: python 007-M-Kingo.py <input.xlsx> <output.xlsx>",file=sys.stderr);sys.exit(1)
    ip,op=sys.argv[1],sys.argv[2]
    if not os.path.exists(ip):print(f"No encontrado: {ip}",file=sys.stderr);sys.exit(1)
    df=pd.read_excel(ip,header=encontrar_fila_header(ip));df=limpiar_nombres_columnas(df)
    filas=[]
    for _,row in df.iterrows():
        if pd.isna(row.get("Envop")) or str(row.get("Envop")).strip()=="":row=row.copy();row["Envop"]="Sin Envop"
        if validar_fila(row):
            filas.append({
                "CodigoVariedad":str(row["Variedad"]).strip().upper(),
                "CodigoEnvop":str(row["Envop"]).strip().upper(),
                "CodigoCalibre":str(row["Calibre"]).strip().upper(),
                "Cantidad":float(row["Cantidad"]),
                "PrecioUnitario":float(row["PrecioUnitario"])
            })
    if not filas:print("ERROR: No se encontraron filas validas",file=sys.stderr);sys.exit(1)
    total_cajas=int(sum(f["Cantidad"] for f in filas));costos=obtener_costos(ip)
    df_p=generar_precios(filas);df_g=generar_gastos(costos,total_cajas)
    with pd.ExcelWriter(op,engine="openpyxl") as w:
        df_p.to_excel(w,sheet_name="Precios",index=False);df_g.to_excel(w,sheet_name="Gastos",index=False)
    print(f"FILAS:{len(df_p)}");print(f"COLUMNAS:{len(df_p.columns)}")
if __name__=="__main__":main()

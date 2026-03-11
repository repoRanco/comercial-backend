"""004-A-Wonong.py - ETL Aéreo Wonong. Uso: python 004-A-Wonong.py <input.xlsx> <output.xlsx>"""
import sys,os,logging,pandas as pd
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger=logging.getLogger(__name__)
MAPEO_COLUMNAS={'品种(Variety)':'Variedad','规格(Size)':'Calibre','品牌(Brand)':'Marca','净重(Net Weight)':'Envop','数量(Quantity)':'Cantidad','单价(Unit Price)':'PrecioUnitario'}
MAPEO_COSTOS={
    "C1税金/Import Duty":(40,"01"),"B 代销佣金/Commission(6%*A)":(40,"02"),
    "C4清关费/Customs Clearance Charge":(40,"04"),"C3运杂费/Freight And Miscellaneous Charges":(40,"04"),
    "进场费/Market Entry Fee(Market Charge ¥4500/Container)":(40,"05"),"进场费/Market Entry Fee":(40,"05"),
    "打冷费/Cooling Charge(Market Charge ¥200/Container/Day)":(40,"06"),"打冷费/Cooling Charge":(40,"06"),
    "装卸费/Stevedoring Charge(Market Charge ¥16/Pallet)":(40,"07"),"装卸费/Stevedoring Charge":(40,"07"),
    "吊柜费/Container Hoisting Charge(Market Charge ¥260/Container)":(40,"08"),
    "C6其他费用/Other":(40,"11"),"C6其他费用/Other Trucking":(40,"10"),
    "Repack":(40,"09"),"C2海运费/Ocean Freight":(41,"01"),"C2运费/Air Freight($6*G.W)":(41,"01"),
}
SUBITEMS_40=["01","02","04","05","06","07","08","09","10","11"];SUBITEMS_41=["01"]
PALABRAS_FIN_DATOS=['TOTAL'];COLUMNAS_REQUERIDAS=['Variedad','Calibre','Marca','Envop','Cantidad','PrecioUnitario']
ETIQUETA='DL';ESPECIE='CE';CATEGORIA='CAT1'
def encontrar_fila_header(ruta):
    df_temp=pd.read_excel(ruta,header=None,nrows=10)
    for idx,row in df_temp.iterrows():
        s=' '.join([str(c) for c in row if pd.notna(c)])
        if '品种(Variety)' in s and '规格(Size)' in s and '数量(Quantity)' in s:return idx
    return 3
def buscar_valor_numerico(row,desde_col):
    for i in range(desde_col,len(row)):
        try:
            v=row.iloc[i]
            if pd.isna(v):continue
            if isinstance(v,(int,float)) and v>=0:return float(v)
            s=str(v).replace('￥','').replace(',','').replace(' ','').strip()
            if s and s!='nan' and not any(c.isalpha() for c in s.replace('.','').replace('-','')):
                f=float(s)
                if f>=0:return f
        except:continue
    return 0.0
def limpiar_nombres_columnas(df):
    df.columns=df.columns.astype(str);m={}
    for col in df.columns:
        for p,s in MAPEO_COLUMNAS.items():
            if p in str(col).strip():m[col]=s;break
    df=df.rename(columns=m);f=[c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if f:raise Exception(f"Columnas faltantes: {f}")
    return df
def validar_fila(row):
    if any(p in str(row.get('Marca','')) for p in PALABRAS_FIN_DATOS):return False
    for col in ['Variedad','Envop','Cantidad','PrecioUnitario']:
        if pd.isna(row.get(col)):return False
    try:
        if float(row['Cantidad'])<=0:return False
        if float(row['PrecioUnitario'])<0:return False
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
    df=pd.DataFrame(filas);df['_m']=df['Cantidad']*df['PrecioUnitario']
    grp=df.groupby(['CodigoVariedad','CodigoCalibre','CodigoEnvop'],sort=False).agg(Cajas=('Cantidad','sum'),_mt=('_m','sum')).reset_index()
    grp['PrecioLiq']=grp['_mt']/grp['Cajas']
    return pd.DataFrame([{'Fila':i+1,'CodigEspecie':ESPECIE,'CodigoVariedad':r['CodigoVariedad'],'CodigoEnvop':r['CodigoEnvop'],'CodigoCalibre':r['CodigoCalibre'],'CodigoEtiqueta':ETIQUETA,'CodigoCategoria':CATEGORIA,'Cajas':int(r['Cajas']),'PrecioLiq':round(r['PrecioLiq'],7)} for i,r in grp.iterrows()])
def generar_gastos(costos,total_cajas):
    rows=[]
    for sub in SUBITEMS_40:rows.append({'Especie':ESPECIE,'CodigoItem':40,'CodigoSubItem':sub,'Cajas':total_cajas,'Valor':round(costos.get((40,sub),0.0),2)})
    for sub in SUBITEMS_41:rows.append({'Especie':ESPECIE,'CodigoItem':41,'CodigoSubItem':sub,'Cajas':total_cajas,'Valor':round(costos.get((41,sub),0.0),2)})
    return pd.DataFrame(rows)
def main():
    if len(sys.argv)<3:print("Uso: python 004-A-Wonong.py <input.xlsx> <output.xlsx>",file=sys.stderr);sys.exit(1)
    ip,op=sys.argv[1],sys.argv[2]
    if not os.path.exists(ip):print(f"No encontrado: {ip}",file=sys.stderr);sys.exit(1)
    df=pd.read_excel(ip,header=encontrar_fila_header(ip));df=limpiar_nombres_columnas(df)
    filas=[]
    for _,row in df.iterrows():
        if pd.isna(row.get('Envop')) or str(row.get('Envop')).strip()=="":row=row.copy();row['Envop']='Sin Envop'
        if validar_fila(row):filas.append({'CodigoVariedad':str(row['Variedad']).strip().upper(),'CodigoEnvop':str(row['Envop']).strip().upper(),'CodigoCalibre':str(row['Calibre']).strip().upper(),'Cantidad':float(row['Cantidad']),'PrecioUnitario':float(row['PrecioUnitario'])})
    if not filas:print("ERROR: No se encontraron filas válidas",file=sys.stderr);sys.exit(1)
    total_cajas=int(sum(f['Cantidad'] for f in filas));costos=obtener_costos(ip)
    df_p=generar_precios(filas);df_g=generar_gastos(costos,total_cajas)
    with pd.ExcelWriter(op,engine='openpyxl') as w:
        df_p.to_excel(w,sheet_name='Precios',index=False);df_g.to_excel(w,sheet_name='Gastos',index=False)
    print(f"FILAS:{len(df_p)}");print(f"COLUMNAS:{len(df_p.columns)}")
if __name__=="__main__":main()

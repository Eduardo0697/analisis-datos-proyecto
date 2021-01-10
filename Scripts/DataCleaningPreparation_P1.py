# Librerias utilizadas
# Tiempo de ejeución aproximado 1521.5017850399017

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import missingno as msno
import time



ARCHIVO_ENTRADA = "./DATASETS/GPKG/USERS_JOINED_TEST.gpkg"
ARCHIVO_SALIDA = "./DATASETS/USERS_UPDATE_TEST29DEC.gpkg"
SHOW_GRAPHS = False

# Empezamos a contar el tiempo
start = time.time()

# Conjunto de usuarios integrados
df_users = gpd.read_file(ARCHIVO_ENTRADA)
print('Conjunto de usuarios cargado', df_users.head())
print('Tamaño dataset', df_users.shape)
print('Informacion dataset')
df_users.info()

if(SHOW_GRAPHS):
    msno.matrix(df_users.sort_values(by=['giro','tarifa','rpu']))
    plt.title('Valores nulos en el dataset de usuarios previo a limpieza')
    plt.show()

# Lista de columnas incompletas
name_full_empty = [ df_users.columns[index] for index, value in enumerate(df_users.isna().sum()) if value == df_users.shape[0]  ]
# Eliminamos las columnas incompletas
df_users.drop(columns=name_full_empty, inplace=True)

print('....')

# Mapeamos y Remplazamos los valores nulos referentes a los frentes de manzana
values = {
    'MZ_FT_FECHA_CEU':df_users['MZ_FT_FECHA_CEU'].describe()['top'],
    'MZ_FT_PUESAMBU_' : 9,
    'MZ_FT_PUESAMBU_C'  : 'No especificado',
    'MZ_FT_PUESSEMI_' : 9,
    'MZ_FT_PUESSEMI_C' : 'No especificado',
    'MZ_FT_RAMPAS_' : 9,
    'MZ_FT_RAMPAS_C' : 'No especificado',
    'MZ_FT_ARBOLES_' : 9,
    'MZ_FT_ARBOLES_C' : 'No especificado',
    'MZ_FT_GUARNICI_' : 9,
    'MZ_FT_GUARNICI_C' : 'No especificado',
    'MZ_FT_BANQUETA_' : 9,
    'MZ_FT_BANQUETA_C' : 'No especificado',
    'MZ_FT_TELPUB_' : 9,
    'MZ_FT_TELPUB_C' : 'No especificado',
    'MZ_FT_ALUMPUB_' : 9,
    'MZ_FT_ALUMPUB_C' : 'No especificado',
    'MZ_FT_SENALIZA_' : 9,
    'MZ_FT_SENALIZA_C' : 'No especificado',
    'MZ_FT_RECUCALL_' : 9,
    'MZ_FT_RECUCALL_C' : 'No especificado',
    'MZ_FT_ACESOAUT_' : 9,
    'MZ_FT_ACESOAUT_C' : 'No especificado',
    'MZ_FT_ACESOPER_' : 9,
    'MZ_FT_ACESOPER_C' : 'No especificado',
    'MZ_FT_TIPOVIA_C' : 9,
}

df_users.fillna(value=values, inplace=True)

# Eliminamos las columnas que contienen el valor y preservamos la clave de los frentes de manzanas
# FT_COLS_USELESS = [
#     'MZ_FT_ACESOPER_C', 'MZ_FT_ACESOAUT_C', 'MZ_FT_RECUCALL_C', 'MZ_FT_SENALIZA_C', 'MZ_FT_ALUMPUB_C', 'MZ_FT_TELPUB_C',
#     'MZ_FT_BANQUETA_C', 'MZ_FT_GUARNICI_C', 'MZ_FT_ARBOLES_C', 'MZ_FT_RAMPAS_C', 'MZ_FT_PUESSEMI_C', 'MZ_FT_PUESAMBU_C',
#     'MZ_FT_FECHA_CEU'
#     ]
# df_users.drop(columns=FT_COLS_USELESS, inplace=True)


# Eliminamos las columnas que contienen el valor y preservamos la clave de las manzanas
MZ_COLS_USELESS = ['MZ_ENT', 'MZ_NOM_ENT', 'MZ_NOM_MUN',
                   'MZ_MUN','MZ_FECHA_POLI', 'MZ_FECHA_INF', 'MZ_FECHA_CEU']
                   
df_users.drop(columns=MZ_COLS_USELESS, inplace=True)


# Mapeamos y Remplazamos los valores nulos referentes a las manzanas
# El valor de 4 indica que no esta especificado. Esto solo para las caracteristicas de las vialidades.
values2 = {
    'MZ_ACESOPER_' : 4,
    'MZ_ACESOPER_C' : 'No especificado',
    'MZ_ACESOAUT_' : 4,
    'MZ_ACESOAUT_C' : 'No especificado',
    'MZ_RECUCALL_' : 4,
    'MZ_RECUCALL_C': 'No especificado',
    'MZ_SENALIZA_' : 4,
    'MZ_SENALIZA_C': 'No especificado',
    'MZ_ALUMPUB_' : 4,
    'MZ_ALUMPUB_C': 'No especificado',
    'MZ_TELPUB_' : 4,
    'MZ_TELPUB_C': 'No especificado',
    'MZ_BANQUETA_' : 4,
    'MZ_BANQUETA_C': 'No especificado',
    'MZ_GUARNICI_' : 4,
    'MZ_GUARNICI_C': 'No especificado',
    'MZ_ARBOLES_' : 4,
    'MZ_ARBOLES_C': 'No especificado',
    'MZ_RAMPAS_' : 4,
    'MZ_RAMPAS_C' : 'No especificado',
    'MZ_PUESSEMI_' : 4,
    'MZ_PUESSEMI_C': 'No especificado',
    'MZ_PUESAMBU_' : 4,
    'MZ_PUESAMBU_C': 'No especificado'
}
    
df_users.fillna(value=values2, inplace=True)


# Un problema que se ve es que los valores en las manzanas para la seccione de inventario de 
# viviendas (ver diccionario de datos), tiene errores en los datos, '*', estos valores los pasaremos a 0's 
# y posteriormente los vamos a corregir.

columns_inventario_viviendas = [
    'MZ_VIVTOT', 'MZ_TVIVHAB', 'MZ_P_TVIVHAB','MZ_PVIVPARHAB',  'MZ_TVIVPAR',
    'MZ_P_TVIVPAR', 'MZ_TVIVPARHAB', 'MZ_VIVPAR_DES', 'MZ_P_VIVPAR_D',
    'MZ_VIVPAR_UT', 'MZ_P_VIVPAR_U', 'MZ_VIVNOHAB', 'MZ_P_VIVNOHAB'
]
columns_caract_viviendas=[
    'MZ_VPH_PISODT', 'MZ_P_V_PISODT','MZ_VPH_C_ELEC', 'MZ_P_V_C_ELEC', 'MZ_VPH_AGUADV', 'MZ_P_V_AGUADV',
    'MZ_VPH_DRENAJ', 'MZ_P_V_DRENAJ', 'MZ_VPH_EXCUSA', 'MZ_P_V_EXCUSA', 'MZ_V_3MASOCUP', 'MZ_P_3MASOCUP',
    'MZ_PROOCUP_C'
]
columns_caract_poblacion = [
    'MZ_POBTOT', 'MZ_P0A14A', 'MZ_PP0A14A', 'MZ_P15A29A', 'MZ_PP15A29A', 'MZ_P30A59A', 'MZ_PP30A59A',
    'MZ_P_60YMAS', 'MZ_PP_60YMAS', 'MZ_PCON_LIM', 'MZ_PCON_LIM', 'MZ_PPCON_LIM', 'MZ_GRAPROES'
]
cols_selected= columns_inventario_viviendas + columns_caract_viviendas + columns_caract_poblacion
dict_replace_inst = { col : { '*' : 0 } for col in cols_selected}
df_users.replace(dict_replace_inst, inplace=True)

# Los valores que son nulos los pasamos a 0's
cols_to_fill_0 = columns_inventario_viviendas + columns_caract_viviendas + columns_caract_poblacion
values_dict = { col : 0 for col in cols_to_fill_0 }
df_users.fillna(value=values_dict, inplace=True)

# Columnas con valores nulos
cols_null_values = { list(df_users.columns)[index] : value for index,value in enumerate(df_users.isna().sum()) if value > 0}
cols_with_nulls = [ list(df_users.columns)[index] for index, value in enumerate(df_users.isna().sum()) if value > 0]
print('Columnas con valores nulos', cols_null_values)
print('Columnas con valores nulos', cols_with_nulls)


# Llenamos los campos faltantes de nombres de usuario
df_users.fillna(value={ 'nombreUsua' : 'Nombre no especificado'}, inplace=True)

# Elimnaos estas variables por conocimiento del negocio
df_users.drop(columns=['TAX', 'TAG'], inplace=True)
print('Tamaño dataset, 105: ',df_users.shape)

# Llenamos los campos faltantes de tipo de suministro con 0`s
df_users.fillna(value={ 'tipoSumini' : 0}, inplace=True)

# Estos valores del medidor al ser desconocidos o no proveidos se mandaran a un string
# 'XX' indicando que no tienen valor
# Para el caso de multiplicador lo dejaremos en 0, ya que no se conoce ese dato, y su valor no es importante
# ya que solo sera un marcador que no se pudo asociar
vals = { 'numeroMedi' : 'XX', 'statusMedi': 'XX', 'codigoMedi' : 'XX', 'codigoLote': 'XX', 'giro':'XX','multiplica' : 0}
df_users.fillna(value=vals, inplace=True)

# Imprimos valores nulos actualizados 114 Notebook
cols_null_values = { list(df_users.columns)[index] : value for index,value in enumerate(df_users.isna().sum()) if value > 0}
cols_with_nulls = [ list(df_users.columns)[index] for index, value in enumerate(df_users.isna().sum()) if value > 0]
print('..........')
print('Columnas con valores nulos',cols_null_values)
print('Columnas con valores nulos',cols_with_nulls)

# PAra llenar este valor de consumo promedio, 
# se va a llenar de acuerdo a la media o mediana de su giro, tarifa, hilos, tipoSuministro

cols_to_group = ['giro', 'tarifa', 'tipoSumini', 'hilos', 'multiplica']
df_consumos = df_users[['consumoPro']]
df_users['consumoPro'] = df_users['consumoPro'].fillna(df_users.groupby(cols_to_group)['consumoPro'].transform('median'))


# Aqui podemos ver que valores fueron sustuidos con que valores
diff = df_consumos['consumoPro'].compare(df_users['consumoPro'])
print(diff)

# Aun tenemos 8 valores nulos en consumoPromedio, vamos a verificar porque
print('..........')
print('Valores nulos de consumoPro Antes:',df_consumos['consumoPro'].isna().sum())
print('Valores nulos de consumoPro Ahora:',df_users['consumoPro'].isna().sum())


# Para llenar estos valores lo haremos a un nivel mas bajo de agrupación, unicamente con su giro y tarifa.
cols_to_group_1 = ['giro', 'tarifa']
df_users['consumoPro'] = df_users['consumoPro'].fillna(df_users.groupby(cols_to_group_1)['consumoPro'].transform('median'))

cols_to_group_2 = ['giro']
df_users['consumoPro'] = df_users['consumoPro'].fillna(df_users.groupby(cols_to_group_2)['consumoPro'].transform('median'))

# Aun tenemos 8 valores nulos en consumoPromedio, vamos a verificar porque
print('..........')
print('Valores nulos de consumoPro Antes:',df_consumos['consumoPro'].isna().sum())
print('Valores nulos de consumoPro Ahora:',df_users['consumoPro'].isna().sum())


# Verificar antes de borrar, por si alguna otra columna tiene mas valores nulos,
# aqui asumimos que el unico valor nulo faltante son los de consumoPromedio
df_users.dropna(inplace=True)

cols_null_values = { list(df_users.columns)[index] : value for index,value in enumerate(df_users.isna().sum()) if value > 0}
cols_with_nulls = [ list(df_users.columns)[index] for index, value in enumerate(df_users.isna().sum()) if value > 0]
print('..........')
print('Valores nulos: ',cols_null_values)
print('Tamaño dataset: ',df_users.shape)

if(SHOW_GRAPHS):
    msno.matrix(df_users.sort_values(by=['giro','tarifa','rpu']))
    plt.title('Usuarios sin datos nulos')
    plt.show()


# Tratar los valores duplicados

print('Duplicados en todas las columnas: ', df_users.duplicated().sum())

# Voy en la linea 133

#Eliminamos los duplicados que sean iguales en todas las columnas.
df_users.drop_duplicates(keep='first', inplace=True)

#Para eliminar los registros duplicados que son iguales en rpu y consumoPro
df_users.drop_duplicates(subset=['rpu','consumoPro'], keep='first', inplace=True)

# Reiniciamos los indices
df_users = df_users.reset_index()

# Ahora verificamos si existen duplicados en rpu solamente
print('Duplicados en rpu: ', df_users.duplicated(subset=['rpu'], keep=False).sum())

# Operaciones antes de realizar una agregacion
# Hay que verificar los tipos de cada columnas, esto porque la primera vez ejecuto un error,
#  entonces hay que ver que sean del tipo de dato adecuado. Si son Int que todos sea Int etc.


# Verificamos la conversion a int

list_to_int_1 = [
    'MZ_ACESOPER_', 'MZ_ACESOAUT_', 'MZ_RECUCALL_', 'MZ_SENALIZA_', 'MZ_ALUMPUB_', 'MZ_TELPUB_',
    'MZ_BANQUETA_', 'MZ_GUARNICI_', 'MZ_ARBOLES_', 'MZ_RAMPAS_', 'MZ_PUESSEMI_', 'MZ_PUESAMBU_',
    'MZ_FT_PUESAMBU_', 'MZ_FT_PUESSEMI_', 'MZ_FT_RAMPAS_', 'MZ_FT_ARBOLES_', 'MZ_FT_GUARNICI_',
    'MZ_FT_BANQUETA_', 'MZ_FT_TELPUB_', 'MZ_FT_ALUMPUB_', 'MZ_FT_SENALIZA_', 'MZ_FT_RECUCALL_',
    'MZ_FT_ACESOAUT_', 'MZ_FT_ACESOPER_', 'MZ_FT_TIPOVIA_C'
]

list_to_float = [
    'MZ_P_TVIVHAB', 'MZ_P_TVIVPAR', 'MZ_PVIVPARHAB', 'MZ_P_VIVPAR_D',  'MZ_P_VIVPAR_U', 'MZ_P_VIVNOHAB',
    'MZ_P_V_PISODT', 'MZ_P_V_C_ELEC','MZ_P_V_AGUADV', 'MZ_P_V_DRENAJ', 'MZ_P_V_EXCUSA', 'MZ_P_3MASOCUP', 
    'MZ_PROOCUP_C', 'MZ_PP0A14A',  'MZ_PP15A29A',  'MZ_PP30A59A','MZ_PP_60YMAS',  'MZ_PPCON_LIM', 'MZ_GRAPROES'
]

list_to_int_2 = [
    'MZ_VIVTOT', 'MZ_TVIVHAB', 'MZ_TVIVPAR','MZ_TVIVPARHAB', 'MZ_VIVPAR_DES',  'MZ_VIVPAR_UT',  'MZ_VIVNOHAB',
    'MZ_VPH_PISODT','MZ_VPH_C_ELEC',  'MZ_VPH_AGUADV', 'MZ_VPH_DRENAJ',  'MZ_VPH_EXCUSA',  'MZ_V_3MASOCUP', 
    'MZ_POBTOT','MZ_P0A14A','MZ_P15A29A','MZ_P30A59A','MZ_P_60YMAS','MZ_PCON_LIM',
   
]

 # Debemos quitar estos valor de N.D. para no tener errores en las operacioenes
for col in list_to_int_2:
    df_users[col] = df_users[col].replace('N.D.', 0)
for col in list_to_float:
    df_users[col] = df_users[col].replace('N.D.', 0)
    
list_to_int = list_to_int_1 + list_to_int_2


for col in list_to_int:
    df_users[col] = df_users[col].astype('int')
    assert df_users[col].dtype == 'int'

assert df_users['MZ_TVIVHAB'].dtype == 'int'
# We must verify data integrity
# df_users['MZ_P_TVIVHAB'] = df_users['MZ_TVIVHAB'] / df_users['MZ_VIVTOT']


# Al hacer esta operación, estamos generando valores nulos otra vez,
# por lo que estos valores antes de pasarlos a flotante debemos convertirlos a 0
df_users['MZ_P_TVIVHAB'] = df_users['MZ_P_TVIVHAB'].fillna(0)



df_users['MZ_P_TVIVHAB'] = df_users['MZ_P_TVIVHAB'].astype('float')
assert df_users['MZ_P_TVIVHAB'].dtype == 'float'

df_users['MZ_PPCON_LIM'] = df_users['MZ_PPCON_LIM'].replace('*********', 0)

for col in list_to_float:
    df_users[col] = df_users[col].astype('float')
    assert df_users[col].dtype == 'float'


# A estos que tienen el mismo rpu les vamos a sacar la media de su consumoPromedio y se conservara uno con este valor
cols = list(df_users.columns)
cols.remove('consumoPro')
cols.remove('geometry')
cols.remove('rpu')
# Operaciones a realizar en cada columna
summaries = { column : 'first' for column in cols}
summaries['consumoPro'] = 'mean'
summaries['geometry'] = 'first'
column_names =['rpu']

df_users_grouped_mean = df_users.groupby(by = column_names).agg(summaries).reset_index()

# Nos aseguramos que se realizo la agregación
duplicates_ = df_users_grouped_mean.duplicated(subset = column_names, keep = False)
df_users_grouped_mean[duplicates_].sort_values(by = 'rpu')


print('PREV: ',df_users.shape)
print('PREV: ', df_users['rpu'].unique().shape)
print('ACTUAL: ',df_users_grouped_mean.shape)
print('ACTUAL: ',df_users_grouped_mean['rpu'].unique().shape)

# Volvemos a verificar si hay valores nulos
cols_null_values = { list(df_users.columns)[index] : value for index,value in enumerate(df_users.isna().sum()) if value > 0}
cols_with_nulls = [ list(df_users.columns)[index] for index, value in enumerate(df_users.isna().sum()) if value > 0]
print(cols_null_values)
print(cols_with_nulls)


if(SHOW_GRAPHS):
    msno.matrix(df_users_grouped_mean.sort_values(by=['giro','tarifa','rpu']))
    plt.title('Usuarios sin datos nulos ni duplicados')
    plt.show()

# Convertimos nuestro conjunto de datos a un GeoDataFrame para exportarlo como archivo espacial
gdf = gpd.GeoDataFrame(df_users_grouped_mean)
gdf.to_file(ARCHIVO_SALIDA, layer='users', driver="GPKG")

end = time.time()

print('Tiempo total de ejecución', end-start)
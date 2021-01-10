

import geopandas as gpd
import pandas as pd
import datetime as dt


ARCHIVO_USUARIOS = "../DATASETS/USERS_UPDATE.gpkg"
ARCHIVO_HISTORIOC = "../DATASETS/Consumos/viewConsumosDK.csv"
ARCHIVO_SALIDA_HISTORICO = "../DATASETS/Consumos/consumos_selected.csv"

df_users = gpd.read_file(ARCHIVO_USUARIOS)
print(df_users.head())
df_consumos = pd.read_csv(ARCHIVO_HISTORIOC)
print(df_consumos.head())

# Estos son los registros de consumos que necesitamos unicamente, porque los demas estan de sobra.
consumos_to_select = df_consumos.isin({'rpu': df_users['rpu'].unique() })
consumos = df_consumos[consumos_to_select['rpu'] == True].reset_index(drop=True)
print(consumos.head())

print('Numero de Usuarios con consumos y datos de usuario:', consumos['rpu'].unique().shape  )


print('Consumos antes')
consumos.info()
# Primero debemos convertir la fecha de [11/09/2019] a 11/09/2019
consumos['mesConsumo'] = consumos['mesConsumo'].str.strip('[')
consumos['mesConsumo'] = consumos['mesConsumo'].str.strip(']')
consumos['mesConsumo'] = pd.to_datetime(
    consumos['mesConsumo'],
    format = "%Y-%m-%d"
)
assert consumos['mesConsumo'].dtype == 'datetime64[ns]'

# Verificamos de nueva cuenta los tipos de datos de los usuarios
print('Consumos despues', consumos_selected.info())
print(consumos_selected.head())

print(consumos_selected.head())
consumos_selected.to_csv(ARCHIVO_SALIDA_HISTORICO,index=False)
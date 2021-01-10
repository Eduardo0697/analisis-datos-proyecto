import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import time

inicio = time.time()

ARCHIVO_SERIESTIEMPO = "./DATASETS/Consumos/consumos_selected_TEST29DIC.csv"
ARCHIVO_SALIDA_PARA_R = "./DATASETS/Consumos/users_multiindex_TODOSMUNICIPIOS_29DIC.csv"
SHOW_GRAPHS = False

df_users = gpd.read_file("./DATASETS/USERS_UPDATE.gpkg")

df_consumos = pd.read_csv(ARCHIVO_SERIESTIEMPO)
print(df_consumos.head())
df_consumos.info()

# Agrupamos a los usuarios por mes de consumo para contarlos
df_consumos_counts_user = df_consumos.groupby('rpu')['mesConsumo'].count()

# # Graficamos el numero de registros por mes
# plt.bar(df_consumos_counts_user.value_counts().index, df_consumos_counts_user.value_counts()/df_consumos_counts_user.value_counts().sum())
# plt.show()


no_registros_considerar = df_consumos_counts_user[df_consumos_counts_user > 11].value_counts().sum()
no_registros_totales = df_consumos_counts_user.value_counts().sum()

print('Porcentaje de registros a considerar: ',str(no_registros_considerar/no_registros_totales *100) +' %')


# Usuarios a seleccionar
users_with_12 = list(df_consumos_counts_user.index[df_consumos_counts_user > 11])

print('Numero de usuarios con mas de 11 registros',len(users_with_12))

# Usuarios seleccionados
df_users_final = df_consumos[df_consumos['rpu'].isin(users_with_12)].reset_index(drop=True)
print('Tamaño del dataset a guardar', df_users_final.shape)


# Numero de usuarios vs numero de registros
print(df_users_final.groupby('rpu')['mesConsumo'].count().value_counts().sort_index())


# Convertimos el mes de consumo a formato de fecha
df_users_final['mesConsumo'] = pd.to_datetime(df_users_final['mesConsumo'], format = "%Y-%m-%d")
print('Dataset despues de transformacion', df_users_final.info())

# Establecemos que el mes de consumo sea el indice
df_users_final = df_users_final.set_index('mesConsumo')
# Agregamos nuevas columnas
df_users_final['Year'] = df_users_final.index.year
df_users_final['Month'] = df_users_final.index.month
df_users_final['Weekday'] = df_users_final.index.weekday

### --------
df_users_multindex = df_users_final.set_index(["rpu",df_users_final.index])
df_users_multi_ordered = df_users_multindex.sort_values(by=['rpu', 'mesConsumo'])

# Seleccionar solo los de 101
# df_users_multi_ordered.reset_index()[df_users_multi_ordered.reset_index().isin({'rpu': list(df_users[df_users['cveMunicip'] == '101' ]['rpu'])})['rpu']].to_csv(ARCHIVO_SALIDA_PARA_R)
# Seleccionar TODOS
df_users_multi_ordered.reset_index().to_csv(ARCHIVO_SALIDA_PARA_R)
if(SHOW_GRAPHS):

    def ecdf(data):
        """Compute ECDF for a one-dimensional array of measurements."""
        # Number of data points: n
        n = len(data)

        # x-data for the ECDF: x
        x = np.sort(data)

        # y-data for the ECDF: y
        y = np.arange(1, n+1) / n

        return x, y

    # Registros por mes

    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])

    ax.grid(linestyle='--')
    ax.bar(df_users_final.groupby(df_users_final.index).count()['rpu'].index, df_users_final.groupby(df_users_final.index).count()['rpu'], width=20)
    ax.set_title('Distribución de los datos de cada mes en los dos años')
    ax.set_xlabel('Mes')
    ax.set_ylabel('Cantidad de registros')

    plt.show()

    # Registros por numero de mes
    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.bar(df_users_final['Month'].value_counts().sort_index().index, df_users_final['Month'].value_counts().sort_index())
    ax.set_title('Distribución de los datos por mes')
    ax.set_xlabel('Mes')
    ax.set_ylabel('Cantidad de registros')
    plt.show()



    # Registros por año
    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.bar(df_users_final['Year'].value_counts().index, df_users_final['Year'].value_counts())
    ax.set_title('Distribución de los datos por año')
    ax.set_xlabel('Año')
    ax.set_ylabel('Cantidad de registros')
    plt.show()

    # ECDF consumos Kwh
    x, y = ecdf(np.log10(df_users_final[(df_users_final['kwh'] > 0)]['kwh']))
    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.plot(x,y, marker='.', linestyle='none', alpha=0.5)
    ax.set_title('ECDF Consumos kwh')
    ax.set_xlabel('Consumo log10')
    ax.set_ylabel('ECDF')
    ax.grid(linestyle='--')
    plt.show()

    # Concumos medios en toda la cobertura de tiempo
    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.plot(df_users_final.groupby(df_users_final.index)['kwh'].median().index, df_users_final.groupby(df_users_final.index)['kwh'].median(), marker='.',)
    ax.set_title('Consumos medio en toda la cobertura de tiempo')
    ax.set_xlabel('Mes')
    ax.set_ylabel('Consumo Medio (kwh)')
    plt.show()

    # Consumos medios por numero del mes

    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.plot(df_users_final.groupby('Month')['kwh'].median().index, df_users_final.groupby('Month')['kwh'].median(), marker='.',)
    ax.set_title('Consumos medio por mes')
    ax.set_xlabel('Mes')
    ax.set_ylabel('Consumo Medio (kwh)')
    plt.show()

    # Distribución de consumo promedio diario por mes
    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.plot(df_users_final['kwh'].index, df_users_final['kwh'], marker='.', linestyle='None', alpha=0.5)
    ax.set_title('Consumos por mes')
    ax.set_xlabel('Mes')
    ax.set_ylabel('Consumo Medio (kwh)')
    plt.show()

print("tiempo" , time.time() - inicio)
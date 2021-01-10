import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.neighbors import NearestNeighbors
import numpy as np
from sklearn import metrics
from sklearn.cluster import DBSCAN
import gower
import time
from sklearn.model_selection import StratifiedShuffleSplit


# ARCHIVO_USUARIOS = "./DATASETS/usuarios_preparados_10Dic.csv"
# ARCHIVO_CENTROIDES = "./DATASETS/TimeSeries/centroides.csv"
# ARCHIVO_USUARIOS_TS = "./DATASETS/TimeSeries/users_clustered.csv"
# ARCHIVO_MANZANAS = "./DATASETS/MANZANAS/MANZANAS.shp"
# ARCHIVO_HISTORICO = "./DATASETS/TimeSeries/users_multiindex_2.csv"
# ARCHIVO_FRENTE_MANZANAS = "./DATASETS/MANZANAS_FRENTES/MANZANAS_FRENTES.shp"
# ARCHIVO_USUARIOS = "./DATASETS/usuarios_preparados_10Dic.csv"
# ARCHIVO_CENTROIDES = "./DATASETS/centroidesTODOS.csv"
# ARCHIVO_USUARIOS_TS = "./DATASETS/users_clusteredTODOS.csv"
# ARCHIVO_MANZANAS = "./DATASETS/MANZANAS/MANZANAS.shp"
# ARCHIVO_HISTORICO = "./DATASETS/Consumos/users_multiindex_TODOSMUNICIPIOS.csv"
# ARCHIVO_FRENTE_MANZANAS = "./DATASETS/MANZANAS_FRENTES/MANZANAS_FRENTES.shp"
ARCHIVO_USUARIOS = "./DATASETS/usuarios_preparados_29TESTDic.csv"
ARCHIVO_CENTROIDES = "./DATASETS/centroidesTODOS_29DIC.csv"
ARCHIVO_USUARIOS_TS = "./DATASETS/users_clusteredTODOS_29DIC.csv"
ARCHIVO_MANZANAS = "./DATASETS/MANZANAS/MANZANAS.shp"
ARCHIVO_HISTORICO = "./DATASETS/Consumos/users_multiindex_TODOSMUNICIPIOS_29DIC.csv"
ARCHIVO_FRENTE_MANZANAS = "./DATASETS/MANZANAS_FRENTES/MANZANAS_FRENTES.shp"
GUARDAR_BD = True
GENERAR_GRAFICAS = False

inicio = time.time()

# Datos para la base de datos
DATA_BD = {
    "port":"5432",
    "user":"postgres", 
    "password":"admin",
    "host":"127.0.0.1",
    "database":"DB_ALL_USERS_TESTDIC",
    "tablaCentroids" : "centroids",
    "tablaUsuarios" : "users",
    "tablaFrentesManzanas" : "frentes_manzanas",
    "tablaConsumos" : "consumos_usuarios",
    "tablaManzanas" : "manzanas"
}

# Lectura de los archivos
df_users = pd.read_csv(ARCHIVO_USUARIOS)
# Leemos el csv de los centroides, y cambiamos el nombre de las columnas con el de la tabla a crear
df_centroides = pd.read_csv(
    ARCHIVO_CENTROIDES, 
    names=["CENTROID", "TS_1", "TS_2", "TS_3", "TS_4", "TS_5", "TS_6", "TS_7", "TS_8", "TS_9", "TS_10", "TS_11", "TS_12"],
    header=0
    )
df_TS_clustered = pd.read_csv(ARCHIVO_USUARIOS_TS)
df_TS_clustered.rename(columns={"index_rpu": "rpu", "cluster": "TimeSeries"}, inplace=True)

print(df_users.head())
print(df_TS_clustered.head())
print(df_centroides.head())
print('Tamaño Usuarios: ', df_users.shape)
print('Tamaño TimeSeries: ', df_TS_clustered.shape)

# Merge de usuarios con cliuster de Series de tiempo
users_complete = df_users.merge(df_TS_clustered, left_on="rpu",right_on="rpu")

# Selección de atributos para clustering
df_users_to_cluster = users_complete[['rpu','consumoPro','TimeSeries', 'tipoTarifa', 'tarifa' ,'divisionGiro', 'statusMedi', 'multiplica', 'hilos']].copy()
df_users_to_cluster.set_index('rpu', inplace=True)
print('-----------------------')
print(df_users_to_cluster.info())

# Arreglamos valores de tipo de dato para poder usarlos en el modelo
df_users_to_cluster['TimeSeries'] = df_users_to_cluster['TimeSeries'].astype('object')
df_users_to_cluster['hilos'] = df_users_to_cluster['hilos'].astype('object')
print('-----------------------')
print(df_users_to_cluster.info())

# Proporciones por diviison de giro
print('-----------ORIGINAL------------')
print(df_users_to_cluster['divisionGiro'].value_counts() / len(df_users_to_cluster))

# Generamos muestra estratificada
split = StratifiedShuffleSplit(n_splits=1, test_size=0.45, random_state=42)
for train_index, test_index in split.split(df_users_to_cluster, df_users_to_cluster['divisionGiro']):
    strat_test_set = df_users_to_cluster.iloc[train_index]
    users_clustered = df_users_to_cluster.iloc[test_index]

print('------------MUESTRA ESTRATIFICADA-----------')
print(users_clustered['divisionGiro'].value_counts() / len(users_clustered))

print('Tamaño de datos a procesar', len(users_clustered))

# Calculo de la matriz de distancias con metrica gower
start = time.time()
gower_mat = metrics.pairwise_distances(
    gower.gower_matrix(users_clustered),
    metric="precomputed")
end = time.time()
print('Tiempo de ejecución de la matriz de distancia',end - start)

# Comparación de matriz de distancia con el primer registro
first_row = gower_mat[0]
print('Registro base: ',users_clustered[:1], "\n")
print('Registro mas parecido: ',users_clustered[first_row == min(first_row[first_row != min(first_row)])])
print('Registro menos parecido: ',users_clustered[first_row == max(first_row)])

# Implementamos el modelo de DBSCAN con la matriz ya computada
start = time.time()
model = DBSCAN(eps=0.11, min_samples=40, metric="precomputed")
model.fit(gower_mat)
end = time.time()
print('Calculo de DBSCAN: ', end - start)

# Unimos las etiquetas de los resultados

results = users_clustered.copy()
results['cluster'] = model.labels_

# Calculamos el coeficinete de silueta
print('Coeficiente de Silueta eps=0.11, min_samples=40 : ',metrics.silhouette_score(gower_mat, model.labels_, metric='precomputed'))

print('Numero de clusters', results['cluster'].value_counts().shape)
print('Numero de anomalias', results[results['cluster']==-1].shape)
print('Valores de clusters', results['cluster'].value_counts())

if(GUARDAR_BD):
    import psycopg2
    from psycopg2 import Error
    import psycopg2.extras as extras
    from sqlalchemy import create_engine

    def insert_data_table(conn, df, table):
        # Creamos una lista de tuplas, donde cada tupla representa una fila del dataset
        tuples = [tuple(x) for x in df.to_numpy()]
        # Creamos los valores de las columnas en una lista
        cols = ','.join(list(df.columns))
        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("Ejecución terminada de la tabla {}".format(table))
        cursor.close()

    # Establish a connection to the database by creating a cursor object
    conn = psycopg2.connect(port=DATA_BD["port"],
                            user=DATA_BD["user"], 
                            password=DATA_BD["password"],
                            host=DATA_BD["host"],
                            database=DATA_BD["database"]
                            )
    cur = conn.cursor()

    # Propiedades de la conexion con PostgreSQL
    print( conn.get_dsn_parameters(), "\n")
    cur.execute("SELECT version();")
    record = cur.fetchone()
    print("Estas conectado a - ", record, "\n")
    cur.execute("SELECT postgis_full_version();")
    record = cur.fetchone()
    print("Version de POSTGIS - ", record, "\n")

    #------------ CREAMOS Y GUARDAMOS TABLA DE CENTROIDES EN BD----------------
    create_table_centroids = """
        CREATE TABLE {}(
            CENTROID INTEGER PRIMARY KEY,
            TS_1 REAL NOT NULL,
            TS_2 REAL NOT NULL,
            TS_3 REAL NOT NULL,
            TS_4 REAL NOT NULL,
            TS_5 REAL NOT NULL,
            TS_6 REAL NOT NULL,
            TS_7 REAL NOT NULL,
            TS_8 REAL NOT NULL,
            TS_9 REAL NOT NULL,
            TS_10 REAL NOT NULL,
            TS_11 REAL NOT NULL,
            TS_12 REAL NOT NULL
            )
        """.format(DATA_BD["tablaCentroids"])
    cur.execute(create_table_centroids)
    # Ejecutamos nuestra funcion para insertar datos en una tabla especifica.
    insert_data_table(conn, df_centroides, DATA_BD["tablaCentroids"])

    #------------ CREAMOS Y GUARDAMOS TABLA DE USUARIOS----------------
    cols_to_save=[ 
        'rpu','numcuenta','nombreUsua', 'agencia','cveMunicip','statusServ','tarifa','giro','tipoSumini','numeroMedi',
        'statusMedi','codigoMedi','codigoLote','multiplica','hilos','CICLO', 'RUTA','consumoPro','geometry','divisionGiro',
        'tipoTarifa','TimeSeries','cluster','MZ_CVEGEO','MZ_FT_CVEVIAL', 'MZ_FT_OID'
         ]
    # CREACION TABLA USUARIOS

    create_table_users = """
        CREATE TABLE {}(
            rpu VARCHAR PRIMARY KEY, numcuenta VARCHAR, nombreUsua VARCHAR, agencia VARCHAR,CICLO VARCHAR, RUTA VARCHAR,
            cveMunicip VARCHAR, tipoSumini VARCHAR,numeroMedi VARCHAR,
            codigoMedi VARCHAR, statusMedi VARCHAR, codigoLote VARCHAR, statusServ INTEGER,
            multiplica REAL,hilos INTEGER, consumoPro VARCHAR, divisionGiro VARCHAR, giro VARCHAR, tipoTarifa VARCHAR,
            tarifa VARCHAR,TimeSeries INTEGER,cluster INTEGER, MZ_CVEGEO VARCHAR ,MZ_FT_CVEVIAL VARCHAR , 
            MZ_FT_OID VARCHAR,
            geometry GEOMETRY
            )
        """.format(DATA_BD["tablaUsuarios"])
    try:
        cur.execute(create_table_users)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback() 

    insert_data_table(
        conn, 
        users_complete.drop(columns="Unnamed: 0").merge(results.reset_index()[['rpu','cluster']], on="rpu")[cols_to_save],
        DATA_BD["tablaUsuarios"]
    )   

    #------------ GUARDAMOS TABLA MANZANAS EN BD----------------
    df_manzanas =  gpd.read_file(ARCHIVO_MANZANAS)

    cols_manzanas = ['MZ_CVEGEO', 'MZ_VIVTOT', 'MZ_TVIVHAB', 'MZ_P_TVIVHAB', 'MZ_TVIVPAR', 'MZ_P_TVIVPAR','MZ_TVIVPARHAB',
                     'MZ_PVIVPARHAB', 'MZ_VIVPAR_DES', 'MZ_P_VIVPAR_D', 'MZ_VIVPAR_UT', 'MZ_P_VIVPAR_U', 'MZ_VIVNOHAB',
                     'MZ_P_VIVNOHAB', 'MZ_VPH_PISODT', 'MZ_P_V_PISODT', 'MZ_VPH_C_ELEC', 'MZ_P_V_C_ELEC','MZ_VPH_AGUADV',
                     'MZ_P_V_AGUADV', 'MZ_VPH_DRENAJ', 'MZ_P_V_DRENAJ', 'MZ_VPH_EXCUSA', 'MZ_P_V_EXCUSA','MZ_V_3MASOCUP',
                     'MZ_P_3MASOCUP', 'MZ_PROOCUP_C', 'MZ_POBTOT', 'MZ_P0A14A', 'MZ_PP0A14A', 'MZ_P15A29A', 'MZ_PP15A29A',
                     'MZ_P30A59A', 'MZ_PP30A59A', 'MZ_P_60YMAS', 'MZ_PP_60YMAS', 'MZ_PCON_LIM', 'MZ_PPCON_LIM','MZ_GRAPROES',
                     'MZ_ACESOPER_', 'MZ_ACESOAUT_', 'MZ_RECUCALL_', 'MZ_SENALIZA_', 'MZ_ALUMPUB_', 'MZ_TELPUB_',
                     'MZ_BANQUETA_', 'MZ_GUARNICI_', 'MZ_ARBOLES_', 'MZ_RAMPAS_', 'MZ_PUESSEMI_', 'MZ_PUESAMBU_',
                     'MZ_ACESOPER_C', 'MZ_ACESOAUT_C', 'MZ_RECUCALL_C', 'MZ_SENALIZA_C', 'MZ_ALUMPUB_C', 'MZ_TELPUB_C',
                     'MZ_BANQUETA_C', 'MZ_GUARNICI_C', 'MZ_ARBOLES_C', 'MZ_RAMPAS_C', 'MZ_PUESSEMI_C', 'MZ_PUESAMBU_C',
                     'MZ_LOC', 'MZ_AGEB', 'MZ_MZA'
                     ]

    df_MZN = users_complete.drop(columns="Unnamed: 0").merge(results.reset_index()[['rpu', 'cluster']], on="rpu")[cols_manzanas]
    df_manzanas_upload = df_manzanas[['CVEGEO', 'geometry']].merge(df_MZN.drop_duplicates(), left_on="CVEGEO",right_on="MZ_CVEGEO").reset_index(drop=True)

    # Para la tabla de manzanas, SQLalchemy crea la tabla sin necesidad de crear previamente la tabla
    
    db_connection_url = "postgres://{}:{}@{}:{}/{}".format(DATA_BD["user"], DATA_BD["password"], DATA_BD["host"], DATA_BD["port"], DATA_BD["database"]);
    print('Connection URL->', db_connection_url)
    engine = create_engine(db_connection_url)
    df_manzanas_upload.to_postgis(name=DATA_BD["tablaManzanas"], con=engine, if_exists="replace")

    #------------ GUARDAMOS SERIES DE TIEMPO ----------------
    df_TS = pd.read_csv(ARCHIVO_HISTORICO)
    df_TS.drop(columns="Unnamed: 0", inplace=True)
    print(df_TS.head())
    print('Registros de TS totales: ',df_TS['rpu'].shape)
    print('Registros de TS escribir: ',df_TS['rpu'].isin(results.reset_index()['rpu']).sum())
    # Seleccionamos a los usuarios que se generaron datos
    df_TS_save = df_TS[df_TS['rpu'].isin(results.reset_index()['rpu'])].reset_index(drop=True)
    df_TS_save.head()
    # Seleccionamos solo los del año 2018 y 2019
    df_TS_save[~(df_TS_save['Year'] == 2020)]   

    create_table_consumos = """
        CREATE TABLE {}(
            rpu VARCHAR, mesConsumo DATE, kwh INTEGER, Year INTEGER, Month INTEGER, Weekday INTEGER
            )
        """.format(DATA_BD["tablaConsumos"])
    try:
        cur.execute(create_table_consumos)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback() 
        
    insert_data_table(
        conn, 
        df_TS_save[~(df_TS_save['Year'] == 2020)],
        DATA_BD["tablaConsumos"]
    )

    #------------ GUARDAMOS FRENTES DE MANZANA ----------------
    # Creación de tabla frente de manzanas
    df_frentes_manzanas =  gpd.read_file(ARCHIVO_FRENTE_MANZANAS)
    print('Columnas frentes de manzanas', df_frentes_manzanas.columns)

    cols_frentes_manzanas = ['MZ_FT_CVEGEO', 'MZ_FT_CVEVIAL', 'MZ_FT_NOMVIAL', 'MZ_FT_TIPOVIAL', 'MZ_FT_CVEFT',
                             'MZ_FT_SHAPE_LENG', 'MZ_FT_TIPOVIA_C', 'MZ_FT_ACESOPER_', 'MZ_FT_ACESOAUT_','MZ_FT_RECUCALL_',
                             'MZ_FT_SENALIZA_','MZ_FT_ALUMPUB_', 'MZ_FT_TELPUB_', 'MZ_FT_BANQUETA_', 'MZ_FT_GUARNICI_',
                             'MZ_FT_ARBOLES_','MZ_FT_RAMPAS_','MZ_FT_PUESSEMI_', 'MZ_FT_PUESAMBU_', 'MZ_FT_ACESOPER_C',
                             'MZ_FT_ACESOAUT_C','MZ_FT_RECUCALL_C', 'MZ_FT_SENALIZA_C', 'MZ_FT_ALUMPUB_C', 'MZ_FT_TELPUB_C',
                             'MZ_FT_BANQUETA_C', 'MZ_FT_GUARNICI_C','MZ_FT_ARBOLES_C', 'MZ_FT_RAMPAS_C','MZ_FT_PUESSEMI_C',
                             'MZ_FT_PUESAMBU_C', 'MZ_FT_OID'
                             ]
    df_FT = users_complete.drop(columns="Unnamed: 0").merge(results.reset_index()[['rpu','cluster']], on="rpu")[cols_frentes_manzanas]
    df_frentes_upload = df_frentes_manzanas[['OID','geometry']].merge(df_FT.drop_duplicates(), left_on="OID", right_on="MZ_FT_OID").reset_index(drop=True)
    print(df_frentes_upload.head(2))

    engine = create_engine(db_connection_url)
    df_frentes_upload.to_postgis(name=DATA_BD["tablaFrentesManzanas"], con=engine, if_exists="replace")

    #------------ CERRAMOS CONEXION ----------------

    # Terminamos conexion con Postgress
    cur.close()
    conn.close()
    print("Conexion con PostgreSQL terminada")

    fin = time.time()
    print("Tiempo total con almacenamiento", fin - inicio)

if(GENERAR_GRAFICAS):

    # Grafica para determinar el valor de epsilon con KNN-------------------------------------
    nn = NearestNeighbors(n_neighbors=35, metric="precomputed").fit(gower_mat)
    distances, idx = nn.kneighbors()
    distances = np.sort(distances, axis=0)
    distances = distances[:, 1]
    plt.figure(figsize=(15,5))
    plt.plot(distances)
    plt.title("Epsilon value")
    plt.show()

    # Evaluación de coeficientes de silueta para determinar EPS y MinPts---------------------
    # Recomendado solo para pequeñas muestras estratificada de 0.1 0 0.2
    eps_values = np.arange(0.01, 0.2, 0.01)
    min_samples = np.arange(5, 30)

    sil_score = []
    epsvalues = []
    min_samp = []
    no_clusters = []
    noise_points = []

    for epsilon in eps_values:
        for samples in min_samples:
            model_dbscan = DBSCAN(eps=epsilon, min_samples=samples, metric="precomputed")
            model_dbscan.fit(gower_mat)
            labels = model_dbscan.labels_
            SIL = metrics.silhouette_score(
                gower_mat,
                labels,
                metric='precomputed')

            noise_points.append(np.count_nonzero(labels == -1))
            sil_score.append(SIL)
            epsvalues.append(epsilon)
            min_samp.append(samples)
            no_clusters.append(len(np.unique(labels)))
            dbscan_results = list(
                zip(no_clusters, sil_score, epsvalues, min_samp, noise_points)
            )
            print(epsilon, samples)

    dbscan_df = pd.DataFrame( dbscan_results,columns=['no_of_clusters', 'silhouette_score', 'epsilon_values', 'minimum_points', 'anomalies'])
    print(dbscan_df.sort_values(by="silhouette_score", ascending=False).head(5))

    # Generación del grafico

    eps_values = np.arange(0.01, 0.2, 0.01)
    min_samples = np.arange(20, 30)
    plt.figure(figsize=(20, 10))
    row = 0
    fig, ax = plt.subplots(10, 2, figsize=(30, 140))
    sns.set_context("talk")
    for index, epsilon in enumerate(eps_values):
        if index > 9:
            col = 1
        else:
            col = 0

        if index == 10:
            row = 0

        clusters = dbscan_df[dbscan_df["epsilon_values"] == epsilon]["no_of_clusters"]
        sil_score = dbscan_df[dbscan_df["epsilon_values"] == epsilon]["silhouette_score"]
        num_samples = dbscan_df[dbscan_df["epsilon_values"] == epsilon]["minimum_points"]
        sns.scatterplot(
            x="no_of_clusters",
            y="silhouette_score",
            hue="minimum_points",
            palette="deep",
            data=dbscan_df[dbscan_df["epsilon_values"] == epsilon],
            ax=ax[row, col],
        )
        ax[row, col].set_title('Epsilon: ' + str(epsilon))
        ax[row, col].set(xlabel="No Clusters", ylabel="Sil ")
        row = row + 1

    # plt.legend()
    plt.show()
    # ------------------------------------------------------------------------------------


    # Grafica de coeificiente de silueta por cada epsilon evaluando diferente numero de puntos,
    # Recomendada solamente para un numero pequeño de muestras 0.1 o 0.2

    min_samples = np.arange(5, 45, 5).tolist()
    epsilon_values = [0.05, 0.07, 0.09, 0.11, 0.13, 0.15, 0.17, 0.19]
    print("Numero de puntos a evaluar: ",min_samples)
    print("Numero de valores de epsilon a evaluar: ",  epsilon_values)
    data = {}
    for epsilon in epsilon_values:
        list_minPoints = []
        for minPoints in min_samples:
            model = DBSCAN(eps=epsilon, min_samples=minPoints, metric="precomputed")
            model.fit(gower_mat)
            sil = metrics.silhouette_score(gower_mat, model.labels_, metric='precomputed')
            print('Coeficiente de Silueta {} MinSamples {} Eps {}'.format(sil, minPoints, epsilon))
            list_minPoints.append(sil)
        data[epsilon] = list_minPoints

    fig, ax = plt.subplots()
    fig.set_size_inches([15, 7])
    for index, value in enumerate(list(data.keys())):
        ax.plot(min_samples, data[value], marker='o', linestyle='--', label="Eps={}".format(value))
    plt.legend()
    plt.xlabel('MinPoints')
    plt.ylabel('Coeficiente de silueta')
    plt.show()

    # Graficas de los datos recabados por diferentes ejecuciones

    tiempos = [169.31, 240.87, 2200.14, 4725, 6078, 8500, 20681]
    registros = [20000, 23233, 69967, 100000, 115000, 139394, 162627]

    fig, ax = plt.subplots()
    fig.set_size_inches([15, 7])
    ax.plot(registros, tiempos, marker='o', linestyle='--')
    plt.xlabel('Numero de registros')
    plt.ylabel('Tiempo (segundos)')
    plt.show()

    memoria = [1.6, 2.15, 19.58, 40, 52.9, 77.72, 105.79]
    registros = [20000, 23233, 69967, 100000, 115000, 139394, 162627]

    fig, ax = plt.subplots()
    fig.set_size_inches([15, 7])
    ax.plot(registros, memoria, marker='o', linestyle='--')
    plt.xlabel('Numero de registros')
    plt.ylabel('Memoria (GB)')
    plt.show()
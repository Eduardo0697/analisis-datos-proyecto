# Tiempo de ejecución  179.2965271472931
# Tiempo  179.92400527000427

import geopandas as gpd
import pandas as pd
from pandas.api.types import CategoricalDtype
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from numpy import median
import time

GENERAR_GRAFICAS = False
ARCHIVO_ENTRADA = "./DATASETS/USERS_UPDATE_TEST29DEC.gpkg"
ARCHIVO_SALIDA = './DATASETS/usuarios_preparados_29TESTDic.csv'

# # Empezamos a contar el tiempo
start = time.time()

df_users = gpd.read_file(ARCHIVO_ENTRADA)
print('Archivo de entrada: ', ARCHIVO_ENTRADA ,df_users.head())

# Verificamos si hay valores nulos
print('Valores despues de conversion de tipos de datos y seleccion')
df_users.info(verbose=True, null_counts=True)

# Eliminamos filas con valores inconsistentes en numero de hilos
inconsistent_hilos = df_users['hilos'].isin(['0'])
print('Valores inconsistentes antes: ',inconsistent_hilos.sum())

df_users = df_users[~inconsistent_hilos].reset_index(drop=True)
inconsistent_hilos_2 = df_users['hilos'].isin(['0'])
inconsistent_hilos_2.sum()
print('Valores inconsistentes despues: ',inconsistent_hilos_2.sum())

print('Valores de hilos', df_users['hilos'].value_counts())

## ------ VERIFICAMOS TIPOS DE DATOS---------------


## DROP COLUMNS
# El index es un valor del orden de la tabla temporal, lo eliminamos igual.
df_users.drop(columns=['index','OBJECTID'], inplace=True)
# Entidad y estado no los necesitams para el analisis ya que todos corresponden a el estado de Chiapas.
df_users.drop(columns=['entidadFed', 'estado'], inplace=True)
# Para el municipio basta con conservar la clave del municipio
df_users.drop(columns=['municipio'],inplace=True)
# Para la CVE de agencia no la necesitamos, basta con conservar la variable categorica agencia
df_users.drop(columns=['CVA_AGENCI'] , inplace=True)
# Para el analisis no es necesario ni el nombre de usuario ni la direccion
df_users.drop(columns=['direccion'], inplace=True)
# zona puede ser un valor de tipo categorico pero al estar analizando una sola zona, no tiene tanta importancia
df_users.drop(columns=['zona'], inplace=True)
# El dato de frequency se nos comento que no considerar
df_users.drop(columns=['FREQUENCY'], inplace=True)
# xDecimal y yDecimal no se necesitan, ya que su valor se repite en x y y
df_users.drop(columns=['xDecimal', 'yDecimal'], inplace=True)
# MUN_CVE_ENT, MUN_NOM_ENT es un valor que no cambie, es la clave del estado y su nombre.
df_users.drop(columns=['MUN_CVE_ENT', 'MUN_NOM_ENT'], inplace=True)
# MUN_CVE_MUN, MUN_NOM_MUN, la clave y nombre de municipio, pero estos valores ya los tenemos en cveMunicip
df_users.drop(columns=['MUN_CVE_MUN', 'MUN_NOM_MUN'], inplace=True)
# MUN_NOM_SUN, MUN_CVE_SUN lo mismo, asi como la poblacion del municipio
df_users.drop(columns=['MUN_NOM_SUN', 'MUN_CVE_SUN', 'MUN_POB_2018'], inplace=True)
# El nombre de la localidad no lo necesitamos al tener ya su clave de localidad.
# Tampoco el OID, n, feature y nearest. distance por ahora lo dejamos.
df_users.drop(columns=['MZ_NOM_LOC', 'MZ_OID','n', 'feature_x', 'feature_y','nearest_x', 'nearest_y'] , inplace=True)
# PAra el nombre de las vialidades en los frentes de manzanas no es indispensable
# en el analisis, unicamente con su clave
# No eliminar para identificar despues
# df_users.drop(columns=['MZ_FT_NOMVIAL', 'MZ_FT_OID','MZ_FT_CVEGEO'], inplace=True)


## STRING
# El rpu al ser un identificador, nos sirve como string en lugar de un numero grande.
df_users['rpu'] = df_users['rpu'].astype(str)
# OBJECTID de tipo string, ademas que es nuestra referencia del resto de los datos en la base de datos del usuario.
# df_users['OBJECTID'] = df_users['OBJECTID'].astype(str)
# Numcuenta puede ser un valor de tipo string ya que es un identificador.
df_users['numcuenta'] = df_users['numcuenta'].astype(str)


## CATEGORICOS
# agencia puede ser un valor de tipo categorico ya que identifica a que agencia pertenece el usuario y sus valores son limitados.
df_users['agencia'] = df_users['agencia'].astype('category')
# cveMunicipio puede ser un valor de tipo categorico ya que identifica el municipio que pertenece el usuario.
df_users['cveMunicip'] = df_users['cveMunicip'].astype('category')
# Status del servicio variable categorica al tener numero limitado de variables.
df_users['statusServ'] = df_users['statusServ'].astype('category')
# Tarifa variable categorica al tener numero limitado de variables.
df_users['tarifa'] = df_users['tarifa'].astype('category')
# Giro variable categorica al tener numero limitado de variables.
df_users['giro'] = df_users['giro'].astype('category')
# Tipo suministro variable categorica al tener numero limitado de variables.
df_users['tipoSumini'] = df_users['tipoSumini'].astype('category')
# Status de medidor variable categorica al tener numero limitado de variables.
df_users['statusMedi'] = df_users['statusMedi'].astype('category')
# El numero de hilos igual variable categorica al tener numero limitado de variables.

cat_type_0 = CategoricalDtype(categories=['1','2','3'], ordered=True)
df_users['hilos'] = df_users['hilos'].astype(cat_type_0)

# Las variables de  CICLO de facturacion y RUTA tendrian que ser categoricas.
df_users['CICLO'] = df_users['CICLO'].astype('category')
df_users['RUTA'] = df_users['RUTA'].astype('category')

medidores_pendiente = ['numeroMedi', 'codigoMedi', 'codigoLote']


# ------------------ MANZANAS -----------------
# CATEGORICAS

# MZ_CVEGEO es categorica al solo tener cierto numero de manzanas.
df_users['MZ_CVEGEO'] = df_users['MZ_CVEGEO'].astype('category')
df_users['MZ_MZA'] = df_users['MZ_MZA'].astype('category')
df_users['MZ_AGEB'] = df_users['MZ_AGEB'].astype('category')
df_users['MZ_LOC'] = df_users['MZ_LOC'].astype('category')

# Todas las que se refieren a las caracteristicas de las vialidade codigo son categoricas y con un orden.
# Para asignar el orden de estas variable categoricas consultar en notebook de Exploracion 1

# Para el acceso de personas y automoviles, se considera que tiene un mayor peso disponer de este acceso en todas 
# las vialidades que no disponer:

# 3 -> Libre en todas las vialidades
# 2 -> Restringido en alguna vialidad
# 1 -> Restringido en todas las vialidades
# 4 -> no especificado
# 5 -> conjunto habiational

cat_type_2 = CategoricalDtype(categories=[4, 1,5, 2, 3], ordered=True)
df_users['MZ_ACESOPER_'] = df_users['MZ_ACESOPER_'].astype(cat_type_2)
df_users['MZ_ACESOAUT_'] = df_users['MZ_ACESOAUT_'].astype(cat_type_2)

# Para el resto de caractersitticas como el pavimento, etc.

# 1 -> Todas las vialidades
# 2 -> Alguna vialidad
# 3 -> Ninguna vialidad
# 4 -> no especificado
# 5 -> conjunto habiational

cat_type_1 = CategoricalDtype(categories=[4, 3,5, 2, 1], ordered=True)

df_users['MZ_RECUCALL_'] = df_users['MZ_RECUCALL_'].astype(cat_type_1)
df_users['MZ_SENALIZA_'] = df_users['MZ_SENALIZA_'].astype(cat_type_1)
df_users['MZ_ALUMPUB_'] = df_users['MZ_ALUMPUB_'].astype(cat_type_1)
df_users['MZ_TELPUB_'] = df_users['MZ_TELPUB_'].astype(cat_type_1)
df_users['MZ_BANQUETA_'] = df_users['MZ_BANQUETA_'].astype(cat_type_1)
df_users['MZ_GUARNICI_'] = df_users['MZ_GUARNICI_'].astype(cat_type_1)
df_users['MZ_ARBOLES_'] = df_users['MZ_ARBOLES_'].astype(cat_type_1)
df_users['MZ_RAMPAS_'] = df_users['MZ_RAMPAS_'].astype(cat_type_1)
df_users['MZ_PUESSEMI_'] = df_users['MZ_PUESSEMI_'].astype(cat_type_1)
df_users['MZ_PUESAMBU_'] = df_users['MZ_PUESAMBU_'].astype(cat_type_1)

# ------------------ Frentes de Manzanas -----------------
# MZ_FT_CVEGEO      285788 non-null  object  
#  75  MZ_FT_CVEVIAL     285788 non-null  object  
#  77  MZ_FT_TIPOVIAL    285788 non-null  object  
#  78  MZ_FT_CVEFT       285788 non-null  int64   
#  79  MZ_FT_SHAPE_LENG  285788 non-null  float64 


# Convertir a categoricos

# Tipo de via
# Rasgo -> Lado de la manzana que no es vialidad, como canal, arroyo, barranca,
# monte, barda, cerca o límite de parcela, entre otros, siempre en ausencia de una vialidad.
# No especificado < Rasgo < carretera < conjunto habitacional < calle peatonal < andador

cat_type_MZ_1 = CategoricalDtype(categories=[9, 4, 3, 7, 2, 1], ordered=True)
df_users['MZ_FT_TIPOVIA_C'] = df_users['MZ_FT_TIPOVIA_C'].astype(cat_type_MZ_1)


# Acceso de personas y automoviles
# No especificado < no aplica < vialidad restringida < conjunto habitacional < vialidad libre de acceso
cat_type_MZ_2 = CategoricalDtype(categories=[9, 8, 1, 7, 2], ordered=True)
df_users['MZ_FT_ACESOPER_'] = df_users['MZ_FT_ACESOPER_'].astype(cat_type_MZ_2)
df_users['MZ_FT_ACESOAUT_'] = df_users['MZ_FT_ACESOAUT_'].astype(cat_type_MZ_2)


# Para la condiciones del pavimento en la vialidad
# No especificado < no aplica < conjunto habitacional < sin recubrimiento < empedrado o adoquin < pavimento o concreto
cat_type_MZ_3 = CategoricalDtype(categories=[9, 8, 7, 3, 2, 1], ordered=True)
df_users['MZ_FT_RECUCALL_'] = df_users['MZ_FT_RECUCALL_'].astype(cat_type_MZ_3)


# Para el resto de las caracteristicas de las vialidades

cat_type_MZ_4 = CategoricalDtype(categories=[9, 8, 7, 2, 1], ordered=True)
df_users['MZ_FT_SENALIZA_'] = df_users['MZ_FT_SENALIZA_'].astype(cat_type_MZ_4)
df_users['MZ_FT_ALUMPUB_'] = df_users['MZ_FT_ALUMPUB_'].astype(cat_type_MZ_4)
df_users['MZ_FT_TELPUB_'] = df_users['MZ_FT_TELPUB_'].astype(cat_type_MZ_4)
df_users['MZ_FT_BANQUETA_'] = df_users['MZ_FT_BANQUETA_'].astype(cat_type_MZ_4)
df_users['MZ_FT_ARBOLES_'] = df_users['MZ_FT_ARBOLES_'].astype(cat_type_MZ_4)
df_users['MZ_FT_RAMPAS_'] = df_users['MZ_FT_RAMPAS_'].astype(cat_type_MZ_4)
df_users['MZ_FT_PUESSEMI_'] = df_users['MZ_FT_PUESSEMI_'].astype(cat_type_MZ_4)
df_users['MZ_FT_PUESAMBU_'] = df_users['MZ_FT_PUESAMBU_'].astype(cat_type_MZ_4)
df_users['MZ_FT_GUARNICI_'] = df_users['MZ_FT_GUARNICI_'].astype(cat_type_MZ_4)


# Posiblemente podriamos darle un orden categorico igual
df_users['MZ_FT_TIPOVIAL'] = df_users['MZ_FT_TIPOVIAL'].astype('category')


print('Valores despues de conversion de tipos de datos y seleccion')
df_users.info(verbose=True, null_counts=True)

# ------------Feature Engineering---------------


# Mapeamos los valores de tipo suministro a un numero menos de categorias
print('Tipos de suministros, antes ', df_users['tipoSumini'].dtype)
mapping = { 3 : 'baja', 4 : 'baja', 1 : 'alta', 2 : 'alta', 0 : 'desconocido' }
df_users['tipoSumini'] = df_users['tipoSumini'].replace(mapping)
print('Tipos de suministro, despues' ,df_users['tipoSumini'].unique())



# Mapeamos los giro a una nueva categoria DivisionGiro

print('Giros antes', df_users['giro'].value_counts())

mapping_division_giros = {
    r'^0...$': 'Industrias agropecuaria',
    r'^1...$': 'Industrias extractivas',
    r'^2...$': 'Industrias manufactureras',  # Industrias de transformacion
    r'^4...$': 'Construccion',
    r'^5...$': 'Electricidad, gas y vapor',
    r'^60..$': 'Comercio al por mayor o menor',  # Division 6 sector comercial

    r'^6100$': 'Comercio al por mayor o menor',  # Compraventa de articulos alimenticios, bebidas y tabaco
    r'^6101$': 'Comercio al por mayor o menor',  # compraventa de arituclos alimenticios agricolas, no elab
    r'^6102$': 'Comercio al por mayor o menor',  # compraventa prod alimen ganaderia, caza, pesca, no elaborados
    r'^6103$': 'Comercio al por mayor o menor',  # Compraventa de alimentos, bebidas, abarrotes y otros ramos
    r'^6104$': 'Comercio al por mayor o menor',  # compraventa de otros productos alimenticios elaborados
    r'^6105$': 'Comercio al por mayor o menor',  # compraventa de bebidas no alcoholicas
    r'^6106$': 'Comercio al por mayor o menor',  # compraventa de cerveza
    r'^6107$': 'Comercio al por mayor o menor',  # compraventa de compraventa de vinos, aguardientes
    r'^6108$': 'Comercio al por mayor o menor',  # No identificado
    r'^6149$': 'Comercio al por mayor o menor',  # No identificado
    r'^6155$': 'Comercio al por mayor o menor',  # No identificado
    r'^6170$': 'Comercio al por mayor o menor',  # No identificado
    r'^6171$': 'Comercio al por mayor o menor',  # No identificado
    r'^6189$': 'Comercio al por mayor o menor',  # No identificado
    r'^6194$': 'Comercio al por mayor o menor',  # No identificado

    r'^6200$': 'Comercio al por mayor o menor',  # compraventa de arituclos para el hogar y uso personal
    r'^6201$': 'Comercio al por mayor o menor',  # Almacenes y tiendas departamentales autoabastecimiento
    r'^6202$': 'Comercio al por mayor o menor',  # boneterias, sederias y mercerias
    r'^6203$': 'Comercio al por mayor o menor',  # perfumerias y articulos de tocador
    r'^6204$': 'Comercio al por mayor o menor',  # librerias
    r'^6205$': 'Comercio al por mayor o menor',  # compraventa de articulos optica
    r'^6206$': 'Comercio al por mayor o menor',  # instrumentos musicales
    r'^6207$': 'Comercio al por mayor o menor',  # Tiendas de discos
    r'^6208$': 'Comercio al por mayor o menor',  # platerias, joyerias y relojerias
    r'^6209$': 'Comercio al por mayor o menor',  # tiendas regalos y bazares
    r'^6221$': 'Comercio al por mayor o menor',  # Farmacias y boticas
    r'^6231$': 'Comercio al por mayor o menor',  # Compraventa telas, camisires y blancos en general
    r'^6232$': 'Comercio al por mayor o menor',  # compraventa de alfombras, tapetes
    r'^6237$': 'Comercio al por mayor o menor',  # compraventa de alfombras, tapetes
    r'^6251$': 'Comercio al por mayor o menor',  # compraventa aparators, articulos y accesorios electronicos
    r'^6270$': 'Comercio al por mayor o menor',  # No identificado

    r'^620A$': 'Comercio al por mayor o menor',  # compraventa otros articulos para hogar y uso personal

    r'^62C1$': 'Comercio al por mayor o menor',  # Compraventa de cristaleria, loza, porcelana

    r'^6289$': 'Comercio al por mayor o menor',  # No identificado
    r'^62F2$': 'Comercio al por mayor o menor',  # Papeleria y articulos de oficina
    r'^62F1$': 'Comercio al por mayor o menor',  # Compraventa equipo y material fotografico y cinematografico

    r'^62H0$': 'Comercio al por mayor o menor',  # No identificado
    r'^62HA$': 'Comercio al por mayor o menor',  # No identificado
    r'^62H2$': 'Comercio al por mayor o menor',  # Compraventa de ropa para damas
    r'^62H1$': 'Comercio al por mayor o menor',  # Compraventa de ropa para caballeros
    r'^62H3$': 'Comercio al por mayor o menor',  # Compraventa de ropa para niños
    r'^62H4$': 'Comercio al por mayor o menor',  # Zapaterias
    r'^62H5$': 'Comercio al por mayor o menor',  # Cajones de ropa
    r'^62H6$': 'Comercio al por mayor o menor',  # Compraventa de otras prendas de vestir
    r'^62H7$': 'Comercio al por mayor o menor',  # Compraventa de petacas, portafolios y art similares

    r'^62M1$': 'Comercio al por mayor o menor',  # compraventa muebles y accesorios
    r'^62M2$': 'Comercio al por mayor o menor',  # Compraventa de aparatos ortopedicos

    r'^6300$': 'Comercio al por mayor o menor',  # Compraventa de materias primas
    r'^6301$': 'Comercio al por mayor o menor',  # Compraventa de algodon de pluma
    r'^6302$': 'Comercio al por mayor o menor',  # Compraventa de tabaco en ramas
    r'^6303$': 'Comercio al por mayor o menor',  # Compraventa de algodon de pluma
    r'^6304$': 'Comercio al por mayor o menor',  # Compraventa de cueros y pieles
    r'^6305$': 'Comercio al por mayor o menor',  # Compraventa de otras materias primas

    r'^6351$': 'Comercio al por mayor o menor',  # Compraventa de materiales electricos
    r'^6361$': 'Comercio al por mayor o menor',  # Ferreterias
    r'^6362$': 'Comercio al por mayor o menor',  # Tlapaleria

    r'^63A1$': 'Comercio al por mayor o menor',  # Compraventa de metales
    r'^63C1$': 'Comercio al por mayor o menor',  # Compraventa de Materias primas de ganaderi
    r'^63D1$': 'Comercio al por mayor o menor',  # materias primas de ganaderia
    r'^63D2$': 'Comercio al por mayor o menor',  # Compraventa de semillas todas clases
    r'^63D3$': 'Comercio al por mayor o menor',  # Compraventa de abonos, fertilizantes, insecticidas
    r'^63D4$': 'Comercio al por mayor o menor',  # Compraventa de aanilinas, aniles, tintes
    r'^63D5$': 'Comercio al por mayor o menor',  # Compraventa de otros productos quimicos
    r'^63D6$': 'Comercio al por mayor o menor',  # Compraventa de papel y carton
    r'^63D7$': 'Comercio al por mayor o menor',  # Compraventa de pinturas, lacas barnices
    r'^63D8$': 'Comercio al por mayor o menor',  # Vidrerias
    r'^63D9$': 'Comercio al por mayor o menor',  # Madererias
    r'^63M1$': 'Comercio al por mayor o menor',  # Compraventa de muebles sanitarios

    r'^6323$': 'Comercio al por mayor o menor',  # No identificado
    r'^6374$': 'Comercio al por mayor o menor',  # No identificado
    r'^6399$': 'Comercio al por mayor o menor',  # No identificado
    r'^63B8$': 'Comercio al por mayor o menor',  # No identificado
    r'^63E1$': 'Comercio al por mayor o menor',  # No identificado
    r'^63W1$': 'Comercio al por mayor o menor',  # No identificado
    r'^63Y0$': 'Comercio al por mayor o menor',  # No identificado

    r'^6403$': 'Comercio al por mayor o menor',  # No identificado
    r'^6431$': 'Comercio al por mayor o menor',  # No identificado
    r'^64NN$': 'Comercio al por mayor o menor',  # No identificado
    r'^6400$': 'Comercio al por mayor o menor',  # Compraventa de maquinaria, implementoos, aparatos
    r'^64M1$': 'Comercio al por mayor o menor',  # Compraventa de muebles y equipos para oficina
    r'^64W1$': 'Comercio al por mayor o menor',  # compraventa para labores agropecuarias
    r'^64W2$': 'Comercio al por mayor o menor',  # Compraventa para industricas, excepto construccion
    r'^64W3$': 'Comercio al por mayor o menor',  # Compraventa para la industria de construccion
    r'^64W4$': 'Comercio al por mayor o menor',  # Compraventa prefacciones partes para maquinas
    r'^64Y1$': 'Comercio al por mayor o menor',  # Compraventa de aparator, instrumentos medicos

    r'^6503$': 'Comercio al por mayor o menor',  # No identificado
    r'^6520$': 'Comercio al por mayor o menor',  # No identificado
    r'^6533$': 'Comercio al por mayor o menor',  # No identificado
    r'^6592$': 'Comercio al por mayor o menor',  # No identificado
    r'^65D3$': 'Comercio al por mayor o menor',  # No identificado
    r'^65M3$': 'Comercio al por mayor o menor',  # No identificado
    r'^65NN$': 'Comercio al por mayor o menor',  # No identificado

    r'^6521$': 'Comercio al por mayor o menor',  # Iusacel
    r'^6521$': 'Comercio al por mayor o menor',  # radiomovil dipsa

    r'^6500$': 'Comercio al por mayor o menor',  # compraventa de rvehiculos y refacciones y accesorios
    r'^65D1$': 'Comercio al por mayor o menor',  # compraventa de camaras y llantas

    r'^6511$': 'Comercio al por mayor o menor',  # Compraventa de refacciones, partes nuevos
    r'^6512$': 'Comercio al por mayor o menor',  # Compraventa de refacciones, partes usados
    r'^65P1$': 'Comercio al por mayor o menor',  # Compraventa de automoviles y camiones nuevos
    r'^65P2$': 'Comercio al por mayor o menor',  # Compraventa de automoviles y camiones usados
    r'^65P3$': 'Comercio al por mayor o menor',  # Compraventa de otros vehiculos, refacciones y accesorios

    r'^6600$': 'Comercio al por mayor o menor',  # distribuidores de gas combustible
    r'^66D1$': 'Comercio al por mayor o menor',  # expendios de lubricantes
    r'^66D2$': 'Comercio al por mayor o menor',  # expendios de petroleos y tractolina
    r'^66D3$': 'Comercio al por mayor o menor',  # estaciones de gasolina
    r'^66D4$': 'Comercio al por mayor o menor',  # compraventa de leña, carbon y mineral

    r'^6603$': 'Comercio al por mayor o menor',  # No identificado
    r'^6623$': 'Comercio al por mayor o menor',  # No identificado
    r'^6624$': 'Comercio al por mayor o menor',  # No identificado
    r'^6638$': 'Comercio al por mayor o menor',  # No identificado
    r'^6647$': 'Comercio al por mayor o menor',  # No identificado
    r'^6665$': 'Comercio al por mayor o menor',  # No identificado
    r'^6666$': 'Comercio al por mayor o menor',  # No identificado

    r'^6700$': 'Comercio al por mayor o menor',  # Compraventa de articulos diversos
    r'^6702$': 'Comercio al por mayor o menor',  # compraventa de otros articulos diversos
    r'^6701$': 'Comercio al por mayor o menor',  # compraventa de terrenos y edificios

    r'^6800$': 'Comercio al por mayor o menor',  # No identificado
    r'^6859$': 'Comercio al por mayor o menor',  # No identificado
    r'^6864$': 'Comercio al por mayor o menor',  # No identificado
    r'^6896$': 'Comercio al por mayor o menor',  # No identificado
    r'^6903$': 'Comercio al por mayor o menor',  # No identificado
    r'^6937$': 'Comercio al por mayor o menor',  # No identificado
    r'^6YL3$': 'Comercio al por mayor o menor',  # No identificado
    r'^6J02$': 'Comercio al por mayor o menor',  # No identificado

    #     r'^6...$': 'Sector comercial',
    #     r'^U...$': 'Sector comercial',
    #     r'^UUU.$': 'Comercio al por mayor o menor',
    #     r'^UOOO$': 'Comercio al por mayor o menor',
    #     r'^U00.$': 'Comercio al por mayor o menor',
    #     r'^U10.$': 'Comercio al por mayor o menor',
    #     r'^U00$': 'Comercio al por mayor o menor',
    #     r'^WY01$': 'Comercio al por mayor o menor',
    #     r'^U100$': 'Comercio al por mayor o menor', # radiomovil dipsa
    #     r'^U1OO$': 'Comercio al por mayor o menor', # radiomovil dipsa

    r'^7...$': 'Transportes, correos y almacenamiento',  # Sector de transportes
    r'^9...$': 'Sector residencial',

    #     r'^M...$': 'Sector suministros',

    r'^\*\*01$': 'Dependencias diversas de la administracion',
    r'^\*001$': 'Dependencias diversas de la administracion',
    r'^01$': 'Dependencias diversas de la administracion',
    r'^1$': 'Dependencias diversas de la administracion',

    r'^\*\*03$': 'Servicios de salud y de asistencia social',  # Sanatorios
    r'^\*\*04$': 'Servicios de salud y de asistencia social',  # Clinicas
    r'^\*\*05$': 'Servicios de salud y de asistencia social',  # Otros suministros medicos y veterinarios
    r'^\*\*56$': 'Servicios de salud y de asistencia social',  # Consultorios medicos diversos
    r'^\*\*45$': 'Servicios de salud y de asistencia social',  # Maternidades
    r'^\*\*46$': 'Servicios de salud y de asistencia social',  # Asilos y casas de salud
    r'^\*\*47$': 'Servicios de salud y de asistencia social',  # Laboratorios de analisis clinicos
    r'^M5..$': 'Servicios de salud y de asistencia social',  # Suministros de asistencia medica
    r'^M946$': 'Servicios de salud y de asistencia social',  # Asilos y casas de salud
    r'^M949$': 'Servicios de salud y de asistencia social',  #
    r'^MX20$': 'Servicios de salud y de asistencia social',  #
    r'^L046$': 'Servicios de salud y de asistencia social',  # Asilos y casas de salud
    r'^L102$': 'Servicios de salud y de asistencia social',  # Institutos de salud
    r'^LE04$': 'Servicios de salud y de asistencia social',  # Institutos de salud

    r'^\*006$': 'Servicios educativos',  # Jardines de niños
    r'^\*\*06$': 'Servicios educativos',  # Jardines de niños
    r'^\*\*07$': 'Servicios educativos',  # Escuelas
    r'^\*\*08$': 'Servicios educativos',  # Academias
    r'^\*\*09$': 'Servicios educativos',  # Institutos
    r'^09$': 'Servicios educativos',  # Institutos
    r'^\*\*41$': 'Servicios educativos',  # Escuelas mixtas
    r'^\*\*10$': 'Servicios educativos',  # Universidades
    r'^\*\*11$': 'Servicios educativos',  # Otros suministros educativos y culturales
    r'^M011$': 'Servicios educativos',  # Otros suministros educativos y culturales
    r'^\*011$': 'Servicios educativos',  # Otros suministros educativos y culturales
    r'^11$': 'Servicios educativos',  # Otros suministros educativos y culturales
    r'^\*\*12$': 'Servicios educativos',  # Institutos
    r'^M3..$': 'Servicios educativos',  # Suministros privados de enseñanza
    r'^M910$': 'Servicios educativos',  # Centros educativos y culturales
    r'^M911$': 'Servicios educativos',  # Centros educativos y culturales
    r'^MD40$': 'Servicios educativos',  # Centros educativos y culturales
    r'^LD01$': 'Servicios educativos',  # SEP Escuelas
    r'^LD07$': 'Servicios educativos',  # SEP Escuelas
    r'^L039$': 'Servicios educativos',  # SEP Escuelas
    r'^L040$': 'Servicios educativos',  # SEP Escuelas
    r'^L041$': 'Servicios educativos',  # SEP Escuelas
    r'^LD39$': 'Servicios educativos',  # SEP Escuelas
    r'^LD40$': 'Servicios educativos',  # SEP Escuelas
    r'^L011$': 'Servicios educativos',  # SEP Escuelas
    r'^L107$': 'Servicios educativos',  # CONALEP
    r'^L110$': 'Servicios educativos',  # CONALEP
    r'^L111$': 'Servicios educativos',  # CONALEP
    r'^L129$': 'Servicios educativos',  # CONALEP
    r'^L140$': 'Servicios educativos',  #
    r'^LD0.$': 'Servicios educativos',  #
    r'^LD10$': 'Servicios educativos',  #
    r'^LD29$': 'Servicios educativos',  #
    r'^LDF2$': 'Servicios educativos',  #
    r'^LZES$': 'Servicios educativos',  #
    r'^KD06$': 'Servicios educativos',  # IPN
    r'^KD39$': 'Servicios educativos',  #

    r'^\*\*13$': 'Servicios de esparcimiento',  # Clubs
    r'^\*\*17$': 'Servicios de esparcimiento',  # Otros centros sociales y de esparcimiento
    r'^\*\*22$': 'Servicios de esparcimiento',  # Balnearios y albercas
    r'^\*\*23$': 'Servicios de esparcimiento',  # Billares y bloiches
    r'^\*\*24$': 'Servicios de esparcimiento',  # Centros nocturnos
    r'^\*\*25$': 'Servicios de esparcimiento',  # Centros recreativos y casinos
    r'^\*\*27$': 'Servicios de esparcimiento',  # Plazas de toros
    r'^\*\*29$': 'Servicios de esparcimiento',  # Otros centros de exhibicion
    r'^\*\*28$': 'Servicios de esparcimiento',  # Arenas de box de lucha y frontones
    r'^28$': 'Servicios de esparcimiento',  # Arenas de box de lucha y frontones
    r'^M1..': 'Servicios de esparcimiento',  # Suministros de esparcimiento
    r'^M91.': 'Servicios de esparcimiento',  # Clubes
    r'^CP16$': 'Servicios de esparcimiento',  # Cinepolis
    r'^CR16$': 'Servicios de esparcimiento',  # Cinemas de la rep
    r'^L117$': 'Servicios de esparcimiento',  #
    r'^LDF1$': 'Servicios de esparcimiento',  #

    r'^\*\*30$': 'Informacion en medios masivos',  # Estaciones de radio
    r'^\*\*32$': 'Informacion en medios masivos',  # Editoriales que no tienen talleres propios
    r'^\*\*31$': 'Informacion en medios masivos',  # Estaciones de television
    r'^\*\*81$': 'Informacion en medios masivos',  # Produccion y distribicion de peliculas
    r'^MY63$': 'Informacion en medios masivos',  # R.T.V MEX NTE
    r'^MY64$': 'Informacion en medios masivos',  # TV azteca
    r'^MY76$': 'Informacion en medios masivos',  # Alestra
    r'^AL51$': 'Informacion en medios masivos',  # Alestra
    r'^MC31$': 'Informacion en medios masivos',  # Telefonia por cable sa de cv
    r'^MS0.$': 'Informacion en medios masivos',  # Pegaso sa de cv Movistar
    r'^U102$': 'Informacion en medios masivos',  # Pegaso sa de cv Movistar
    r'^MA01$': 'Informacion en medios masivos',  # American tower
    r'^MA30$': 'Informacion en medios masivos',  # Varios
    r'^MU00$': 'Informacion en medios masivos',  # Varios
    r'^MX01$': 'Informacion en medios masivos',  # Varios
    r'^PT01$': 'Informacion en medios masivos',  # Grupo ATT
    r'^U10.$': 'Informacion en medios masivos',  # Radiomovil dipsa

    r'^\*\*34$': 'Servicios profesionales, científicos y técnicos',  # Otros conjuntos profesionales
    r'^\*\*53$': 'Servicios profesionales, científicos y técnicos',  # Despachos de abogados
    r'^\*\*54$': 'Servicios profesionales, científicos y técnicos',  # Despachos de contadores
    r'^M054$': 'Servicios profesionales, científicos y técnicos',  # Despachos de contadores
    r'^\*\*55$': 'Servicios profesionales, científicos y técnicos',  # Despachos de ingenieros
    r'^\*\*58$': 'Servicios profesionales, científicos y técnicos',  # Despachos de investigacion
    r'^\*\*59$': 'Servicios profesionales, científicos y técnicos',  # agencias aduanales
    r'^\*\*60$': 'Servicios profesionales, científicos y técnicos',  # agencias de publicidad
    r'^\*\*61$': 'Servicios profesionales, científicos y técnicos',  # agencias de turismo y viajes
    r'^\*\*62$': 'Servicios profesionales, científicos y técnicos',  # Corredores de bienes raices
    r'^\*\*63$': 'Servicios profesionales, científicos y técnicos',  # Otros tipos de agencias
    r'^M963$': 'Servicios profesionales, científicos y técnicos',  # Otros tipos de agencias
    r'^M962$': 'Servicios profesionales, científicos y técnicos',  # Otros tipos de agencias
    r'^\*\*52$': 'Servicios profesionales, científicos y técnicos',  # Notarias
    r'^M7..$': 'Servicios profesionales, científicos y técnicos',
    # Suministros de profesionistas agencias y de presentacion
    r'^R1NR$': 'Servicios profesionales, científicos y técnicos',  # Agencia municipal

    r'^\*\*77$': 'Servicios inmobiliarios y de alquiler',  # Alquiler frigorificos
    r'^\*\*78$': 'Servicios inmobiliarios y de alquiler',  # Almacenaje y bodegas
    r'^\*\*79$': 'Servicios inmobiliarios y de alquiler',  # Estacionamientos y pensiones de vehiculos
    r'^MY79$': 'Servicios inmobiliarios y de alquiler',  # Estacionamientos, pensiones de vehiculos
    r'^M979$': 'Servicios inmobiliarios y de alquiler',  # Estacionamientos, pensiones de vehiculos
    r'^\*\*80$': 'Servicios inmobiliarios y de alquiler',  # Alquiler y divsersos

    r'^\*\*37$': 'Servicios de alojamiento temporal',  # Casas de huespedes
    r'^\*\*35$': 'Servicios de alojamiento temporal',  # Moteles
    r'^\*014$': 'Servicios de alojamiento temporal',  # Hoteles
    r'^\*\*14$': 'Servicios de alojamiento temporal',  # Hoteles
    r'^M2..$': 'Servicios de alojamiento temporal',
    # Suministros de alojamiento temporal hoteles, moteles, posadas, etc

    r'^\*\*64$': 'Servicios de preparación de alimentos y bebidas',  # Restaurante, cafes y fondas
    r'^\*\*65$': 'Servicios de preparación de alimentos y bebidas',  # Loncherias y taquerias
    r'^\*\*66$': 'Servicios de preparación de alimentos y bebidas',  # Neverias y refrequerias
    r'^\*\*67$': 'Servicios de preparación de alimentos y bebidas',  # Cantinas y bares
    r'^\*\*68$': 'Servicios de preparación de alimentos y bebidas',  # Cervecerias
    r'^\*\*69$': 'Servicios de preparación de alimentos y bebidas',  # Pulquerias
    r'^\*\*70$': 'Servicios de preparación de alimentos y bebidas',
    # Otris servicios de preparacion y venta de alimentos
    r'^M8..$': 'Servicios de preparación de alimentos y bebidas',
    # Suministros de preparacion y venta de alimentos y bebida

    # 812
    r'^\*\*82$': 'Servicios personales',  # Peluquerias
    r'^\*\*83$': 'Servicios personales',  # Salones de belleza
    r'^\*\*75$': 'Servicios Personales',  # Agencias funerarias
    r'^MY75$': 'Servicios Personales',  # Agencias funerarias
    r'^M975$': 'Servicios Personales',  # Agencias funerarias
    r'^\*\*76$': 'Servicios Personales',  # Panteones y cementerios
    r'^M650$': 'Servicios Personales',  # Baños publicos
    r'^M680$': 'Servicios Personales',  # Tintorerias
    r'^M682$': 'Servicios Personales',  # Peluqerias
    r'^M683$': 'Servicios Personales',  # Salones de belleza
    r'^MY82$': 'Servicios Personales',  # Peluqerias

    # 813
    r'^\*\*72$': 'Asociaciones y organizaciones',  # Asociaciones sindicales y politicas
    r'^\*\*73$': 'Asociaciones y organizaciones',  # Asociaciones religiosas
    r'^M973$': 'Asociaciones y organizaciones',  # Asociaciones religiosas
    r'^\*\*74$': 'Asociaciones y organizaciones',  # Otras asociaciones no lucrativas
    r'^M972$': 'Asociaciones y organizaciones',  # Oficinas de partidos politicos
    r'^M974$': 'Asociaciones y organizaciones',  # Oficinas administrativas (No lucrativas)
    r'^MY73$': 'Asociaciones y organizaciones',  # Iglesias, templos (asociaciones religiosas)
    r'^M071$': 'Asociaciones y organizaciones',  # Camaras industriales y de comercio

    # 811
    r'^\*\*51$': 'Servicios de reparacion y mantenimiento',  # Autolavados
    r'^M651$': 'Servicios de reparacion y mantenimiento',  # Lavado, engrasado, pulido y encerado de autos
    r'^MY80$': 'Servicios de reparacion y mantenimiento',  # Estudios, fotografica, reparacion radios y tv
    r'^M006$': 'Servicios de reparacion y mantenimiento',  # Refaccionaria

    # 2213
    r'^\*\*18$': 'Captación, tratamiento y suministro de agua',  # Bombeo de aguas potables y negras
    r'^MY18$': 'Captación, tratamiento y suministro de agua',  # Bombeo de aguas potables y negras
    r'^M018$': 'Captación, tratamiento y suministro de agua',  # Bombeo de aguas potables y negras
    r'^R018$': 'Captación, tratamiento y suministro de agua',  # Bombeo de aguas potable y negras
    r'^RSMP$': 'Captación, tratamiento y suministro de agua',  # Bombeo de aguas potable y negras
    r'^RZB.$': 'Captación, tratamiento y suministro de agua',  # Bombeo de aguas potable y negras

    # 31
    r'^\*\*21$': 'Industrias manufactureras',  # Rastros, frigorificos y otros
    r'^MY21$': 'Industrias manufactureras',  # Rastros, frigorificos y otros
    r'^R021$': 'Industrias manufactureras',  # Rastros, frigorificos y otros
    r'^\*\*84$': 'Industrias manufactureras',  # Fabricas de hielo
    r'^\*\*96$': 'Industrias manufactureras',  # plantas industriales de basura
    r'^MY84$': 'Industrias manufactureras',  # Fabricas de hielo
    r'^MY02$': 'Industrias manufactureras',  # Singer

    # 23 Construccion
    r'^\*\*20$': 'Alumbrado publico',  # 237212 SCIAN
    r'^MY20$': 'Alumbrado publico',  # 237212 SCIAN
    r'R120': 'Alumbrado publico',  #
    r'R020': 'Alumbrado publico',  #

    # 52 Servicios financieros y de seguros
    r'^M4..$': 'Servicios financieros y de seguros',  # Suministros de credito, seguros y fianzas
    r'^M042$': 'Servicios financieros y de seguros',  # Bancos
    r'^BM01$': 'Servicios financieros y de seguros',  # Banamex
    r'^BI01$': 'Servicios financieros y de seguros',  # HSBC
    r'^BN01$': 'Servicios financieros y de seguros',  # Banorte
    r'^BS01$': 'Servicios financieros y de seguros',  # Santander
    r'^BS00$': 'Servicios financieros y de seguros',  # Santander
    r'^BN42$': 'Servicios financieros y de seguros',  # Bancrecer
    r'^BN00$': 'Servicios financieros y de seguros',  # Banco mercantil del norte
    r'^NM01$': 'Servicios financieros y de seguros',  # Nacional monte de piedad
    r'^BA42$': 'Servicios financieros y de seguros',  # Banco ahorro
    r'^FID6$': 'Servicios financieros y de seguros',  # Financiera Independiente

    # 93 Actividades legislativas, gubernamentales, de impartición de justicia y de organismos internacionales y extraterritoriales
    r'^\*\*85$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # Juzgados
    r'^\*\*86$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # Delegaciones policiacas
    r'^M086$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # Delegaciones policiacas
    r'^\*\*87$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # Carceles-reclusorios
    r'^\*\*88$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # plazas publicas
    r'^\*\*93$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # Casas de jueces auxiliares
    r'^M971$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R190$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  # Cuerpo de bomberos
    r'^R001$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R034$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R101$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R103$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R118$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R121$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R178$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R186$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^R190$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L001$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L101$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L078$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L103$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L108$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L109$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L126$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L128$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L146$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L148$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L2H6$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L601$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L6K6$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L6K7$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L8D6$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LD12$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LE01$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L017$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L187$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^L189$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LEG4$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LFDE$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LGT2$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LIC7$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LIE1$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^LWN4$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^K[1-9]..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KA..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KB..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KD0[1-5]$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KD18$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KD29$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KF..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KH..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KL..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KR..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KW..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^KY..$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^K001$': 'Actividades legislativas, gubernamentales, de impartición de justicia',  #
    r'^T...$': 'Actividades legislativas, gubernamentales, de impartición de justicia',

    # Comercio al por menor 46
    r'^\*\*89$': 'Comercio al por mayor o menor',  # Tiendas de autoservicio
    r'^MY89$': 'Comercio al por mayor o menor',  # Tiendas de autoservicio
    r'^M689$': 'Comercio al por mayor o menor',  # No identificado
    r'^MY01$': 'Comercio al por mayor o menor',  # Tiendas de aurrera
    r'^CA01$': 'Comercio al por mayor o menor',  # Tiendas de waltmart
    r'^M011$': 'Comercio al por mayor o menor',  # Material educativo
    r'^ME..$': 'Comercio al por mayor o menor',  # Elektra
    r'^MV01$': 'Comercio al por mayor o menor',  # Grupo algedi
    r'^FS01$': 'Comercio al por mayor o menor',  # Farmacias
    r'^FS92$': 'Comercio al por mayor o menor',  # Farmacias
    r'^OD01$': 'Comercio al por mayor o menor',  # Opticas
    r'^PD62$': 'Comercio al por mayor o menor',  # Autosrvicio
    r'^GA00$': 'Comercio al por mayor o menor',  # Autosrvicio
    r'^OFM1$': 'Comercio al por mayor o menor',  # Autosrvicio
    r'^DLH2$': 'Comercio al por mayor o menor',  # Tienda de ropa
    r'^SU31$': 'Comercio al por mayor o menor',  # TSuburbia

    # Comercio al por mayor 46
    r'^MY83$': 'Comercio al por mayor o menor',  # Proveedora de muebles
    r'^M007$': 'Comercio al por mayor o menor',  # Proveedora del panadero
    r'^CO31$': 'Comercio al por mayor o menor',  # Comercializadora

    # 56 Servicios de apoyo a los negocios y manejo de residuos, y servicios de remediación 56142
    r'^MT..$': 'Servicios de apoyo a los negocios',  # OFICINAS, LOCALES, CASETAS Y CENTRALES DE TELEFONIA SS

    # 48-49 Transportes, correos y almacenamiento
    r'^MY77$': 'Transportes, correos y almacenamiento',  # TFM SA DE CV
    r'^MY78$': 'Transportes, correos y almacenamiento',  # ALMACENES DE SEMILLAS Y CEREALES - ALMACENAMIENTO GRAL

    # Suministros diversos
    r'^M602$': 'Suministros diversos',  # No identificado
    r'^M667$': 'Suministros diversos',  # No identificado
    r'^M674$': 'Suministros diversos',  # No identificado
    r'^M681$': 'Suministros diversos',  # No identificado
    r'^M684$': 'Suministros diversos',  # No identificado
    r'^M687$': 'Suministros diversos',  # No identificado
    r'^M000$': 'Suministros diversos',  # No identificado
    r'^M001$': 'Suministros diversos',  # No identificado
    r'^M014$': 'Suministros diversos',  # No identificado
    r'^M063$': 'Suministros diversos',  # No identificado
    r'^M901$': 'Suministros diversos',  # No identificado
    r'^MB62$': 'Suministros diversos',  # No identificado
    r'^MF01$': 'Suministros diversos',  # No identificado
    r'^MY00$': 'Suministros diversos',  # No identificado
    r'^MY04$': 'Suministros diversos',  # No identificado
    r'^MY05$': 'Suministros diversos',  # No identificado
    r'^MY06$': 'Suministros diversos',  # No identificado
    r'^MY09$': 'Suministros diversos',  # No identificado
    r'^MY10$': 'Suministros diversos',  # No identificado
    r'^MY11$': 'Suministros diversos',  # No identificado
    r'^MY12$': 'Suministros diversos',  # No identificado
    r'^MY17$': 'Suministros diversos',  # No identificado
    r'^MY19$': 'Suministros diversos',  # No identificado
    r'^MY23$': 'Suministros diversos',  # No identificado
    r'^MY24$': 'Suministros diversos',  # No identificado
    r'^MY28$': 'Suministros diversos',  # No identificado
    r'^MY3.$': 'Suministros diversos',  # No identificado
    r'^MY4.$': 'Suministros diversos',  # No identificado
    r'^MY5.$': 'Suministros diversos',  # No identificado
    r'^MY60$': 'Suministros diversos',  # No identificado
    r'^MY62$': 'Suministros diversos',  # No identificado
    r'^MY65$': 'Suministros diversos',  # No identificado
    r'^MY66$': 'Suministros diversos',  # No identificado
    r'^MY67$': 'Suministros diversos',  # No identificado
    r'^MY68$': 'Suministros diversos',  # No identificado
    r'^MY72$': 'Suministros diversos',  # No identificado
    r'^MY74$': 'Suministros diversos',  # No identificado
    r'^MY81$': 'Suministros diversos',  # No identificado
    r'^MY9.$': 'Suministros diversos',  # No identificado
    r'^WY01$': 'Suministros diversos',  # No identificado
    r'U000$': 'Suministros diversos',  # No identificado
    r'U001$': 'Suministros diversos',  # No identificado
    r'U003$': 'Suministros diversos',  # No identificado
    r'U00$': 'Suministros diversos',  # No identificado
    r'U00$': 'Suministros diversos',  # No identificado
    r'UUU0$': 'Suministros diversos',  # No identificado
    r'UOOO$': 'Suministros diversos',  # No identificado

    r'^AT75$': 'Giro no identificado',  # No identificado
    r'^3Y80$': 'Giro no identificado',  # No identificado
    r'^S001$': 'Giro no identificado',  # No identificado
    r'^99$': 'Giro no identificado',  # No identificado
    r'^A002$': 'Giro no identificado',  # No identificado
    r'^N180$': 'Giro no identificado',  # No identificado
    r'^G103$': 'Giro no identificado',  # No identificado
    r'^3211$': 'Giro no identificado',  # No identificado
    r'^3M83$': 'Giro no identificado',  # No identificado
    r'^XX$': 'Giro no identificado',  # No identificado

}

df_users['divisionGiro'] = df_users['giro'].str.upper().replace(regex=mapping_division_giros)
pd.set_option('display.max_rows', None)
print('Nueva categoria DivisionGiro', df_users['divisionGiro'].value_counts())
pd.set_option('display.max_rows', 60)


# Mapeamos las tarifas en una nueva categoria tipoTarifa

print('Tarifas: ', df_users['tarifa'].value_counts())
mapping_tipo_tarifa = {
    r'^01$': 'Domestico',
    r'^1.$': 'Domestico',
    r'^02$': 'General Baja tension',
    r'^2P$': 'General Baja tension',
    r'^03$': 'General Baja tension',
    r'^5A$': 'Servicios publicos',
    r'^06$': 'Servicios publicos',
    r'^68$': 'General Ordinaria Media Tension',
    r'^78$': 'General Horaria Media tension',
    r'^09$': 'Agricolas',
    r'^9C$': 'Agricolas',
    r'^9M$': 'Agricolas',
}
df_users['tipoTarifa'] = df_users['tarifa'].str.upper().replace(regex=mapping_tipo_tarifa)

print('Tipo Tarifa', df_users['tipoTarifa'].value_counts())


df_users.to_csv(ARCHIVO_SALIDA)

end = time.time()

print('Tiempo total de ejecución', end-start)

if(GENERAR_GRAFICAS):

    # Funcion para graficar la distribucion acumulada
    def ecdf(data):
        """Compute ECDF for a one-dimensional array of measurements."""
        # Number of data points: n
        n = len(data)
        # x-data for the ECDF: x
        x = np.sort(data)
        # y-data for the ECDF: y
        y = np.arange(1, n+1) / n
        return x, y
    # paper, notebook, talk, poster... (small to large)

    # Gráfica "Proporciones de distribución de hilos por tarifa"

    # Se divide el data frame entre la suma de cada columna
    counts_df = df_users.groupby(['tarifa','hilos'])['rpu'].count().unstack()
    counts_df_percents_df = counts_df.T.div(counts_df.sum(axis=1), axis='columns').T
    
    sns.set_palette("tab10")
    sns.set_context("talk")
    fig, ax = plt.subplots()
    fig.set_size_inches([15,7])
    counts_df_percents_df.plot(kind='bar', stacked=True, ax=ax)
    ax.legend(loc='center right')
    ax.set_title('Proporciones de distribucion de hilos por tarifa')
    ax.set_xlabel('Tarifas')
    ax.grid(linestyle='--')
    plt.show()

    # Gráfica "Mediana del consumo promedio diario por tarifa e hilos"

    sns.set_style("whitegrid")
    sns.set_palette("tab10")
    sns.set_context("talk")
    g = sns.catplot(x="tarifa", 
                    y='consumoPro', 
                    hue="hilos",
                    data=df_users, 
                    kind='bar', 
                    ci=None,
                    estimator = median,
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Mediana del consumo promedio diario por tarifa e hilos', y=1.03)
    g.set(ylabel="Consumo promedio diario (kwh)",
        xlabel="Tarifas")
    plt.xticks(rotation=90)
    g.set(ylim=(-10,120))
    plt.show()

    # Gráfica "Mediana del consumo promedio diario por tarifa e hilos"
    sns.set_palette("tab10")
    sns.set_context("talk")
    g = sns.catplot(x="tarifa",
                    y="consumoPro",
                    hue="hilos",
                    kind="strip",
                    data=df_users,
                    dodge=True,
                    height=8,
                    alpha=0.3,
                    aspect=2 # aspect*height <- width
                )
    g.fig.suptitle('Distribución de los consumos promedios por tarifa e hilos', y=1.03)
    g.set(ylabel="Consumo promedio diario (kwh)",
        xlabel="Tarifas")
    g.set(ylim=(-10,1000))
    plt.show()

    # Multiples graficas de diagramas de violin tarifas e hilos
    fig, ax = plt.subplots(8,2)
    fig.set_size_inches([15,60])
    row=0
    for index,tarifa in enumerate(list(df_users['tarifa'].unique())):
        if index > 7:
            col = 1
        else:
            col = 0
            
        if index == 8:
            row=0
            
        sns.violinplot(x="hilos",
                        y="consumoPro",
                        data=df_users[df_users['tarifa'] == tarifa],
                        dodge=True,
                        ax=ax[row,col],
                    )
        ax[row,col].set_title('Tarifa: '+ str(tarifa))
        ax[row,col].set(xlabel="Hilos", ylabel="Consumo promedio diario (kwh)")
        row = row + 1

    # Multiples graficas de diagramas de caja tarifas e hilos
    fig, ax = plt.subplots(8,2)
    fig.set_size_inches([15,60])
    row=0
    for index,tarifa in enumerate(list(df_users['tarifa'].unique())):
        if index > 7:
            col = 1
        else:
            col = 0
            
        if index == 8:
            row=0
            
        sns.boxplot(x="hilos",
                        y="consumoPro",
                        data=df_users[df_users['tarifa'] == tarifa],
                        dodge=True,
                        ax=ax[row,col],
                    sym=""
                    )
        ax[row,col].set_title('Tarifa: '+ str(tarifa))
        ax[row,col].set(xlabel="Hilos", ylabel="Consumo promedio diario (kwh)")
        row = row + 1

    # Funcion de distribucion acumulada del consumo promedio diario
    x, y = ecdf(df_users['consumoPro'])
    fig, ax = plt.subplots()
    fig.set_size_inches([15,5])
    ax.plot(x,y, marker='.', linestyle='none', alpha=0.5)
    ax.set_title('Funcion de distribución acumulada')
    ax.set_xlabel('Consumo promedio diario (kwh)')
    ax.set_ylabel('CDF')
    ax.set_xscale('log')
    ax.grid(linestyle='--')
    plt.show()

    # Funciones de distribuciones acumladas por tarifas _1
    fig, ax = plt.subplots()
    fig.set_size_inches([15,7])

    for tarifa in list(df_users['tarifa'].unique())[:8]:
        x, y = ecdf(df_users[df_users['tarifa'] == tarifa]['consumoPro'])
        ax.plot(x,y, marker='.', linestyle='none', alpha=0.5, label=tarifa)

    ax.legend(loc='center right')
    ax.set_title('Funcion de distribución acumulada')
    ax.set_xlabel('Consumo promedio diario (kwh)')
    ax.set_ylabel('CDF')
    ax.set_xscale('log')
    ax.grid(linestyle='--')
    plt.show()

    # Distribuciones de los datos KDE
    fig, ax = plt.subplots(8,2)
    fig.set_size_inches([20,80])
    row=0
    for index,tarifa in enumerate(list(df_users['tarifa'].unique())):
        if index > 7:
            col = 1
        else:
            col = 0
            
        if index == 8:
            row=0
            
        sns.kdeplot(x="consumoPro",
                    data=df_users[(df_users['consumoPro'] > 0) & (df_users['tarifa'] == tarifa)],
                    hue="hilos",
                    ax=ax[row,col],
                    log_scale=True,
                    fill=True,
                    alpha=.5,
                    linewidth=0,
                    )
        ax[row,col].set_title('Tarifa: '+ str(tarifa))
        ax[row,col].set(xlabel="Hilos", ylabel="Consumo promedio diario (kwh)")
        row = row + 1

    # Distribuciones espacial de los usuarios
    sns.set_style("whitegrid")
    sns.set_palette("Set2")
    sns.set_context("poster")

    g = sns.relplot(x="y",
                y="x",
                data=df_users, 
                alpha=0.2,
                height=15, 
                aspect=1)

    g.fig.suptitle('Distribución espacial de los usuarios', y=1.03)
    g.set(ylabel="Y",
        xlabel="X")

    plt.show()

    # Distribucion espacial de los usuarios por tipo de via
    sns.set_style("whitegrid")
    sns.set_palette("Set2")

    # paper, notebook, talk, poster... (small to large)
    sns.set_context("poster")

    g = sns.relplot(x="y",
                y="x",
                hue="MZ_FT_TIPOVIA_C",
                data=df_users, 
                alpha=0.3,
                height=15, 
                aspect=1)

    g.fig.suptitle('Distribución espacial de los usuarios por tipo de via', y=1.03)
    g.set(ylabel="Y",
        xlabel="X")

    plt.show()

    # Usuarios por tipo de via
    sns.set_style("whitegrid")
    sns.set_palette("tab10")
    g = sns.catplot(x="MZ_FT_TIPOVIAL", 
                    data=df_users, 
                    kind='count', 
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Usuarios por tipo de via', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Tipo de via")

    plt.xticks(rotation=90)
    plt.show()

    # Usuarios por tipo de pavimento en calles
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="MZ_FT_RECUCALL_", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['MZ_FT_RECUCALL_'].value_counts().index,
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Usuarios por tipo de pavimento en calles', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Tipo de pavimento"
        )
    g.set_xticklabels([
        "Pavimento o concreto",
        "Sin recubrimiento",
        "No especificado",
        "Empedrado", 
        "No aplica", 
        "Conjunto habitacional", 
            
        ])
    plt.xticks(rotation=45)
    plt.show()

    # Usuarios por presencia de alumbrado publico
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="MZ_FT_ALUMPUB_", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['MZ_FT_ALUMPUB_'].value_counts().index,
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Usuarios por presencia de alumbrado publico', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Alumbrado publico"
        )
    g.set_xticklabels([
        "Si tiene",
        "No especificado",
        "No tiene",
        "No aplica", 
        "Conjunto habitacional", 
            
        ])
    plt.xticks(rotation=45)
    plt.show()

    # Usuarios por presencia de puestos semifijos en las vialidades
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="MZ_FT_PUESSEMI_", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['MZ_FT_PUESSEMI_'].value_counts().index,
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Usuarios con presencia de puestos semifijos en la vialidad', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Presencia de puestos semifijos"
        )
    g.set_xticklabels([
        "No tiene",
        "No especificado",
        "Si tiene",
        "No aplica", 
        "Conjunto habitacional", 
            
        ])
    plt.xticks(rotation=45)
    plt.show()

    # Usuarios con presencia de comercio o servicio ambulante en las vialidades
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="MZ_FT_PUESAMBU_", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['MZ_FT_PUESAMBU_'].value_counts().index,
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Usuarios con presencia de comercio o servicio ambulante en la vialidad', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Presencia de comercio o servicio ambulante"
        )
    g.set_xticklabels([
        "No tiene",
        "No especificado",
        "Si tiene",
        "No aplica", 
        "Conjunto habitacional", 
            
        ])
    plt.xticks(rotation=45)
    plt.show()

    # Distribucion de usuarios por recubrimiento de piso
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.displot(x="MZ_P_V_PISODT", 
                    data=df_users, 
                    kind='hist', 
                    height=8,
                    aspect=2,
                    bins=100,
                    kde=True
                )

    g.fig.suptitle('Distribución de usuarios con recubrimiento en piso (% por manzana)', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Porcentaje de viviendas con recubrimiento en piso"
        )
    plt.show()

    # Distribucion de usuarios con servicio de energia electrica
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.displot(x="MZ_P_V_C_ELEC", 
                    data=df_users, 
                    kind='hist', 
                    height=8,
                    aspect=2,
                    bins=100,
                    kde=True
                )

    g.fig.suptitle('Distribución de usuarios con servicio de energia electrica (% por manzana)', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Porcentaje de viviendas con servicio de energia electrica"
        )
    plt.show()

    # Distribucion de promedio de escolaridad por manzana
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.displot(x="MZ_GRAPROES", 
                    data=df_users, 
                    kind='hist', 
                    height=8,
                    aspect=2,
                    kde=True
                )

    g.fig.suptitle('Distribución de promedio de escolaridad por manzana', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Promedio de escolaridad por manzana"
        )
    plt.show()


    # Distribucion de usuarios con presencia de comercio o servicio ambulante en la manzana
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="MZ_PUESAMBU_", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['MZ_PUESAMBU_'].value_counts().index,
                    height=8,
                    aspect=2
                )

    g.fig.suptitle('Usuarios con presencia de comercio o servicio ambulante en la manzana', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Presencia de comercio o servicio ambulante"
        )
    g.set_xticklabels([
        "Ninguna vialidad",
        "Alguna vialidad",
        "No especificado",
        "Todas las vialidades", 
        "Conjunto habitacional", 
            
        ])
    plt.xticks(rotation=45)
    plt.show()


    # Giros categorizados
    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="divisionGiro", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['divisionGiro'].value_counts().index,
                    height=10,
                    aspect=2
                )

    g.fig.suptitle('Giros categorizados', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Categorias Giros"
        )
    g.set(ylim=(-10,2500))
    plt.xticks(rotation=90)
    plt.show()

    # Tipos de tarifas

    sns.set_style("whitegrid")
    sns.set_palette("tab10")

    g = sns.catplot(x="tipoTarifa", 
                    data=df_users, 
                    kind='count', 
                    order = df_users['tipoTarifa'].value_counts().index,
                    height=6,
                    aspect=2
                )

    g.fig.suptitle('Tipos tarifas', y=1.03)
    g.set(ylabel="Numero de registros",
        xlabel="Tipo Tarifa"
        )
    g.set(ylim=(-10,2000))
    plt.xticks(rotation=90)
    plt.show()
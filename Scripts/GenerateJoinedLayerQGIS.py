# tiempo de ejecución en qgis 510 segundos


from qgis.core import *
import qgis.utils
import processing

# Cargamos la capa de municipios
municipios = QgsVectorLayer(r"Users/eduardohdz/Documents/Escuela/QGIS/JOIN_FILES/SHAPE_FILES/MUNICIPIOS/MUNICIPIOS.shp", "MUN_LAYER")
# Filtramos la capa de municipios
# municipios_filter = processing.run("qgis:executesql", {
#     "INPUT_DATASOURCES": municipios,
#     "INPUT_QUERY" : "SELECT * FROM MUNICIPIOS WHERE CVE_MUN = '07027' or CVE_MUN = '07101'",
#     "OUTPUT": 'TEMPORARY_OUTPUT'}
#                                    )

print('H')
QgsProject.instance().addMapLayer(municipios)
# print('Name ',municipios_filter['OUTPUT'].sourceName())
# municipios_filter['OUTPUT'].setName('MUNICIPIOS_FILTERED')


usuarios = QgsVectorLayer(r"Users/eduardohdz/Documents/Escuela/QGIS/JOIN_FILES/SHAPE_FILES/USERS_TUXTLA_CHIAPA/USERS_TUXTLA_CHIAPA.shp","USUARIOS_LAYER")

users_contained = processing.run("qgis:joinattributesbylocation", {
    "INPUT": usuarios,
    "JOIN": municipios,
    "PREDICATE": [0],
    "METHOD": 1,
    "DISCARD_NONMATCHING" : True,
    "PREFIX" : 'MUN_',
    "OUTPUT": 'TEMPORARY_OUTPUT'
})
users_contained['OUTPUT'].setName('USERS_FILTERED')
# QgsProject.instance().addMapLayer(users_contained['OUTPUT'])
#Ahora sleccionaremos unicamente a los usuarios que esten dentro de las geometrias de los municipios

manzanas = QgsVectorLayer(r"Users/eduardohdz/Documents/Escuela/QGIS/JOIN_FILES/SHAPE_FILES/MANZANAS/MANZANAS.shp", "MANZANAS_LAYER")

users_associated_MZ = processing.run("qgis:joinbynearest", {
    "INPUT": users_contained['OUTPUT'],
    "INPUT_2": manzanas,
    'OUTPUT': 'TEMPORARY_OUTPUT',
    "PREFIX": 'MZ_',
    "NEIGHBORS": 1
})
users_associated_MZ['OUTPUT'].setName('USERS_FILTERED_MZ')


frentes_manzanas = QgsVectorLayer(r"Users/eduardohdz/Documents/Escuela/QGIS/JOIN_FILES/SHAPE_FILES/MANZANAS_FRENTES/MANZANAS_FRENTES.shp", "MANZANAS_FRENTES_LAYER")

users_associated_MZ_FT = processing.run("qgis:joinbynearest", {
    "INPUT": users_associated_MZ['OUTPUT'],
    "INPUT_2": frentes_manzanas,
    'OUTPUT': 'TEMPORARY_OUTPUT',
    "PREFIX": 'MZ_FT_',
    "NEIGHBORS": 1
})
users_associated_MZ_FT['OUTPUT'].setName('USERS_FILTERED_MZ_FT')

# Añadimos las capas
# QgsProject.instance().addMapLayer(municipios_filter['OUTPUT'])
QgsProject.instance().addMapLayer(manzanas)
QgsProject.instance().addMapLayer(frentes_manzanas)
# QgsProject.instance().addMapLayer(caminos)
QgsProject.instance().addMapLayer(users_associated_MZ_FT['OUTPUT'])


#Save the new layer as a Geopackage or a Shapefile, or CSV

# SaveVectorOptions contains many settings for the writer process

save_options = QgsVectorFileWriter.SaveVectorOptions()
# Uncomment to save it as a ESRI Shapefile
# save_options.driverName = "ESRI Shapefile"

transform_context = QgsProject.instance().transformContext()
path_output_file = "/Users/eduardohdz/Documents/Escuela/PT2/ANALISIS/ANLISIS_LOCAL/DATASETS/GPKG/USERS_JOINED_TEST.gpkg"

error = QgsVectorFileWriter.writeAsVectorFormatV2(users_associated_MZ_FT['OUTPUT'],
                                                  path_output_file,
                                                  QgsCoordinateTransformContext(),
                                                  save_options)
if error[0] == QgsVectorFileWriter.NoError:
    print("success GPKG!")
else:
  print(error)

save_options.driverName = "ESRI Shapefile"
path_output_file = "/Users/eduardohdz/Documents/Escuela/PT2/ANALISIS/ANLISIS_LOCAL/DATASETS/SHP/USERS_JOINED_TEST.shp"
error = QgsVectorFileWriter.writeAsVectorFormatV2(users_associated_MZ_FT['OUTPUT'],
                                                  path_output_file,
                                                  QgsCoordinateTransformContext(),
                                                  save_options)
if error[0] == QgsVectorFileWriter.NoError:
    print("success SHP!")
else:
  print(error)

print('Finished! :D')

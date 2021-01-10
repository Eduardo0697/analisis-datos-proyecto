# Librerias necesarias
library(dtwclust)
library(parallel)
library(tidyverse)

start_time <- Sys.time()
# Lectura del conjunto de series de tiempo

# data_consumos_multi <- read_csv("./DATASETS/users_multiindex_2.csv")
data_consumos_multi <- read_csv("../../ANLISIS_LOCAL/DATASETS/Consumos/users_multiindex_TODOSMUNICIPIOS.csv")

# Realizamos pivot de la tablar para tener cada registro como una columna, 
# dejando como nulos los datos inexistentes

data_consumos_wider <- data_consumos_multi %>%
  select(rpu, mesConsumo, kwh) %>%
  pivot_wider(names_from = mesConsumo , values_from = kwh)

# Obtenemos los nombres de las columnas y los ordenamos
columns_ordered <- sort(colnames(data_consumos_wider))

# Quitamos rpu del final y lo agregamos al principio
col_order <- c("rpu", columns_ordered[-length(columns_ordered)])

# Reordenamos las columnas
data_consumos_wider <- data_consumos_wider %>%
  select(all_of(col_order)) %>%
  column_to_rownames('rpu')

# Al no tener casi registros del 2020, los excluimos del analisis
data_consumos_2 <- data_consumos_wider %>%
  select(-("2020-01-01":"2020-02-01"))


# Teniendo los datos de cada usuario como un registro, procedemos al clustering de series de tiempo.


# El algortimo nos solicita las series de tiempo de la siguiente forma:

# Las series de tiempo tienen que ser una matriz, un dataframe o una lista.
# Solo las lsitas pueden tener diferentes tamaños o dimensiones multiples.

# Convertimos las matrices o conjunto de datos a una lista univariable de series de tiempo m
list_series <- tslist(data_consumos_2)

# A esta lista quitamos los valores nulos
list_series_nonna <- list_series %>%
  lapply(function(x) x[!is.na(x)])

# Normalizamos las series de tiempo
list_series_z <- dtwclust::zscore(list_series_nonna)


# -----------------CLUSTERING --------------------------


consumos_clusters <- tsclust(list_series_z,
                             #preproc = "zscore",
                             type="partitional",
                             k=14L,
                             #seed= 889L,
                             distance="dtw_basic", # dtw_basic or sbd
                             window.size = 5L,
                             centroid= "pam",
                             trace = TRUE,
                             control = partitional_control(
                               pam.precompute = FALSE,
                               iter.max = 100L
                             )
                             #args = tsclust_args(cent = list(trace = TRUE))
)

gpc <- plot(consumos_clusters, type = "centroids" )
gpc + labs(title = "14 Clusters" )


# Add cluster results to data frame
cluster <- consumos_clusters@cluster               ## Extraemos el cluster para cada elemento
index_rpu <- names(list_series_z)                  ## Extraemos los identificadores de cada serie de tiempo
add_cluster_to_csv<-cbind(index_rpu,cluster)       ## Combinamos el identificador y su cluster correspondiente

# Obtenemos los centroides

centroides <- consumos_clusters@centroids          ## Extraemos los centroides

# Reinterpolamos los centroides para tener un vector de la misma longitud
centroides_reinterpolated <- reinterpolate(centroides, new.length = max(lengths(centroides)))

# Guardamos los centroides en otro dataset

df_centroides <- as.data.frame(do.call(rbind, lapply(centroides_reinterpolated, as.vector)))
df_centroides <- cbind(centroide=rownames(df_centroides), df_centroides)

# Añadimos los clusters y centroides a un csv
#write.csv(df_centroides,"./centroides.csv", row.names = FALSE)
#write.csv(add_cluster_to_csv,"./users_clustered.csv", row.names = FALSE)

# PRUEBA
write.csv(df_centroides,"./../../ANLISIS_LOCAL/DATASETS/centroidesTODOS.csv", row.names = FALSE)
write.csv(add_cluster_to_csv,"./../../ANLISIS_LOCAL/DATASETS/users_clusteredTODOS.csv", row.names = FALSE)

# Usuarios y consumos normalizados

# data_consumos_normalized <- dtwclust::zscore(data_consumos_2)

end_time <- Sys.time()
end_time - start_time
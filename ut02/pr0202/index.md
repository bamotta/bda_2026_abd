# PR0202: Uso del cliente HDFS

## Preparación del entorno
```
hdfs dfs -mkdir /andres
```

Salida:

![Salida](image1.png)

```
 hdfs dfs -mkdir /andres/proyecto_datos
```

Salida:

![Salida](image2.png)

## Carga de Datasets
Primero subimos el archivo al contenedor de docker:
```
docker cp "C:\Users\abdam\Documents\fp\IA y Big Data\Big Data Aplicado\archive\vehicle_price_prediction.csv" namenode:/home/
```
![Salida](image3.png)


Una vez lo tenemos en docker, lo subimos a la carpeta que hemos creado en el espacio de hdfs:
```
hdfs dfs -put /home/vehicle_price_prediction.csv /andres/proyecto_datos
```
![Salida](image4.png)

## Exploración de Datos
Visualizamos el contenido del archivo:
```
hdfs dfs -cat /andres/proyecto_datos/vehicle_price_prediction.csv
```
![Salida](image5.png)

Obtenemos el número de líneas del dataset:
```
hdfs dfs -cat /andres/proyecto_datos/vehicle_price_prediction.csv | wc -l
```
![Salida](image6.png)

## Organización del proyecto
Creamos un subdirectorio *procesados* y otro *backup*:
```
hdfs dfs -mkdir /andres/proyecto_datos/procesados
hdfs dfs -mkdir /andres/proyecto_datos/backup
```
![Salida](image7.png)

Copiamos el archivo original en *backup*:
```
hdfs dfs -cp /andres/proyecto_datos/vehicle_price_prediction.csv /andres/proyecto_datos/backup
```
![Salida](image8.png)

Movemos el archivo original a *procesados*:
```
hdfs dfs -mv /andres/proyecto_datos/vehicle_price_prediction.csv /andres/proyecto_datos/procesados
```
![Salida](image9.png)


## Colaboración en el equipo
Creamos el directorio compartido:
```
hdfs dfs -mkdir /compartido
hdfs dfs -chmod 777 /compartido
```
![Salida](image10.png)

Copiamos alli el archivo:
```
hdfs dfs -cp /andres/proyecto_datos/procesados/vehicle_price_prediction.csv /compartido
```
![Salida](image11.png)

## Recuperación de datos
Descargamos en local el archivo de *procesados*:
```
hdfs dfs -get /andres/proyecto_datos/procesados/vehicle_price_prediction.csv /
```
![Salida](image12.png)

## Control de accesos
Podemos verificar los permisos de los archivos al hacer ```hdfs dfs -ls```.

Cambiamos los permisos del archivo en *backup*:
```
hdfs dfs -chmod 700 /andres/proyecto_datos/backup/vehicle_price_prediction.csv
```
![Salida](image13.png)

## Mantenimiento
Eliminamos un archivo temporal:
```
hdfs dfs -rm /temporal.txt
```
![Salida](image14.png)


[Volver](../index.md)
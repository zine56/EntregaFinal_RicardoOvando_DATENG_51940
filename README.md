# DAG de precios de Stock
a partir del ejemplo dado por el profesor se crea un dag para cargar los datos de stock en la bd correspondiente, 
el contenido del .env se envia mediante la entrega en coderhouse

# Distribución de los archivos
Los archivos a tener en cuenta son:
* `docker_images/`: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_stock.py`: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de stock.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `postgres_data/`: Carpeta con los datos de Postgres.
* `scripts/`: Carpeta con los scripts de Spark.
    * `postgresql-42.5.2.jar`: Driver de Postgres para Spark.
    * `common.py`: Script de Spark con funciones comunes.
    * `ETL_Stock.py`: Script de Spark que ejecuta el ETL.

# Pasos para ejecutar el ejemplo
1. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`.
```bash
mkdir -p dags,logs,plugins,postgres_data,scripts
```
2. Crear un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
EMAIL_TO=...
AIRFLOW__SMTP__SMTP_HOST=...
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=...
AIRFLOW__SMTP__SMTP_PASSWORD=...
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=...
UMBRAL_MIN=1
UMBRAL_MAX=50
DRIVER_CLASS_PATH=/tmp/drivers/postgresql-42.5.2.jar
SPARK_SCRIPTS_DIR=/opt/airflow/scripts
REDSHIFT_CONN_HOST=...
REDSHIFT_CONN_PORT=5439
REDSHIFT_CONN_DB=...
REDSHIFT_CONN_USER=...
REDSHIFT_CONN_SCHEMA=...
REDSHIFT_CONN_PASSWORD=...
REDSHIFT_CONN_URL="jdbc:postgresql://${REDSHIFT_CONN_HOST}:${REDSHIFT_CONN_PORT}/${REDSHIFT_CONN_DB}?user=${REDSHIFT_CONN_USER}&password=${REDSHIFT_CONN_PASSWORD}"
DRIVER_PATH=/tmp/drivers/postgresql-42.5.2.jar
API_TOKEN=...
```

UMBRAL_MIN y UMBRAL_MAX son el precio minimo y máximo , se busca el maximo de los datos obtenenidos de la api, si se salen de esos umbrales
envia un correo para dar aviso del minimo y el maximo.

3. Descargar las imagenes de Airflow y Spark.
```bash
docker-compose pull lucastrubiano/airflow:airflow_2_6_2
docker-compose pull lucastrubiano/spark:spark_3_4_1
```
4. Las imagenes fueron generadas a partir de los Dockerfiles ubicados en `docker_images/`. Si se desea generar las imagenes nuevamente, ejecutar los comandos que están en los Dockerfiles.

5. Ejecutar el siguiente comando para levantar los servicios de Airflow y Spark.
```bash
docker-compose up --build
```
6. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.
7. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
8. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`
9. Ejecutar el DAG `etl_stock`.
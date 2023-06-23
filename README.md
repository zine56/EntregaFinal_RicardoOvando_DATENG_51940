# Script de inserción de datos en Amazon Redshift desde una API

Este script se encarga de obtener datos de una API y almacenarlos en una tabla en Amazon Redshift. Los datos obtenidos son información sobre acciones bursátiles y se insertan en la tabla "stock".

## Variables de entorno

El script utiliza varias variables de entorno para la conexión a la base de datos y la autenticación con la API. Asegúrate de configurar estas variables antes de ejecutar el script:

- `DB_NAME`: El nombre de la base de datos en Amazon Redshift.
- `DB_USER`: El nombre de usuario para la conexión a la base de datos.
- `DB_PASSWORD`: La contraseña del usuario para la conexión a la base de datos.
- `DB_HOST`: El host o dirección IP del servidor de Amazon Redshift.
- `DB_PORT`: El puerto utilizado para la conexión a Amazon Redshift.
- `API_TOKEN`: El token de autenticación para acceder a la API de datos bursátiles.

## Funcionamiento del script

El script sigue los siguientes pasos:


El script realizará los siguientes pasos:

1. Conectarse a Redshift utilizando las variables de entorno proporcionadas.
2. Crear la tabla "stock" si no existe.
3. Genera un identificador único de lote utilizando un UUID y un hash
4. Obtener datos de la API de datos de acciones para los símbolos especificados.
5. Realiza múltiples solicitudes a la API de datos bursátiles para obtener la información de las acciones en grupos de símbolos.
6. Concatena los datos obtenidos en un DataFrame.
7. Calcula la clave de hora actual utilizando el formato "YYYY_MM_DD_HH_00".
8. Limpiar y transformar los datos obtenidos.
9. Eliminar los registros existentes con la misma clave (ticker, hour_key).
10. Insertar los datos transformados en la tabla "stock" en Redshift.
11. Cierra la conexión con la base de datos.

Durante la ejecución del script, se mostrarán mensajes de progreso y posibles errores.

## Claves (Keys) utilizadas

- **Sort Key**: La columna "hour_key" se define como la sort key para la tabla "stock". Esto permite que los datos se almacenen y ordenen en Amazon Redshift según esta columna.
- **Distribution Key**: La columna "ticker" se define como la distribution key para la tabla "stock". Esto distribuye los datos en Amazon Redshift según los valores de esta columna, lo que puede ayudar a mejorar el rendimiento de las consultas.


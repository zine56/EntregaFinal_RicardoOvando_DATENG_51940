import os
import requests
import psycopg2
import hashlib
import uuid
import traceback
import datetime

import pandas as pd

from psycopg2 import extensions
from psycopg2 import sql
from requests.exceptions import RequestException

print(os.environ)

try:
    extensions.ISOLATION_LEVEL_AUTOCOMMIT = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
    conn = psycopg2.connect(
        dbname=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        host=os.environ['DB_HOST'],
        port=os.environ['DB_PORT']
    )
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS "stock" (
            "ticker" VARCHAR(10) NOT NULL,
            "hour_key" VARCHAR(17) NOT NULL,
            "name" VARCHAR(100) NULL DEFAULT NULL,
            "exchange_short" VARCHAR(20) NULL DEFAULT NULL,
            "exchange_long" VARCHAR(100) NULL DEFAULT NULL,
            "mic_code" VARCHAR(100) NULL DEFAULT NULL,
            "currency" VARCHAR(10) NULL DEFAULT NULL,
            "price" FLOAT8 NULL DEFAULT NULL,
            "day_high" FLOAT8 NULL DEFAULT NULL,
            "day_low" FLOAT8 NULL DEFAULT NULL,
            "day_avg" FLOAT8 NULL DEFAULT NULL,
            "day_open" FLOAT8 NULL DEFAULT NULL,
            "52_week_high" FLOAT8 NULL DEFAULT NULL,
            "52_week_low" FLOAT8 NULL DEFAULT NULL,
            "market_cap" BIGINT NULL DEFAULT NULL,
            "previous_close_price" FLOAT8 NULL DEFAULT NULL,
            "previous_close_price_time" TIMESTAMP NULL DEFAULT NULL,
            "day_change" FLOAT8 NULL DEFAULT NULL,
            "volume" BIGINT NULL DEFAULT NULL,
            "is_extended_hours_price" BOOL NULL DEFAULT NULL,
            "last_trade_time" TIMESTAMP NULL DEFAULT NULL,
            "insertion_date" TIMESTAMP NULL DEFAULT GETDATE(),
            "batch_id" BIGINT NULL DEFAULT NULL,
            "price_percentage" FLOAT8 NULL DEFAULT NULL
        )
        DISTKEY("ticker")
        SORTKEY("hour_key");
    ''')

    uuid_string = str(uuid.uuid4()).replace('-', '')
    encoded_string = uuid_string.encode('utf-8')
    batch_id = int(hashlib.sha1(encoded_string).hexdigest(), 16) % 2147483647

    stock_symbols = ["GME", "AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "META", "NFLX", "NVDA", "JPM", "AMD", "AMC"]

    groups = [stock_symbols[i:i+3] for i in range(0, len(stock_symbols), 3)]

    hour_key = datetime.datetime.now().strftime('%Y_%m_%d_%H_00')

    concatenated_df = pd.DataFrame()

    for group in groups:
        group_string = ",".join(group)
        print(group_string)

        response = requests.get(f"https://api.stockdata.org/v1/data/quote?symbols={group_string}&api_token={os.environ['API_TOKEN']}")
        data = response.json()
        print("data", data)
        df = pd.DataFrame(data['data'])
        df['hour_key'] = hour_key
        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)
        print(concatenated_df)

    # Eliminar duplicados basados en la clave ('ticker', 'hour_key')
    concatenated_df.drop_duplicates(subset=['ticker', 'hour_key'], keep='first', inplace=True)

    # Verificar valores nulos o vacíos en cada fila y eliminar las filas que los contengan
    rows_with_null_values = concatenated_df.isnull().any(axis=1)
    if rows_with_null_values.any():
        print("Filas con valores nulos o vacíos:")
        print(concatenated_df[rows_with_null_values])
        concatenated_df = concatenated_df[~rows_with_null_values]

    # Realizar transformaciones de datos 
    concatenated_df['price_percentage'] = concatenated_df['price'] * 100
    concatenated_df['day_avg'] = (concatenated_df['day_high'] + concatenated_df['day_low']) / 2

    print(concatenated_df.describe())

    # Eliminar registros existentes con la misma clave de la tabla
    for _, row in concatenated_df.iterrows():
        cur.execute(
            """
            DELETE FROM "stock"
            WHERE "ticker" = %s
            AND "hour_key" = %s
            """,
            (row['ticker'], row['hour_key'])
        )

    # Insertar los datos transformados en la tabla
    for _, row in concatenated_df.iterrows():
        cur.execute(
            """
            INSERT INTO "stock" (
                "ticker",
                "hour_key",
                "name",
                "exchange_short",
                "exchange_long",
                "mic_code",
                "currency",
                "price",
                "day_high",
                "day_low",
                "day_avg",
                "day_open",
                "52_week_high",
                "52_week_low",
                "market_cap",
                "previous_close_price",
                "previous_close_price_time",
                "day_change",
                "volume",
                "is_extended_hours_price",
                "last_trade_time",
                "insertion_date",
                "batch_id",
                "price_percentage"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['ticker'],
                row['hour_key'],
                row['name'],
                row['exchange_short'],
                row['exchange_long'],
                row['mic_code'],
                row['currency'],
                row['price'],
                row['day_high'],
                row['day_low'],
                row['day_avg'],
                row['day_open'],
                row['52_week_high'],
                row['52_week_low'],
                row['market_cap'],
                row['previous_close_price'],
                row['previous_close_price_time'],
                row['day_change'],
                row['volume'],
                row['is_extended_hours_price'],
                row['last_trade_time'],
                datetime.datetime.now(),
                batch_id,
                row['price_percentage']
            )
        )
    conn.commit()

except RequestException as e:
    error_message = str(e)
    print("Error en la solicitud del API:", error_message)
except psycopg2.Error as e:
    error_message = str(e)
    print("Error durante la inserción:", error_message)
except Exception as e:
    error_message = str(e)
    traceback.print_exc()

finally:
    cur.close()
    conn.close()

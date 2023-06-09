import os
import requests
import psycopg2
import hashlib
import uuid
import traceback

import pandas as pd
import json

from psycopg2 import extensions
from psycopg2 import sql
print(os.environ)

try:
    # por algun motivo si no le pongo esta linea de autocommit no inserta, incluso teniendo el commit alla final
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
            "id" INT IDENTITY(1,1) PRIMARY KEY,
            "ticker" VARCHAR(10) NULL DEFAULT NULL,
            "name" VARCHAR(100) NULL DEFAULT NULL,
            "exchange_short" VARCHAR(20) NULL DEFAULT NULL,
            "exchange_long" VARCHAR(100) NULL DEFAULT NULL,
            "mic_code" VARCHAR(100) NULL DEFAULT NULL,
            "currency" VARCHAR(10) NULL DEFAULT NULL,
            "price" FLOAT8 NULL DEFAULT NULL,
            "day_high" FLOAT8 NULL DEFAULT NULL,
            "day_low" FLOAT8 NULL DEFAULT NULL,
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
            "batch_id" BIGINT NULL DEFAULT NULL
        );
    ''')

    uuid_string = str(uuid.uuid4()).replace('-', '')
    encoded_string = uuid_string.encode('utf-8')
    batch_id = int(hashlib.sha1(encoded_string).hexdigest(), 16) % 2147483647

    stock_symbols = ["GME", "AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "META", "NFLX", "NVDA", "JPM", "AMD", "AMC"]

    # la api tiene un limite de 3 simbolos por request, por eso tengo que hacer multiples requests
    groups = [stock_symbols[i:i+3] for i in range(0, len(stock_symbols), 3)]
    group_strings = [",".join(group) for group in groups]

    df_list = []

    for group_string in group_strings:
        print(group_string)

        response = requests.get(f"https://api.stockdata.org/v1/data/quote?symbols={group_string}&api_token={os.environ['API_TOKEN']}")
        data = response.json()
        print("data", data)
        df = pd.DataFrame(data['data'])
        df_list.append(df)
        print(df)

    concatenated_df = pd.concat(df_list)
    concatenated_df = concatenated_df.reset_index(drop=True)
    print(concatenated_df)


    # Mostrar estadísticas del DataFrame
    print(concatenated_df.describe())


    # Insert each row into the table
    for _, row in concatenated_df.iterrows():
        cur.execute(
            """
            INSERT INTO "stock" (
                "ticker",
                "name",
                "exchange_short",
                "exchange_long",
                "mic_code",
                "currency",
                "price",
                "day_high",
                "day_low",
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
                "batch_id"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row['ticker'],
                row['name'],
                row['exchange_short'],
                row['exchange_long'],
                row['mic_code'],
                row['currency'],
                row['price'],
                row['day_high'],
                row['day_low'],
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
                batch_id
            )
        )

    conn.commit()

except requests.RequestException as e:
    error_message = str(e)
    print("Error occurred during API request:", error_message)
except psycopg2.Error as e:
    error_message = str(e)
    print("Error occurred during insert:", error_message)
except Exception as e:
    # An error occurred during the execution
    error_message = str(e)
    traceback.print_exc()

finally:
    # Close the cursor and connection
    cur.close()
    conn.close()

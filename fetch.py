import os
import requests
import psycopg2
import hashlib
import uuid
import traceback

from psycopg2 import sql
from psycopg2 import extensions

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

    uuid_string = str(uuid.uuid4()).replace('-', '')
    encoded_string = uuid_string.encode('utf-8')
    batch_id = int(hashlib.sha1(encoded_string).hexdigest(), 16) % 2147483647

    stock_symbols = ["GME", "AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "META", "NFLX", "NVDA", "JPM", "AMD", "AMC"]

    groups = [stock_symbols[i:i+3] for i in range(0, len(stock_symbols), 3)]
    group_strings = [",".join(group) for group in groups]

    for group_string in group_strings:
        print(group_string)

        response = requests.get(f"https://api.stockdata.org/v1/data/quote?symbols={group_string}&api_token={os.environ['API_TOKEN']}")
        data = response.json()
        print(data)


        if data['meta'] and data['meta']['returned'] > 0:
            for item in data['data']:
                insert_statement = sql.SQL(
                    '''INSERT INTO "ricardoovando04_coderhouse"."stock" ("ticker", "name","exchange_short", "exchange_long", "mic_code", "currency", "price","day_high", "day_low", "day_open", "52_week_high", "52_week_low", "market_cap", "previous_close_price", "previous_close_price_time","day_change", "volume", "is_extended_hours_price", "last_trade_time", "batch_id") VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});''').format(
                    sql.Literal(item['ticker']),
                    sql.Literal(item['name']),
                    sql.Literal(item['exchange_short']),
                    sql.Literal(item['exchange_long']),
                    sql.Literal(item['mic_code']),
                    sql.Literal(item['currency']),
                    sql.Literal(item['price']),
                    sql.Literal(item['day_high']),
                    sql.Literal(item['day_low']),
                    sql.Literal(item['day_open']),
                    sql.Literal(item['52_week_high']),
                    sql.Literal(item['52_week_low']),
                    sql.Literal(item['market_cap']),
                    sql.Literal(item['previous_close_price']),
                    sql.Literal(item['previous_close_price_time']),
                    sql.Literal(item['day_change']),
                    sql.Literal(item['volume']),
                    sql.Literal(item['is_extended_hours_price']),
                    sql.Literal(item['last_trade_time']),
                    sql.Literal(batch_id)
                )
                print(insert_statement)
                #exit()
                cur.execute(insert_statement)
                conn.commit()
                print("Insert successful for item:", item['ticker'])
        else:
            print("Error: No data returned from the API")

    cur.close()
    conn.close()
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



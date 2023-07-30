# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import uuid
import hashlib
from os import environ as env
import json

from pyspark.sql.functions import lit,col,to_timestamp,when,current_timestamp, max as max_, min as min_

from commons import ETL_Spark

class ETL_Stock(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")
    
    def send_email(self, subject, ticker, value):
        msg = MIMEMultipart()
        msg['From'] = env['AIRFLOW__SMTP__SMTP_MAIL_FROM']
        msg['To'] = env['EMAIL_TO']
        msg['Subject'] = subject
        msg.attach(MIMEText(f"Ticker {ticker} tiene un valor de {value}"))
        server = smtplib.SMTP(env['AIRFLOW__SMTP__SMTP_HOST'], env['AIRFLOW__SMTP__SMTP_PORT'])
        server.starttls()
        server.login(env['AIRFLOW__SMTP__SMTP_USER'], env['AIRFLOW__SMTP__SMTP_PASSWORD'])
        server.sendmail(env['AIRFLOW__SMTP__SMTP_USER'], env['EMAIL_TO'], msg.as_string())
        server.quit()

    def run(self):
        process_date = datetime.now().strftime("%Y-%m-%d")
        #process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")
        uuid_string = str(uuid.uuid4()).replace('-', '')
        encoded_string = uuid_string.encode('utf-8')
        batch_id = int(hashlib.sha1(encoded_string).hexdigest(), 16) % 2147483647

        stock_symbols = ["GME", "AMZN", "AAPL", "GOOGL", "MSFT", "TSLA", "META", "NFLX", "NVDA", "JPM", "AMD", "AMC"]

        groups = [stock_symbols[i:i+3] for i in range(0, len(stock_symbols), 3)]

        
        combined_df = None

        for group in groups:
            group_string = ",".join(group)
            print(group_string)

            response = requests.get(f"https://api.stockdata.org/v1/data/quote?symbols={group_string}&api_token={env['API_TOKEN']}")
            if response.status_code == 200:
                data = response.json()["data"]
                print(data)
                json_data = json.dumps(data)
                df = self.spark.read.json(self.spark.sparkContext.parallelize([json_data]), multiLine=True)
                #df = self.spark.read.json(self.spark.sparkContext.parallelize(data), multiLine=True)
                #df = self.spark.read.json(self.spark.sparkContext.parallelize(data), multiLine=True)
                print(df.schema)

                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)
            else:
                print("Error al extraer datos de la API")
                data = []
                raise Exception("Error al extraer datos de la API")
        combined_df = combined_df.dropDuplicates()

        combined_df.printSchema()
        combined_df.show()            
        print(combined_df)
        return combined_df

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        uuid_string = str(uuid.uuid4()).replace('-', '')
        encoded_string = uuid_string.encode('utf-8')
        batch_id = int(hashlib.sha1(encoded_string).hexdigest(), 16) % 2147483647
        hour_key = datetime.now().strftime('%Y_%m_%d_%H_00')

        umbral_max = env['UMBRAL_MAX']
        umbral_min = env['UMBRAL_MIN']

        umbral_max = float(env['UMBRAL_MAX'])
        umbral_min = float(env['UMBRAL_MIN'])

        high_price_row = df_original.filter(df_original.price > umbral_max).orderBy(df_original.price.desc()).first()
        low_price_row = df_original.filter(df_original.price < umbral_min).orderBy(df_original.price.asc()).first()

        if high_price_row is not None:
            self.send_email("Alert: Precio Alto SELL SELL SELL", high_price_row['ticker'], high_price_row['price'])

        if low_price_row is not None:
            self.send_email("Alert: Precio Bajo BUY BUY BUY ", low_price_row['ticker'], low_price_row['price'])



        df_original = df_original.withColumn("price_percentage", col('price') * 100)
        df_original = df_original.withColumn("day_avg", (col('day_high') + col('day_low')) / 2)
        df_original = df_original.withColumn("hour_key", lit(hour_key))
        df_original = df_original.withColumn("batch_id", lit(batch_id))
        df_original = df_original.withColumn("last_trade_time", to_timestamp(col("last_trade_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        df_original = df_original.withColumn("market_cap", when(col("market_cap").cast("bigint").isNotNull(), col("market_cap").cast("bigint")).otherwise(lit(None)))
        df_original = df_original.withColumn("previous_close_price_time", to_timestamp(col("previous_close_price_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        df_original = df_original.withColumn("batch_id", col("batch_id").cast("bigint"))

        df_original.printSchema()
        df_original.show()

        return df_original

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        df_final = df_final.withColumn("insertion_date", current_timestamp())


        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_CONN_URL']) \
            .option("dbtable", f"{env['REDSHIFT_CONN_SCHEMA']}.stock") \
            .option("user", env['REDSHIFT_CONN_LOGIN']) \
            .option("password", env['REDSHIFT_CONN_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Stock()
    etl.run()

# Import the required libraries
import pandas as pd
import numpy as np
import psycopg2
import pytz
import boto3
import pyfiglet
import warnings
from src import config as cfg
from src import modules_monitoring as mm
from datetime import date, datetime, timedelta

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)


#START PROCESS ----------------------------------------------------------------------------------------

#get the database credentials
aws_access_key, aws_secret_key, db_conn_raw, db_conn_clean, report_recipients, report_custom_date = cfg.get_secret()

#set var current date
naive_datetime = datetime.now(pytz.utc)
gmt_plus_7 = pytz.timezone('Asia/Jakarta')

current_date_7 = naive_datetime.astimezone(gmt_plus_7) - timedelta(days=1)
current_date = current_date_7.date()

#connect to DB
db_conn = psycopg2.connect(**db_conn_clean)
db_cursor = db_conn.cursor()

#connect to S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='ap-southeast-3'
)


art = pyfiglet.figlet_format('Monitoring')
print(art)

try:

    monitoring_zmb_us = mm.trace_monitoring_zmb_us(db_conn, current_date)
    print('[ZMB US] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_zmb_us)

    monitoring_mb51_us = mm.trace_monitoring_mb51_us(db_conn, current_date)
    print('[MB51 US] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_mb51_us)

    monitoring_mb51_ds = mm.trace_monitoring_mb51_ds(db_conn, current_date)
    print('[MB51 DS] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_mb51_ds)

    monitoring_sodogi = mm.trace_monitoring_sodogi(db_conn, current_date)
    print('[SODOGI] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_sodogi)

    monitoring_openlink = mm.trace_monitoring_openlink(db_conn, current_date)
    print('[OPENLINK] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_openlink)

    monitoring_mb51_esm = mm.trace_monitoring_mb51_esm(db_conn, current_date)
    print('[MB51 ESM] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_mb51_esm)

    monitoring_sodogi_esm = mm.trace_monitoring_sodogi_esm(db_conn, current_date)
    print('[SODOGI ESM] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_sodogi_esm)

    monitoring_openlink_esm = mm.trace_monitoring_openlink_esm(db_conn, current_date)
    print('[OPENLINK ESM] Processing Data Monitoring into DB...')
    mm.process_monitoring_to_db(db_conn, monitoring_openlink_esm)

    mm.refresh_matview_monitoring(db_conn)

except Exception as e:
    print(f"Error processing monitoring: {e}")
    raise e


#close db
db_conn.close()
db_cursor.close()
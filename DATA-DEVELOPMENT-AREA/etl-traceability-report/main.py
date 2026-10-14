# Import the required libraries
import pandas as pd
import numpy as np
import os
import time
import psycopg2
import pytz
import boto3
import pyfiglet
import warnings
from src import config as cfg
from src import modules_additional as ma
from src import modules_report_etl as mre
from src import modules_report_traceability as mrt
from datetime import date, datetime, timedelta

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)


#START PROCESS ----------------------------------------------------------------------------------------

#get the database credentials
aws_access_key, aws_secret_key, db_conn_raw, db_conn_clean, report_recipients, report_custom_date = cfg.get_secret()
print(f"REPORT_RECIPIENTS={report_recipients}")
print(f"REPORT_CUSTOM_DATE={report_custom_date}")

#load environment variables
cfg.load_yaml(report_recipients)

#get recipients from environment variables
recipients = os.getenv('RECIPIENTS').split(',')

#set var current date
naive_datetime = datetime.now(pytz.utc)
gmt_plus_7 = pytz.timezone('Asia/Jakarta')

if report_custom_date == '':
    current_date_7 = naive_datetime.astimezone(gmt_plus_7) - timedelta(days=1)
    current_date = current_date_7.date()
else:
    current_date = date(int(report_custom_date[:4]), int(report_custom_date[5:7]), int(report_custom_date[-2:]))

#set destination report
s3_bucket = 'trace-dataset'
report_path = f"report/combined_log_{current_date}.xlsx"

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


try:
    while True:

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Checking at {current_time} UTC...")

        # status = mre.check_status(db_conn_raw)
        status = 'Done'
        if status == 'Done':
            print(f"Running the script for report {current_date}.\n")

            art_1 = pyfiglet.figlet_format('ETL Report')
            print(art_1)
            print('Compiling Import Log...')
            df_import_log = mre.process_import_log(s3, db_conn_raw, current_date)
            df_import_log = pd.merge(df_import_log, mre.info_filtered_data(db_conn_raw, current_date), how='left', on='task_name')
            df_import_log['not_processed_raw_data'] = df_import_log['not_processed_raw_data'].apply(lambda x: int(x) if pd.notna(x) else 0)
            df_import_log['sum_val_raw_filtered_col'] = df_import_log.apply(lambda x: x['sum_val_raw_filtered_col'] if pd.notna(x['sum_val_raw_filtered_col']) else x['sum_dest_num_column'], axis=1)

            print('Compiling Migration Log...')
            df_migration_log = mre.process_migration_log(db_conn_clean, current_date)

            print('Connecting Import Log and Migration Log...\n')
            if len(df_import_log)>0 and len(df_migration_log)>0:
                etl_log = pd.merge(df_import_log, df_migration_log, on='task_name', how='outer', suffixes=('_import', '_migration'))
                etl_log['start_process_timestamp'] = etl_log.apply(lambda x: x['start_import_timestamp'] if pd.notna(x['start_import_timestamp']) else x['start_migration_timestamp'], axis=1)

                for col in ['duration_s3_to_raw', 'duration_raw_to_clean']:
                    etl_log[col] = etl_log[col].fillna('0s').replace('0.0s', '0s')

                for col in ['columns_from_s3', 'records_from_s3', 'records_s3_inserted_to_raw',
                            'records_raw_inserted_to_clean', 'records_raw_that_update_clean',
                            'records_raw_skipped', 'records_raw_processed_to_clean', 'total_dest_column']:
                    etl_log[col] = etl_log[col].apply(lambda x: int(x) if pd.notna(x) else 0)

                etl_log['status_records_raw_to_clean'] = etl_log.apply(lambda x: 'NOT PASS' if x['records_s3_inserted_to_raw'] > x['not_processed_raw_data'] + x['records_raw_processed_to_clean'] else 'PASS', axis=1)

                for col in ['status_record_s3_to_raw', 'status_column_s3_to_raw', 'status_records_raw_to_clean']:
                    etl_log[col] = etl_log[col].fillna('-')
                
                etl_log['remark_etl'] = etl_log.apply(lambda x: mre.notes_etl(x), axis=1)

                filtered_cols = [
                    'task_name', 'start_process_timestamp', 'records_from_s3', 'records_s3_inserted_to_raw',
                    'columns_from_s3', 'sum_source_num_column', 'sum_dest_num_column', 'duration_s3_to_raw',
                    'status_record_s3_to_raw', 'status_column_s3_to_raw', 'records_raw_processed_to_clean',
                    'records_raw_inserted_to_clean', 'records_raw_that_update_clean', 'total_dest_column',
                    'duration_raw_to_clean', 'status_records_raw_to_clean', 'remark_etl'
                    ]
                etl_log = etl_log[filtered_cols].astype(str)

                list_processed_task = etl_log['task_name'].to_list()
                df_unprocessed_task = mre.create_df_unprocessed_file(db_conn_clean, list_processed_task, current_date)[filtered_cols]

                etl_log = pd.concat([etl_log, df_unprocessed_task], axis=0).sort_values(by='task_name', ascending=True)

                art_2 = pyfiglet.figlet_format('Traceability Report')
                print(art_2)

                #get recnums
                processed_recnum_trans = mrt.get_processed_recnum_trans(db_conn, current_date)
                processed_recnum_trans = processed_recnum_trans[processed_recnum_trans['recnum'].notna()]
                processed_recnum_trans['recnum'] = processed_recnum_trans['recnum'].astype(int)
                print(f'Found {len(processed_recnum_trans)} transaction inserted or updated into DB Clean.')

                #get transactional ID
                print('Obtaining the ID from each transactional data...')
                id_map = {}
                for task_name, group in processed_recnum_trans.groupby('task_name'):
                    table_name = mrt.get_target_table(task_name)
                    recnums = group['recnum'].tolist()

                    if table_name != None:
                        id_results = mrt.get_ids_for_recnums(db_conn, table_name, recnums)
                        for id_value, recnum in id_results:
                            id_map[(task_name, recnum)] = id_value
                    else:
                        pass

                trans_conn_event = processed_recnum_trans.copy()
                trans_conn_event['id'] = trans_conn_event\
                    .apply(lambda row: id_map.get((row['task_name'], row['recnum']), None), axis=1)

                #get event informations
                print('Obtaining event informations for each transactional ID...')
                event_map = {}
                list_unique_id = [x for x in trans_conn_event['id']]
                event_results = mrt.get_events_info(db_conn, list_unique_id)
                for id, id_inc, plant in event_results:
                    event_map[id] = [id_inc, plant]

                trans_conn_event['id_inc'], trans_conn_event['plant'] = zip(*trans_conn_event\
                    .apply(lambda row: event_map.get(row['id'], [None, None]), axis=1)
                    )

                #initial cleaning trans_conn_event
                trans_conn_event['id'] = trans_conn_event['id'].fillna('-')
                trans_conn_event['plant'] = trans_conn_event['plant'].fillna('-').replace('','-')
                trans_conn_event['id_inc'] = trans_conn_event['id_inc'].fillna(0).astype(int)

                data_clean_info = trans_conn_event\
                    .groupby('task_name')['recnum'].count()\
                    .reset_index()\
                    .rename(columns={'recnum':'total_data_clean'})

                trans_conn_event = pd.merge(trans_conn_event, data_clean_info, how='left', on='task_name')
                trans_conn_event = trans_conn_event[['task_name', 'total_data_clean', 'id', 'id_inc', 'plant']]

                #partition data
                data_processed = trans_conn_event[trans_conn_event['id_inc'] != 0]
                print(f'\nTotal Processed Data: {len(data_processed)}')
                data_unprocessed = trans_conn_event[(trans_conn_event['id'] != '-') & (trans_conn_event['id_inc'] == 0)]
                print(f'Total Unprocessed Data: {len(data_unprocessed)}')
                data_skipped = trans_conn_event[trans_conn_event['id'] == '-']
                print(f'Total Skipped Data: {len(data_skipped)}')

                data_processed_mb51_ds = trans_conn_event[(trans_conn_event['id_inc'] != 0) & (trans_conn_event['task_name']=='TRANS_MB51DS')]
                data_processed_mb51_us = trans_conn_event[(trans_conn_event['id_inc'] != 0) & (trans_conn_event['task_name']=='TRANS_MB51US')]
                data_processed_mb51_esm = trans_conn_event[(trans_conn_event['id_inc'] != 0) & (trans_conn_event['task_name']=='TRANS_MB51_ESM')]
                data_processed_zmb_us = trans_conn_event[(trans_conn_event['id_inc'] != 0) & (trans_conn_event['task_name']=='TRANS_ZMB_US')]

                data_unprocessed_mb51_ds = data_unprocessed[data_unprocessed['task_name']=='TRANS_MB51DS']
                data_unprocessed_mb51_us = data_unprocessed[data_unprocessed['task_name']=='TRANS_MB51US']
                data_unprocessed_mb51_esm = data_unprocessed[data_unprocessed['task_name']=='TRANS_MB51_ESM']
                data_unprocessed_openlink = data_unprocessed[data_unprocessed['task_name']=='TRANS_OPENLINK']
                data_unprocessed_openlink_esm = data_unprocessed[data_unprocessed['task_name']=='TRANS_OPENLINK_ESM']
                data_unprocessed_sodogi = data_unprocessed[data_unprocessed['task_name']=='TRANS_SODOGIDS']
                data_unprocessed_sodogi_esm = data_unprocessed[data_unprocessed['task_name']=='TRANS_SODOGI_ESM']
                data_unprocessed_zmb_us = data_unprocessed[data_unprocessed['task_name']=='TRANS_ZMB_US']

                #transform data processed and skipped, and trace data unprocessed
                data_processed_ = mrt.transform_data_processed(data_processed)
                data_skipped_ = mrt.transform_data_skipped(data_skipped)

                print('\nTracing unprocessed row Trans MB51DS...')
                agg_result_mb51_ds = mrt.trace_unprocessed_mb51_ds(db_conn, data_unprocessed_mb51_ds, data_processed_mb51_ds, current_date)
                print('Tracing unprocessed row Trans MB51US...')
                agg_result_mb51_us = mrt.trace_unprocessed_mb51_us(db_conn, data_unprocessed_mb51_us, data_processed_mb51_us, current_date)
                print('Tracing unprocessed row Trans Openlink...')
                agg_result_openlink = mrt.trace_unprocessed_openlink(db_conn, data_unprocessed_openlink, current_date)
                print('Tracing unprocessed row Trans SODOGI...')
                agg_result_sodogi = mrt.trace_unprocessed_sodogi(db_conn, data_unprocessed_sodogi, current_date)
                print('Tracing unprocessed row Trans ZMB US...')
                agg_result_zmb_us = mrt.trace_unprocessed_zmb_us(db_conn, data_unprocessed_zmb_us, current_date)
                print('Tracing unprocessed row Trans MB51 ESM...')
                agg_result_mb51_esm = mrt.trace_unprocessed_mb51_esm(db_conn, data_unprocessed_mb51_esm, data_processed_mb51_esm, current_date)
                print('Tracing unprocessed row Trans Openlink ESM...')
                agg_result_openlink_esm = mrt.trace_unprocessed_openlink_esm(db_conn, data_unprocessed_openlink_esm, current_date)
                print('Tracing unprocessed row Trans SODOGI ESM...\n')
                agg_result_sodogi_esm = mrt.trace_unprocessed_sodogi_esm(db_conn, data_unprocessed_sodogi_esm, current_date)

                #merge data processed, unprocessed, and skipped
                data_unprocessed_ = pd.concat([
                    agg_result_zmb_us, agg_result_sodogi, agg_result_openlink,
                    agg_result_mb51_us, agg_result_mb51_ds, agg_result_mb51_esm,
                    agg_result_openlink_esm, agg_result_sodogi_esm
                    ], axis=0)

                data_processed_['processed_total_transaction'] = data_processed_\
                    .apply(lambda x: f"{x['processed_trans_to_event']} Row; {x['processed_plant']} Plant", axis=1)

                merge_processed_unprocessed = pd.merge(data_processed_, data_unprocessed_, how='outer', on='task_name')
                merge_processed_unprocessed['total_disconnected_trans_id'] = merge_processed_unprocessed['total_disconnected_trans_id'].apply(lambda x: 0 if pd.isna(x) else int(x))
                merge_processed_unprocessed['list_disconnected_trans_id'] = merge_processed_unprocessed['list_disconnected_trans_id'].fillna('-')
                merge_processed_unprocessed['status'] = merge_processed_unprocessed['total_disconnected_trans_id'].apply(lambda x: 'PASS' if x==0 else 'NOT PASS')
                total_disconnected_trans_id = merge_processed_unprocessed['total_disconnected_trans_id'].sum()
                merge_processed_unprocessed = merge_processed_unprocessed[[
                    'task_name', 'total_data_clean', 'processed_total_transaction', 'list_processed_plant',
                    'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
                    'remark_unprocessed_trans_to_event', 'list_disconnected_trans_id', 'status'
                    ]]

                traceability_log = pd.concat([merge_processed_unprocessed, data_skipped_], axis=0)


                art_3 = pyfiglet.figlet_format('ETL-Traceability Report')
                print(art_3)

                print('Connecting ETL Report and Traceability Report...')
                etl_traceability_log = pd.merge(etl_log, traceability_log, how='left', on='task_name')

                for col in ['processed_total_transaction', 'unprocessed_total_transaction']:
                    etl_traceability_log[col] = etl_traceability_log[col].fillna('0 Row; 0 Plant')
                for col in ['list_processed_plant', 'list_unprocessed_plant_trans_to_event', 'list_disconnected_trans_id']:
                    etl_traceability_log[col] = etl_traceability_log[col].fillna('-').replace('','-')

                etl_traceability_log['total_data_clean'] = etl_traceability_log.apply(lambda x: 0 if x['task_name'] in ['TRANS_VBAP', 'TRANS_VBKD', 'TRANS_ZWBMM', 'TRANS_ZWBMM_CERTI'] else x['total_data_clean'], axis=1)
                etl_traceability_log['total_data_clean'] = etl_traceability_log.apply(lambda x: int(x['records_raw_inserted_to_clean']) + int(x['records_raw_that_update_clean']) if pd.isna(x['total_data_clean']) else int(x['total_data_clean']), axis=1)
                etl_traceability_log['total_data_clean'] = etl_traceability_log['total_data_clean'].astype(int)
                total_data_clean = etl_traceability_log['total_data_clean'].sum()

                mask_pass_condition = (etl_traceability_log['status'].isna()) & (etl_traceability_log['total_data_clean'] == 0) 
                mask_existing_status = etl_traceability_log['status'].notna()
                mask_not_pass_condition = (etl_traceability_log['status'].isna()) & (etl_traceability_log['total_data_clean'] > 0) & (etl_traceability_log['processed_total_transaction'] == '0 Row; 0 Plant')

                etl_traceability_log['status'] = np.select(
                    [mask_pass_condition, mask_existing_status, mask_not_pass_condition],
                    ['PASS', etl_traceability_log['status'], 'NOT PASS'],
                    default=etl_traceability_log['status']
                )

                etl_traceability_log['remark_unprocessed_trans_to_event'] = np.select(
                    [mask_pass_condition, mask_existing_status, mask_not_pass_condition],
                    ['This transaction data is not loaded into events_bc.', etl_traceability_log['remark_unprocessed_trans_to_event'], 'Needs to be checked.'],
                    default=etl_traceability_log['remark_unprocessed_trans_to_event']
                )

                etl_traceability_log['list_initial_plant'] = etl_traceability_log['list_processed_plant'] + '; ' + etl_traceability_log['list_unprocessed_plant_trans_to_event']
                etl_traceability_log['list_initial_plant'] = etl_traceability_log['list_initial_plant'].str.split('; ').apply(lambda plants: [p for p in set(plants) if '-' not in p])
                etl_traceability_log['total_initial_unique_plants'] = etl_traceability_log['list_initial_plant'].apply(len)
                etl_traceability_log['initial_total_transaction'] = etl_traceability_log['total_data_clean'].astype(str) + ' Row; ' + etl_traceability_log['total_initial_unique_plants'].astype(str) + ' Plant'

                etl_traceability_log['list_initial_plant'] = etl_traceability_log['list_initial_plant'].apply('; '.join)
                etl_traceability_log['list_initial_plant'] = etl_traceability_log['list_initial_plant'].fillna('-')

                etl_traceability_log.loc[
                    etl_traceability_log['task_name']\
                        .isin([
                            'TRANS_OPENLINK', 'TRANS_OPENLINK_ESM', 'TRANS_SODOGI_ESM',
                            'TRANS_VBAP', 'TRANS_VBKD', 'TRANS_ZWBMM', 'TRANS_ZWBMM_CERTI'
                        ]),
                    'list_initial_plant'
                ] = '-'

                new_filtered_cols = [
                    'task_name', 'start_process_timestamp', 'records_from_s3', 'records_s3_inserted_to_raw',
                    'columns_from_s3', 'sum_source_num_column', 'sum_dest_num_column', 'duration_s3_to_raw',
                    'status_record_s3_to_raw', 'status_column_s3_to_raw', 'records_raw_processed_to_clean',
                    'total_dest_column', 'duration_raw_to_clean', 'status_records_raw_to_clean', 'remark_etl',
                    'initial_total_transaction', 'list_initial_plant', 'processed_total_transaction',
                    'list_processed_plant', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
                    'remark_unprocessed_trans_to_event', 'list_disconnected_trans_id', 'status'
                ]
                etl_traceability_log = etl_traceability_log[new_filtered_cols]

                etl_traceability_log = etl_traceability_log.\
                    rename(columns={
                        'task_name':'Task Name',
                        'start_process_timestamp':'Start Process Timestamp',
                        'records_from_s3':'Records from S3',
                        'records_s3_inserted_to_raw':'Records S3 Inserted to Raw',
                        'columns_from_s3':'Columns from S3',
                        'sum_source_num_column':'Sum of Sample Numerical Column S3',
                        'sum_dest_num_column':'Sum of Sample Numerical Column Raw',
                        'duration_s3_to_raw':'Duration S3 to Raw',
                        'status_record_s3_to_raw':'Status Record S3 to Raw',
                        'status_column_s3_to_raw':'Status Column S3 to Raw',
                        'records_raw_processed_to_clean':'Records Raw Processed to Clean',
                        'total_dest_column':'Columns Raw to Clean',
                        'duration_raw_to_clean':'Duration Raw to Clean',
                        'status_records_raw_to_clean':'Status Record Raw to Clean',
                        'remark_etl':'Remark ETL',
                        'initial_total_transaction':'[DB Clean] Total Transaction',
                        'list_initial_plant':'[DB Clean] List Plant',
                        'processed_total_transaction':'[Processed] Total Transaction',
                        'list_processed_plant':'[Processed] List Plant',
                        'unprocessed_total_transaction':'[Unprocessed] Total Transaction',
                        'list_unprocessed_plant_trans_to_event':'[Unprocessed] List Plant',
                        'remark_unprocessed_trans_to_event':'[Unprocessed] Issue Log',
                        'list_disconnected_trans_id':'List Unidentified Trans ID',
                        'status':'Daily Traceability Status'
                    })
                
                print('Separate ETL Report and Traceability Report...')
                etl_log_ = etl_traceability_log[[
                    'Task Name', 'Start Process Timestamp', 'Records from S3',
                    'Records S3 Inserted to Raw', 'Columns from S3',
                    'Sum of Sample Numerical Column S3',
                    'Sum of Sample Numerical Column Raw', 'Duration S3 to Raw',
                    'Status Record S3 to Raw', 'Status Column S3 to Raw',
                    'Records Raw Processed to Clean', 'Columns Raw to Clean',
                    'Duration Raw to Clean', 'Status Record Raw to Clean', 'Remark ETL'
                ]]

                traceability_log_ = etl_traceability_log[[
                    'Task Name', 'Daily Traceability Status', '[DB Clean] Total Transaction',
                    '[DB Clean] List Plant', '[Processed] Total Transaction', '[Processed] List Plant',
                    '[Unprocessed] Total Transaction', '[Unprocessed] List Plant',
                    '[Unprocessed] Issue Log', 'List Unidentified Trans ID', 

                ]]
                traceability_log_ = traceability_log_[~traceability_log_['Task Name'].isin(['TRANS_VBAP', 'TRANS_VBKD', 'TRANS_ZWBMM', 'TRANS_ZWBMM_CERTI'])]
                
                print('Checking Partial Trans...\n')
                data_needs_tobe_monitored = pd.concat([data_processed_mb51_ds, data_processed_mb51_us, data_processed_mb51_esm, data_processed_zmb_us], axis=0)
                data_needs_tobe_monitored = data_needs_tobe_monitored[['task_name', 'id']]

                list_monitored_task = data_needs_tobe_monitored['task_name'].to_list()
                list_monitored_id = data_needs_tobe_monitored['id'].to_list()

                if len(list_monitored_id) > 0:
                    monitoring_detail_partial = mrt.daily_monitoring_partial(db_conn, list_monitored_task, list_monitored_id)
                else:
                    monitoring_detail_partial = pd.DataFrame(columns=[
                        'task_name', 'id', 'status_restan', 'status_events_bc',
                        'status_traceability', 'qty_restan', 'update_date_restan',
                        'is_disconnected'
                    ])

                print('Creating Internal Code & Glosary...\n')
                df_internal_code = ma.internal_code()
                df_glosary = ma.glosary()

                print(f"Uploading report to S3 at {report_path}.")
                # ma.save_df_to_s3(etl_log_, traceability_log_, monitoring_detail_partial, df_internal_code, df_glosary, s3, s3_bucket, report_path)

                print("Sending ETL-Traceability report to email.")
                # ma.send_email_with_attachment(s3, s3_bucket, report_path, recipients, current_date, etl_log_, total_data_clean, total_disconnected_trans_id, report_custom_date)
            
            else:
                print(f'There is no ETL activity on {current_date}.')
                # ma.send_email_no_activity(recipients, current_date, report_custom_date)
            
            print(f"\nReport processed and email sent successfully.")
            break

        else:
            print("Traceability Status is not 'Done'. Waiting for 15 minutes...")
            time.sleep(900)
    
except Exception as e:
    db_conn.rollback()
    print(f"Error processing report: {e}")
    raise e


#close db
db_conn.close()
db_cursor.close()
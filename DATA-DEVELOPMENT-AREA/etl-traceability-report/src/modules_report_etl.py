#Import the required libraries
import pandas as pd
import psycopg2
import codecs
import math
from io import StringIO
from datetime import date

#ETL LOG FUNCTIONS ----------------------------------------------------------------------------------------

#status check
def check_status(connection_param):
    query_status = """
        select state
        from task_ecs_log_test
        where task_name in ('traceability', 'traceability_cepsa_daily')
        order by input_date desc
        limit 1
        ;
    """
    status = pd.read_sql(query_status, con=psycopg2.connect(**connection_param))
    return status['state'][0] if not status.empty else None

#define num col trace
def numerical_trace_cols(file_name):
    if 'trans_ds_material_movement' in file_name:
        return 'qty_in_un_of_entry'
    elif 'trans_us_material_movement' in file_name:
        return 'qty_in_un_of_entry'
    elif 'trans_mm_mb51_esp' in file_name:
        return 'qty_in_un_of_entry'
    elif 'openlink_gai_contract' in file_name:
        return 'parcel_bl_quantity'
    elif 'trans_sd_sodogi' in file_name:
        return 'do_qty'
    elif 'trans_zmb_mill' in file_name:
        return 'quantity'
    elif 'trans_zwbmm_certi' in file_name:
        return None
    elif 'trans_zwbmm' in file_name:
        return 'data_element_kg_wb'
    elif 'trans_vbap' in file_name:
        return None
    elif 'trans_vbkd' in file_name:
        return None
    elif 'trans_sd_esp_sales_shipment' in file_name:
        return 'issued_qty'
    elif 'trans_soci_ds_material_movement' in file_name:
        return 'qty_in_un_of_entry'
    else:
        return None

#get col information from s3 file
def s3_cols_information(s3_client, check_dat, check_col):
    bucket = 'trace-dataset'
    col_informations = {}

    for key, col in zip(check_dat, check_col):
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()

            df = None
            encodings = ['utf-8', 'latin1', 'ascii']
            for encoding in encodings:
                try:
                    decoded_content = codecs.decode(content, encoding=encoding)
                    df = pd.read_csv(StringIO(decoded_content), sep='~\|\^', quotechar='"', encoding=encoding, dtype=str)
                    break
                except UnicodeDecodeError as e:
                    print(f"Error decoding DAT file with encoding {encoding}: {e}. Trying a different encoding...")

            if df is None:
                raise ValueError("Your encodings are not compatible with the data.")
            else:
                count_col = len(df.columns)
                if col!=None:
                    df[col] = df[col].apply(lambda x: '-' + x.removesuffix('-') if x.endswith('-') else x)
                    sum_num_col = df[col].astype(float).sum()
                    col_informations[key.replace('trans/','')] = [count_col, col, math.floor(sum_num_col)]
                else:
                    col_informations[key.replace('trans/','')] = [count_col, '-', 0]
        except Exception as e:
            print(f"Error while process key {key}: {e}")
    
    return col_informations

#get col information from db_raw
def raw_cols_information(connection_param, task, process, source_col):
    query_total_cols = f"""
        select count(0) cnt
        from parameter_etl_import_raw
        where
            task_name = '{task}' and
            source_column_name not in ('input_date', 'etl_date', 'process_id', 'id', 'recnum', 'hash')
    """
    total_cols = pd.read_sql(query_total_cols, con=psycopg2.connect(**connection_param))
    if not total_cols.empty:
        total_cols = total_cols['cnt'].iloc[0]
    else:
        total_cols = 0
    
    query_target_col = f"""
        select destination_column_name, destination_db
        from parameter_etl_import_raw
        where
            task_name = '{task}' and
            source_column_name = '{source_col}'
        limit 1
    """
    target = pd.read_sql(query_target_col, con=psycopg2.connect(**connection_param))
    if not target.empty:
        target_col = target['destination_column_name'].iloc[0]
        target_table = target['destination_db'].iloc[0]

        if task == 'TRANS_ZWBMM':
            query_sum_target_col = f"""
                select sum({target_col}::float) sum_num_target_col
                from {target_table}
                where process_id = '{process}'
            """
        else:
            query_sum_target_col = f"""
                select sum({target_col})::float sum_num_target_col
                from {target_table}
                where process_id = '{process}'
            """
        sum_target_col = pd.read_sql(query_sum_target_col, con=psycopg2.connect(**connection_param))
        sum_target_col = math.floor(sum_target_col['sum_num_target_col'].iloc[0])
    else:
        if task == 'TRANS_OPENLINK_ESM' and source_col=='issued_qty':
            target_col = 'parcel_bl_quantity'
            target_table = 'trans_openlink_esm'

            query_sum_target_col = f"""
                select sum({target_col}) sum_num_target_col
                from {target_table}
                where process_id = '{process}'
            """
            sum_target_col = pd.read_sql(query_sum_target_col, con=psycopg2.connect(**connection_param))
            sum_target_col = math.floor(sum_target_col['sum_num_target_col'].iloc[0])
        else:
            target_col = '-'
            sum_target_col = 0
    
    return total_cols, target_col, sum_target_col

#import log
def process_import_log(s3_client, connection_param, date_param):
    query_import_log = f"""
        with cte as(
            select
                task_name, file_name, process_id, index_range,
                total_source_row::int,
                total_destination_row::int,
                (process_date + interval '7 hours')::timestamp process_date,
                (end_process_timestamp + interval '7 hours')::timestamp end_process_timestamp,
                row_number() over(partition by task_name, file_name, index_range, process_status order by end_process_timestamp desc) priority
            from import_log_new
            where (process_date + interval '7 hours')::date = '{date_param}'
        )
        select * from cte
        where cte.priority = 1
        ;
    """
    df_import_log = pd.read_sql(query_import_log, con=psycopg2.connect(**connection_param))

    list_check_dat = [f'trans/{x}' for x in df_import_log['file_name'].unique()]
    list_check_cols = [numerical_trace_cols(file) for file in list_check_dat]
    col_informations = s3_cols_information(s3_client, list_check_dat, list_check_cols)

    df_import_log['task_name'] = df_import_log['task_name'].replace({'rpt_trc':'MASTER_DOC', 'MASTER_DOCUMENT':'MASTER_DOC'})
    df_import_log['total_source_column'], df_import_log['source_num_column'], df_import_log['sum_source_num_column'] = zip(*df_import_log['file_name'].apply(lambda x: col_informations[x]))
    df_import_log['total_dest_column'], df_import_log['dest_num_column'], df_import_log['sum_dest_num_column'] = zip(*df_import_log.apply(lambda x: raw_cols_information(connection_param, x['task_name'], x['process_id'], x['source_num_column']), axis=1))
    df_import_log = df_import_log.groupby(['task_name', 'total_source_column', 'source_num_column', 'total_dest_column', 'dest_num_column'])\
        .agg({'total_source_row':'sum',
            'total_destination_row':'sum',
            'sum_source_num_column':'sum',
            'sum_dest_num_column':'sum',
            'process_date':'min',
            'end_process_timestamp':'max'})\
        .reset_index()

    df_import_log = df_import_log.rename(columns={
        'total_source_column':'columns_from_s3',
        'total_source_row':'records_from_s3',
        'total_destination_row':'records_s3_inserted_to_raw',
        'process_date':'start_import_timestamp',
        'end_process_timestamp':'end_import_timestamp'
    })

    df_import_log['duration_s3_to_raw'] = df_import_log.apply(lambda x: str(round(pd.to_timedelta(x['end_import_timestamp'] - x['start_import_timestamp']).total_seconds(), 2)) + 's', axis=1)

    if len(df_import_log) > 0:
        df_import_log['status_record_s3_to_raw'] = df_import_log.apply(lambda row: 'PASS' if (row['records_from_s3'] == row['records_s3_inserted_to_raw']) else 'NOT PASS', axis=1)
        df_import_log['status_column_s3_to_raw'] = df_import_log.apply(lambda row: 'NOT PASS' if (row['sum_source_num_column'] != row['sum_dest_num_column']) and (row['dest_num_column']!='-') else 'PASS', axis=1)
    else:
        df_import_log['status_record_s3_to_raw'] = pd.Series(dtype='float')
        df_import_log['status_column_s3_to_raw'] = pd.Series(dtype='float')

    return df_import_log

#migration log
def process_migration_log(connection_param, date_param):
    query_migration_log = f"""
        with cte as(
            select
                task_name, process_id, (start_migration_timestamp + interval '7 hours') start_migration_timestamp, duration,
                row_number() over(partition by task_name, process_id order by start_migration_timestamp desc) priority
            from migration_log
            where
                (start_migration_timestamp + interval '7 hours')::date = '{date_param}' and
                task_name like 'TRANS%'
        ),
        cte2 as(
            select * from cte where cte.priority=1
        )
        select
            cte2.task_name, cte2.process_id,
            (select count(0) from migration_log_detail mld where mld.process_id = cte2.process_id and mld.is_skipped is null and (mld.process_date + interval '7 hours')::date = '{date_param}') records_raw_inserted_to_clean,
            (select count(0) from migration_log_detail mld where mld.process_id = cte2.process_id and mld.is_skipped = 0 and (mld.process_date + interval '7 hours')::date = '{date_param}') records_raw_that_update_clean,
            (select count(0) from migration_log_detail mld where mld.process_id = cte2.process_id and mld.is_skipped in (1,-1) and (mld.process_date + interval '7 hours')::date = '{date_param}') records_raw_skipped,
            cte2.start_migration_timestamp, cte2.duration
        from cte2;
    """
    df_migration_log = pd.read_sql(query_migration_log, con=psycopg2.connect(**connection_param))
    df_migration_log = df_migration_log[~(
        (df_migration_log['records_raw_inserted_to_clean']==0) &
        (df_migration_log['records_raw_that_update_clean']==0) &
        (df_migration_log['records_raw_skipped']==0)
    )]

    df_migration_log = df_migration_log.groupby('task_name')\
        .agg({'records_raw_inserted_to_clean':'sum',
            'records_raw_that_update_clean':'sum',
            'records_raw_skipped':'sum',
            'start_migration_timestamp':'min',
            'duration':'sum'})\
        .reset_index()

    df_migration_log['records_raw_processed_to_clean'] = df_migration_log['records_raw_inserted_to_clean'] + df_migration_log['records_raw_that_update_clean'] + df_migration_log['records_raw_skipped']
    df_migration_log['duration'] = df_migration_log['duration'].apply(lambda x: str(round(x, 2)) + 's')
    df_migration_log = df_migration_log.rename(columns={'duration':'duration_raw_to_clean'})

    return df_migration_log

#info filtered raw data
def info_filtered_data(connection_param, date_param):
    query_info_filtered_data = f"""
        with cte as(
            select distinct
                task_name, process_id
            from import_log_new
            where
                (process_date + interval '7 hours')::date = '{date_param}' and
                task_name in ('TRANS_OPENLINK', 'TRANS_SODOGIDS', 'TRANS_ZMB_US')
        ),
        cte2 as(
            select
                cte.task_name, cte.process_id,
                case
                    when cte.task_name = 'TRANS_OPENLINK' then
                        (
                        select count(0) from trans_openlink to2
                        where
                            to2.process_id = cte.process_id and
                            delivery_status <> 'Approved'
                        )
                    when cte.task_name = 'TRANS_SODOGIDS' then
                        (
                        select count(0) from trans_sodogi_ds ts
                        where
                            ts.process_id = cte.process_id and
                            (
                                (coalesce(nullif(ts.sales_order_no, 0), null) is null) or
                                (coalesce(nullif(ts.do_qty, 0), null) is null) or
                                (coalesce(nullif(ts.billing_no, 0), null) is null)
                            )
                        )
                    when cte.task_name = 'TRANS_ZMB_US' then
                        (
                        select count(0) from trans_zmb_us tz
                        where
                            tz.process_id = cte.process_id and
                            (tz.process_information = 'No Mapping Yet' or tz.date >= '{date_param}')
                        )
                    else 0
                end as not_processed_raw_data,
                
                case
                    when cte.task_name = 'TRANS_OPENLINK' then
                        (
                        select sum(parcel_bl_quantity) from trans_openlink to2
                        where
                            to2.process_id = cte.process_id and
                            not delivery_status <> 'Approved'
                        )
                    when cte.task_name = 'TRANS_SODOGIDS' then
                        (
                        select sum(do_qty) from trans_sodogi_ds ts
                        where
                            ts.process_id = cte.process_id and
                            not 
                            (
                                (coalesce(nullif(ts.sales_order_no, 0), null) is null) or
                                (coalesce(nullif(ts.do_qty, 0), null) is null) or
                                (coalesce(nullif(ts.billing_no, 0), null) is null)
                            )
                        )
                    when cte.task_name = 'TRANS_ZMB_US' then
                        (
                        select sum(total) from trans_zmb_us tz
                        where
                            tz.process_id = cte.process_id and
                            not (tz.process_information = 'No Mapping Yet' or tz.date >= '{date_param}')
                        )
                    else 0
                end as sum_val_raw_filtered_col
                
            from cte
        )
        select
            cte2.task_name,
            sum(not_processed_raw_data)::bigint not_processed_raw_data,
            sum(sum_val_raw_filtered_col)::bigint sum_val_raw_filtered_col
        from cte2
        group by cte2.task_name
        ;
    """
    info_filtered_data = pd.read_sql(query_info_filtered_data, con=psycopg2.connect(**connection_param))
    
    return info_filtered_data

#notes etl
def notes_etl(x):
    list_notes = []
    
    if x['task_name'].startswith('TRANS'):
        if x['source_num_column'] != '-' and x['dest_num_column'] != '-':
            list_notes.append(f"Sample numerical column compared: `{x['source_num_column']}` from S3 and `{x['dest_num_column']}` from DB Raw.")
        else:
            pass

        if x['sum_source_num_column'] != x['sum_dest_num_column']:
            list_notes.append("There is a discrepancy in the sum of values from numerical column compared.")
        else:
            pass

        if x['not_processed_raw_data'] > 0 and x['task_name'] == 'TRANS_OPENLINK':
            list_notes.append(f"{x['not_processed_raw_data']} records raw was not processed to DB Clean because delivery_status <> 'Approved'.")
        elif x['not_processed_raw_data'] > 0 and x['task_name'] == 'TRANS_SODOGIDS':
            list_notes.append(f"{x['not_processed_raw_data']} records raw was not processed to DB Clean because (sales_order_no is 0 or null) or (do_qty is 0 or null) or (billing_no is 0 or null and gi_no is 0 or null).")
        elif x['not_processed_raw_data'] > 0 and x['task_name'] == 'TRANS_ZMB_US':
            list_notes.append(f"{x['not_processed_raw_data']} records raw was not processed to DB Clean because process_information = 'No Mapping Yet' or date >= current date.")
        else:
            pass
        
        if x['records_s3_inserted_to_raw'] != x['not_processed_raw_data'] + x['records_raw_processed_to_clean']:
            list_notes.append("There is a discrepancy in records between the 's3 to raw' process and the 'raw to clean' process.")
        else:
            pass
        
        if x['records_raw_inserted_to_clean'] + x['records_raw_that_update_clean'] + x['records_raw_skipped'] > 0:
            list_notes.append(f"{x['records_raw_inserted_to_clean']} records raw inserted to DB Clean, {x['records_raw_that_update_clean']} records raw that update DB Clean, {x['records_raw_skipped']} records raw skipped.")
        else:
            pass
        
    else:
        if pd.isna(x['records_raw_processed_to_clean']) or x['records_raw_processed_to_clean'] == 0:
            list_notes.append("Approval process is required to perform data migration from raw to clean.")
        else:
            pass

    return '\n'.join(f"- {value}" for value in list_notes)

#create df unprocessed file
def create_df_unprocessed_file(connection_param, list_task, date_param):
    #list task name before 12 March 2025
    if date_param < date(2025,3,12):
        query_all_existing_task_name = '''
            select distinct task_name 
            from migration_log ml 
            where
                task_name like 'TRANS%' and
                task_name not in ('TRANS_TRADER', 'TRANS_SOCIMAS')
            ;
        '''
    else:
        query_all_existing_task_name = '''
            select distinct task_name 
            from migration_log ml 
            where
                task_name like 'TRANS%' and
                task_name not in ('TRANS_TRADER')
            ;
        '''
    all_existing_task_name = pd.read_sql(query_all_existing_task_name, con=psycopg2.connect(**connection_param))
    all_existing_task_name = all_existing_task_name['task_name'].to_list()
    list_unprocessed_task = [task for task in all_existing_task_name if task not in list_task]
    
    df = pd.DataFrame(
        {'task_name':list_unprocessed_task}
    )

    cols_with_val_notpass = ['status_record_s3_to_raw', 'status_column_s3_to_raw', 'status_records_raw_to_clean']
    cols_with_val_zero = [
        'records_from_s3', 'records_s3_inserted_to_raw', 'records_raw_processed_to_clean',
        'records_raw_inserted_to_clean', 'records_raw_that_update_clean'
    ]
    cols_with_val_strip = [
        'start_process_timestamp', 'columns_from_s3', 'sum_source_num_column', 'sum_dest_num_column',
        'duration_s3_to_raw', 'total_dest_column', 'duration_raw_to_clean'
    ]
    
    for col in cols_with_val_notpass:
        df[col] = 'NOT PASS'
    for col in cols_with_val_zero:
        df[col] = '0'
    for col in cols_with_val_strip:
        df[col] = '-'
    df['remark_etl'] = 'The transaction file is not available in S3'

    return df

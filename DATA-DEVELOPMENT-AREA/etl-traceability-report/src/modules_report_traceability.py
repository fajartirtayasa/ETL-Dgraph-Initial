#Import the required libraries
import pandas as pd
import hashlib
import re
from datetime import timedelta


#TRACEABILITY LOG FUNCTIONS ----------------------------------------------------------------------------------------

#get processed recnum transactional data
def get_processed_recnum_trans(connection_db, date_param):
    query_processed_recnum_trans = f"""
        with cte as (
            select
                task_name,
                process_id,
                (start_migration_timestamp + interval '7 hours') as start_migration_timestamp,
                duration,
                row_number() over(partition by task_name, process_id order by start_migration_timestamp desc) as priority
            from migration_log
            where
                (start_migration_timestamp + interval '7 hours')::date = '{date_param}' and
                task_name like 'TRANS%'
        ),
        cte2 as (
            select * from cte where priority = 1
        )
        select
            cte2.task_name,
            cte2.process_id,
            mld.recnum
        from cte2
        left join migration_log_detail mld
        on cte2.process_id = mld.process_id
        where mld.is_skipped is null or mld.is_skipped = 0;
    """
    processed_recnum_trans = pd.read_sql(query_processed_recnum_trans, connection_db)
    return processed_recnum_trans

#define target table
def get_target_table(task):
    mapping_task = {
        'TRANS_MB51DS':'trans_mb51_ds',
        'TRANS_MB51_ESM':'trans_mb51_esm',
        'TRANS_MB51US':'trans_mb51_us',
        'TRANS_OPENLINK':'trans_openlink',
        'TRANS_OPENLINK_ESM':'trans_openlink_esm',
        'TRANS_SODOGIDS':'trans_sodogi',
        'TRANS_SODOGI_ESM':'trans_sodogi_esm',
        'TRANS_ZMB_US':'trans_zmb_us'
    }
    try:
        target = mapping_task[task]
    except:
        target = None
    return target

#get transactional id for each recnum
def get_ids_for_recnums(connection_db, table_name, recnums):
    query_ids_for_recnums = f"select id, recnum from {table_name} where recnum = any(%s)"
    with connection_db.cursor() as cur:
        cur.execute(query_ids_for_recnums, (recnums,))
        return cur.fetchall()

#get event informations
def get_events_info(connection_db, list_id):
    query_events_info = f"select unique_id, id_inc, plant from events_bc where unique_id = any(%s)" #untuk saat ini unique_id di events_bc Prod masih unik, nanti perlu ada adjustment semisal sudah tidak unik karena ada versioning
    with connection_db.cursor() as cur:
        cur.execute(query_events_info, (list_id,))
        return cur.fetchall()

#transform data processed
def transform_data_processed(df):
    total_plant_processed = df\
        .groupby('task_name')['plant'].nunique()\
        .reset_index()\
        .rename(columns={'plant':'processed_plant'})

    df = pd.merge(df, total_plant_processed, how='left', on='task_name')

    df = df\
        .groupby(['task_name', 'total_data_clean', 'processed_plant'])\
        .agg({'id_inc': 'count', 'plant': lambda x: '; '.join(sorted(x.unique()))})\
        .reset_index()\
        .rename(columns={'id_inc': 'processed_trans_to_event', 'plant': 'list_processed_plant'})
    
    df.loc[df['task_name'] == 'TRANS_OPENLINK', 'processed_plant'] = 0

    return df

#transform data skipped
def transform_data_skipped(df):
    filtered_task = ['TRANS_VBAP', 'TRANS_VBKD', 'TRANS_ZWBMM', 'TRANS_ZWBMM_CERTI']
    df = df[df['task_name'].isin(filtered_task)]
    df = df.drop_duplicates()

    df['total_data_clean'] = 0
    df['remark_unprocessed_trans_to_event'] = 'This transaction data is not loaded into events_bc.'
    df['status'] = 'PASS'
    for col in ['processed_total_transaction', 'unprocessed_total_transaction']:
        df[col] = '0 Row; 0 Plant'
    for col in ['list_processed_plant', 'list_unprocessed_plant_trans_to_event', 'list_disconnected_trans_id']:
        df[col] = '-'
    
    df = df[[
        'task_name', 'total_data_clean', 'processed_total_transaction',
        'list_processed_plant', 'unprocessed_total_transaction',
        'list_unprocessed_plant_trans_to_event', 'remark_unprocessed_trans_to_event',
        'list_disconnected_trans_id', 'status'
    ]]
    return df

#notes traceability
def notes_trace(task, x, date_param):

    notes = []

    if task=='TRANS_MB51DS':
        movementtype = x['movementtype']
        status = x['status']
        qty_entry = x['qty_entry']
        qty_out = x['qty_out']
        qty_restan = x['qty_restan']
        entry_date = x['entry_date']

        if status!=None and qty_out!=0 and qty_restan==0:
            notes.append('H-RESTAN01')
        if status!=None and qty_out!=0 and qty_restan!=0:
            notes.append('H-RESTAN02')
        if status=='Yes' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN01')
        if status=='No' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN02')
        if movementtype%2==0:
            notes.append('M-MB51DS01')
        if status=='No' and qty_entry==0 and qty_out==0 and qty_restan==0:
            notes.append('M-MB51DS02')
        if entry_date >= date_param:
            notes.append('M-MB51DS03')
        if not notes:
            notes.append('H-UNKNOWN')

    elif task=='TRANS_MB51US':
        material = x['material']
        ordertype = x['ordertype']
        movementtype = x['movementtype']
        qtyinunofentry = x['qtyinunofentry']
        storagelocation = x['storagelocation']
        type = x['type']
        mill_zmb_us_status = x['mill_zmb_us_status']
        entry_date = x['entry_date']
        text = x['text']
        status = x['status']
        qty_entry = x['qty_entry']
        qty_out = x['qty_out']
        qty_restan = x['qty_restan']
        entry_date = x['entry_date']

        if material in [80000005, 80000006, 80000007]:
            notes.append('L-MB51US01')
        if type=='estate' and material==80000010:
            notes.append('L-MB51US02')
        if type=='estate' and ordertype!=None:
            notes.append('L-MB51US03')
        if type=='estate' and movementtype not in ['351', '961', '601']:
            notes.append('L-MB51US04')
        if type=='mill' and mill_zmb_us_status=='available' and material==80000010:
            notes.append('L-MB51US05')
        if type=='mill' and mill_zmb_us_status=='available' and ordertype!=None:
            notes.append('L-MB51US06')
        if type=='mill' and mill_zmb_us_status=='available' and movementtype=='311':
            notes.append('L-MB51US07')
        if type=='mill' and mill_zmb_us_status=='available' and movementtype=='961' and text!=None and bool(re.search(r'jual|beli|sampel|produksi|ass', text.lower())):
            notes.append('L-MB51US07')
        if type=='mill' and mill_zmb_us_status=='available' and not (movementtype in ['351', '961', '311', '601'] and qtyinunofentry<0 and ('CPO' in storagelocation or 'PK' in storagelocation)):
            notes.append('L-MB51US07')
        if status!=None and qty_out!=0 and qty_restan==0:
            notes.append('H-RESTAN01')
        if status!=None and qty_out!=0 and qty_restan!=0:
            notes.append('H-RESTAN02')
        if status=='Yes' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN01')
        if status=='No' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN02')
        if int(movementtype[-1])%2==0 and movementtype!='914':
            notes.append('M-MB51US01')
        if status=='No' and qty_entry==0 and qty_out==0 and qty_restan==0:
            notes.append('M-MB51US02')
        if entry_date >= date_param:
            notes.append('M-MB51US03')
        if not notes:
            notes.append('H-UNKNOWN')
    
    elif task=='TRANS_OPENLINK':
        backstop_id = x['backstop_id']
        contract_type = x['contract_type']
        bl_date = x['bl_date']
        sap_so_po_number = x['sap_so_po_number']
        delivery_status = x['delivery_status']
        status = x['status']
        is_mb51ds_available = x['is_mb51ds_available']
        is_sodogi_available = x['is_sodogi_available']
        is_valid_sodogi_available = x['is_valid_sodogi_available']
        is_dsi_available = x['is_dsi_available']
        is_valid_dsi_available = x['is_valid_dsi_available']
        is_valid_id_dsi_in_events = x['is_valid_id_dsi_in_events']
        is_invalid_id_dsi_in_events = x['is_invalid_id_dsi_in_events']
        is_valid_sodogi_available_2 = x['is_valid_sodogi_available_2']
        is_valid_sodogi_in_events = x['is_valid_sodogi_in_events']
        is_invalid_sodogi_in_events = x['is_invalid_sodogi_in_events']
        is_valid_sodogi_in_events_2 = x['is_valid_sodogi_in_events_2']
        is_invalid_sodogi_in_events_2 = x['is_invalid_sodogi_in_events_2']
        is_sap_so_po_number_dsi_already_in_events = x['is_sap_so_po_number_dsi_already_in_events']
        is_backstop_3ps_already_in_events = x['is_backstop_3ps_already_in_events']

        if delivery_status != 'Approved':
            notes.append('L-OPNK01')
        if contract_type == 'GAR Interco Purchase':
            notes.append('L-OPNK02')
        if backstop_id == 0:
            notes.append('L-OPNK03')
        if contract_type == 'DSI Interco Sales' and is_mb51ds_available == 0:
            notes.append('M-OPNK01')
        if contract_type == 'DSI Interco Sales' and is_sodogi_available == 0:
            notes.append('M-OPNK02')
        if contract_type == 'DSI Interco Sales' and is_sodogi_available == 1 and is_valid_sodogi_available == 0:
            notes.append('M-OPNK03')
        if contract_type == 'DSI Interco Sales' and sap_so_po_number == 0:
            notes.append('M-OPNK04')
        if contract_type == '3rd Party Sales' and is_dsi_available == 0:
            notes.append('M-OPNK05')
        if contract_type == '3rd Party Sales' and is_dsi_available == 1 and is_valid_dsi_available == 0:
            notes.append('M-OPNK06')
        if contract_type == '3rd Party Sales' and is_dsi_available == 1 and is_valid_dsi_available == 1 and is_valid_sodogi_available_2 == 0:
            notes.append('M-OPNK07')
        if bl_date == None:
            notes.append('M-OPNK08')
        if contract_type == '3rd Party Sales' and is_backstop_3ps_already_in_events == 1:
            notes.append('M-OPNK09')
        if contract_type == 'DSI Interco Sales' and is_sap_so_po_number_dsi_already_in_events == 1:
            notes.append('M-OPNK10')
        if status == 'invalid':
            notes.append('L-OPNK04')
        if (is_valid_sodogi_in_events == 0 and is_invalid_sodogi_in_events == 1) or (is_valid_sodogi_in_events_2 == 0 and is_invalid_sodogi_in_events_2 == 1):
            notes.append('H-OPNK01')
        if contract_type == '3rd Party Sales' and is_valid_id_dsi_in_events == 0 and is_invalid_id_dsi_in_events == 1:
            notes.append('H-OPNK02')
        if not notes:
            notes.append('H-UNKNOWN')
    
    elif task=='TRANS_SODOGIDS':
        sales_order_no = x['sales_order_no']
        do_qty = x['do_qty']
        do_number = x['do_number']
        batch_no = x['batch_no']
        status = x['status']
        cnt_mb51ds_id = x['cnt_mb51ds_id']
        mb51ds601_connected_to_mb51ds101 = x['mb51ds601_connected_to_mb51ds101']
        so_and_batch_in_events_status = x['so_and_batch_in_events_status']
        so_in_events_status = x['so_in_events_status']
        actual_gi_date = x['actual_gi_date']

        if do_qty == 0:
            notes.append('M-SODOGI01')
        if do_number == None:
            notes.append('M-SODOGI02')
        if batch_no == None:
            notes.append('M-SODOGI03')
        if sales_order_no == 0:
            notes.append('M-SODOGI04')
        if sales_order_no != 0 and cnt_mb51ds_id == 0:
            notes.append('M-SODOGI05')
        if sales_order_no != 0 and batch_no != None and cnt_mb51ds_id > 0 and so_and_batch_in_events_status == 'available':
            notes.append('M-SODOGI06')
        if sales_order_no != 0 and batch_no != None and cnt_mb51ds_id > 0 and so_and_batch_in_events_status == 'not available' and so_in_events_status == 'available':
            notes.append('M-SODOGI07')
        if actual_gi_date == None:
            notes.append('M-SODOGI08')
        if actual_gi_date != None and actual_gi_date >= date_param - timedelta(days=1):
            notes.append('M-SODOGI09')
        if sales_order_no != 0 and cnt_mb51ds_id > 0 and mb51ds601_connected_to_mb51ds101 == 1:
            notes.append('M-SODOGI10')
        if status == 'invalid':
            notes.append('L-SODOGI01')
        if not notes:
            notes.append('H-UNKNOWN')
    
    elif task=='TRANS_ZMB_US':
        total = x['total']
        process_information = x['process_information']
        status = x['status']
        qty_out = x['qty_out']
        qty_restan = x['qty_restan']
        range_date = x['range_date']
        date = x['date']

        if total==0:
            notes.append('L-ZMB01')
        if process_information.startswith(('Kirim', 'TBS Kirim', 'Stock')) or process_information=='No Mapping Yet':
            notes.append('L-ZMB02')
        if status!=None and qty_out==0 and qty_restan==0:
            notes.append('M-ZMB01')
        if status!=None and qty_out!=0 and qty_restan==0:
            notes.append('H-RESTAN01')
        if status!=None and qty_out!=0 and qty_restan!=0:
            notes.append('H-RESTAN02')
        if status=='Yes' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN01')
        if status=='No' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN02')
        if range_date > timedelta(1):
            notes.append('M-ZMB02')
        if date >= date_param - timedelta(days=1):
            notes.append('M-ZMB03')
        if not notes:
            notes.append('H-UNKNOWN')
    
    elif task=='TRANS_OPENLINK_ESM':
        sap_so_po_number = x['sap_so_po_number']
        so_po_in_sodogi_status = x['so_po_in_sodogi_status']
        do_numbers = x['do_numbers']
        do_number_sodogi_in_mb51_status = x['do_number_sodogi_in_mb51_status']
        bl_date = x['bl_date']
        bl_number = x['bl_number']

        if so_po_in_sodogi_status=='not available':
            notes.append('M-OPNK_ESM01')
        if so_po_in_sodogi_status=='available' and do_numbers==None:
            notes.append('M-OPNK_ESM02')
        if so_po_in_sodogi_status=='available' and do_numbers!=None and do_number_sodogi_in_mb51_status=='not available':
            notes.append('M-OPNK_ESM03')
        if bl_date==None:
            notes.append('M-OPNK_ESM04')
        if bl_number==None:
            notes.append('M-OPNK_ESM05')
        if sap_so_po_number!=None and str(sap_so_po_number).startswith(('35','36')):
            notes.append('L-OPNK_ESM01')
        if not notes:
            notes.append('H-UNKNOWN')
    
    elif task=='TRANS_SODOGI_ESM':
        do_number = x['do_number']
        issued_qty = x['issued_qty']
        do_number_sodogi_in_mb51_status = x['do_number_sodogi_in_mb51_status']
        good_issued_date = x['good_issued_date']

        if do_number==None:
            notes.append('M-SODOGI_ESM01')
        if issued_qty==0:
            notes.append('M-SODOGI_ESM02')
        if do_number!=None and do_number_sodogi_in_mb51_status=='not available':
            notes.append('M-SODOGI_ESM03')
        if good_issued_date==None:
            notes.append('M-SODOGI_ESM04')
        if not notes:
            notes.append('H-UNKNOWN')
    
    elif task=='TRANS_MB51_ESM':
        movement_type = x['movement_type']
        status = x['status']
        entry_date = x['entry_date']
        qty_entry = x['qty_entry']
        qty_out = x['qty_out']
        qty_restan = x['qty_restan']

        if status!=None and qty_out!=0 and qty_restan==0:
            notes.append('H-RESTAN01')
        if status!=None and qty_out!=0 and qty_restan!=0:
            notes.append('H-RESTAN02')
        if status=='Yes' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN01')
        if status=='No' and qty_out==0 and qty_restan!=0:
            notes.append('M-RESTAN02')
        if movement_type not in [131,531,101,301,311,917,309,261,601,321,344,343,453,451,651,981,971,641,645]:
            notes.append('M-MB51_ESM02')
        if entry_date >= date_param:
            notes.append('M-MB51_ESM03')
        if movement_type%2==0:
            notes.append('M-MB51_ESM01')
        if status=='No' and qty_entry==0 and qty_out==0 and qty_restan==0:
            notes.append('M-MB51_ESM01')
        if not notes:
            notes.append('H-UNKNOWN')
    
    else:
        notes.append('H-UNKNOWN')

    return notes

#tracing unprocessed trans MB51DS
def trace_unprocessed_mb51_ds(connection_db, df, df_2, date_param):
    if not df.empty:
        list_id_mb51_ds = [x for x in df['id']]
        list_id_mb51_ds_processed = [x for x in df_2['id']]

        #mapping main data
        info_mb51_ds_map = {}
        query_info_mb51_ds = f"""
            with cte as(
                select
                    id, plant, movementtype::int, qtyinunofentry::float, batch,
                    storagelocation, material::int, entry_date::date
                from trans_mb51_ds where id = any(%s)
            )
            select
                cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
                r.status_event1 status, null as issue_code
            from cte
            left join restan_mb51_ds_new r
            on cte.id = r.unique_id
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_mb51_ds, (list_id_mb51_ds,))
            info_mb51_ds = cur.fetchall()
            for record in info_mb51_ds:
                id, *details = record
                info_mb51_ds_map[id] = details

        columns_to_add = [
            'plant', 'movementtype', 'qtyinunofentry', 'batch',
            'storagelocation', 'material', 'entry_date',
            'qty_entry', 'qty_out', 'qty_restan', 'status',
            'issue_code'
            ]
        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_mb51_ds_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )

        for col in ['batch', 'storagelocation']:
            df[col] = df[col].fillna('-')
        df['tag'] = 'main'


        #mapping reference data
        query_reference_data_mb51_ds = f"""
            with cte as(
                select
                    'TRANS_MB51DS' task_name, 0 total_data_clean, id,
                    0 id_inc, plant, movementtype::int, qtyinunofentry::float,
                    batch, storagelocation, material::int, entry_date::date
                from trans_mb51_ds
                where
                    entry_date::date < '{date_param}'::date and
                    '{date_param}'::date - interval '5 days' < entry_date::date and
                    movementtype::int %% 2 = 1 and
                    id not in %s
            )
            select
                cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
                r.status_event1 status, null as issue_code
            from cte
            left join restan_mb51_ds_new r
            on cte.id = r.unique_id
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_reference_data_mb51_ds, (tuple(list_id_mb51_ds + list_id_mb51_ds_processed),))
            reference_data_mb51_ds = cur.fetchall()

        reference_mb51_ds_comp = pd.DataFrame(reference_data_mb51_ds,
            columns=[
                'task_name', 'total_data_clean', 'id', 'id_inc',
                'plant', 'movementtype', 'qtyinunofentry', 'batch',
                'storagelocation', 'material', 'entry_date',
                'qty_entry', 'qty_out', 'qty_restan', 'status',
                'issue_code'
            ])
        
        for col in ['batch', 'storagelocation']:
            reference_mb51_ds_comp[col] = reference_mb51_ds_comp[col].fillna('-')
        reference_mb51_ds_comp['tag'] = 'additional'


        #create hash for cols noise comp
        col_hash = ['plant', 'batch', 'storagelocation', 'material']
        for source in [df, reference_mb51_ds_comp]:
            source['hash_noise'] = source[col_hash].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)


        #check noise
        data_unprocessed_mb51_ds_even = df[
            (df['movementtype'].astype(str).str[-1].astype(int) % 2 == 0) &
            (df['movementtype'] != 344)
            ]

        data_unprocessed_mb51_ds_odd = df[
            (df['movementtype'].astype(str).str[-1].astype(int) % 2 == 1) &
            (df['movementtype'] != 343)
            ]
        data_unprocessed_mb51_ds_odd = pd.concat([data_unprocessed_mb51_ds_odd, reference_mb51_ds_comp], axis=0).reset_index(drop=True)

        data_unprocessed_mb51_ds_additional = df[df['movementtype'].isin([343,344])]

        df1_pairs = []
        df2_pairs = []

        i = 1
        print(f'>> Total Unprocessed MB51DS Even to check: {len(data_unprocessed_mb51_ds_even)}')
        for idx, row in data_unprocessed_mb51_ds_even.iterrows():
            print(f'>>>> Progress Check Noise MB51DS Even: {i}/{len(data_unprocessed_mb51_ds_even)}')
            match = data_unprocessed_mb51_ds_odd[
                (data_unprocessed_mb51_ds_odd['hash_noise'] == row['hash_noise']) &
                (data_unprocessed_mb51_ds_odd['movementtype'] == row['movementtype'] - 1) &
                (data_unprocessed_mb51_ds_odd['qtyinunofentry'] == -row['qtyinunofentry'])
            ]

            if not match.empty:
                df1_pairs.append(row)
                df2_pairs.append(match.sort_values(by='id', ascending=False).head(1).iloc[0])
                data_unprocessed_mb51_ds_odd = data_unprocessed_mb51_ds_odd.drop(match.sort_values(by='id', ascending=False).head(1).index)
            
            i = i+1


        df1_pair_df = pd.DataFrame(df1_pairs)
        df2_pair_df = pd.DataFrame(df2_pairs)
        dataframe_pair = pd.concat([df1_pair_df, df2_pair_df])
        if dataframe_pair.empty:
            dataframe_pair = pd.DataFrame(columns=data_unprocessed_mb51_ds_even.columns)
        
        dataframe_pair = dataframe_pair[dataframe_pair['tag']=='main']

        dataframe_single = pd.concat([
            data_unprocessed_mb51_ds_even[~data_unprocessed_mb51_ds_even.index.isin(df1_pair_df.index)],
            data_unprocessed_mb51_ds_odd[data_unprocessed_mb51_ds_odd['tag']=='main']
            ])
        dataframe_single_2 = data_unprocessed_mb51_ds_additional

        dataframe_pair['issue_code'] = dataframe_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51DS02'])
        dataframe_single_2['issue_code'] = dataframe_single_2['issue_code'].apply(lambda x: x if pd.notna(x) else ['L-MB51DS01'])

        data_unprocessed_mb51_ds_new = pd.concat([dataframe_pair, dataframe_single, dataframe_single_2], axis=0)


        #summary remark
        task = data_unprocessed_mb51_ds_new['task_name'].iloc[0]
        data_unprocessed_mb51_ds_new['issue_code'] = data_unprocessed_mb51_ds_new.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = data_unprocessed_mb51_ds_new.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_mb51_ds = data_unprocessed_mb51_ds_new[data_unprocessed_mb51_ds_new['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_mb51_ds = pd.DataFrame({
            'unprocessed_trans_to_event': data_unprocessed_mb51_ds_new.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': data_unprocessed_mb51_ds_new[data_unprocessed_mb51_ds_new['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(data_unprocessed_mb51_ds_new[data_unprocessed_mb51_ds_new['plant']!='-']['plant'].astype(str).unique())) if len(data_unprocessed_mb51_ds_new[data_unprocessed_mb51_ds_new['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_mb51_ds),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_mb51_ds['id'].unique())) if not disconnected_mb51_ds.empty else '-'
        }).reset_index()

        agg_result_mb51_ds['unprocessed_total_transaction'] = f"{agg_result_mb51_ds['unprocessed_trans_to_event'].values[0]} Row; {agg_result_mb51_ds['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_mb51_ds = agg_result_mb51_ds[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_mb51_ds
    
    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans MB51US
def trace_unprocessed_mb51_us(connection_db, df, df_2, date_param):
    if not df.empty:
        list_id_mb51_us = [x for x in df['id']]
        list_id_mb51_us_processed = [x for x in df_2['id']]

        #mapping main data
        info_mb51_us_map = {}
        query_info_mb51_us = f"""
            with cte as(
                select
                    id, name1, batch, plant, material, storagelocation,
                    valuationtype, purchaseorder, precedingdocument,
                    ordername, qtyinunofentry::float, movementtype,
                    postingdate, purchaseordernumber, reference, documentheadertext,
                    materialdocument, ordertype, text, entry_date::date
                from trans_mb51_us 
                where id = ANY(%s)
            )
            select
                cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
                r.status_event1 status, null as issue_code
            from cte
            left join restan_mb51_us_new r
            on cte.id = r.unique_id
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_mb51_us, (list_id_mb51_us,))
            info_mb51_us = cur.fetchall()
            for record in info_mb51_us:
                id, *details = record
                info_mb51_us_map[id] = details

        columns_to_add = [
            'name1', 'batch', 'plant', 'material', 'storagelocation',
            'valuationtype', 'purchaseorder', 'precedingdocument',
            'ordername', 'qtyinunofentry', 'movementtype', 'postingdate',
            'purchaseordernumber', 'reference', 'documentheadertext',
            'materialdocument', 'ordertype', 'text', 'entry_date',
            'qty_entry', 'qty_out', 'qty_restan', 'status', 'issue_code'
            ]
        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_mb51_us_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )

        for col in ['batch', 'storagelocation', 'valuationtype', 'purchaseorder', 'precedingdocument',
                    'ordername', 'purchaseordernumber', 'reference', 'documentheadertext']:
            df[col] = df[col].fillna('-')

        df['material'] = df['material'].astype(int)
        df['type'] = df['name1'].apply(
            lambda name:
                'estate' if 'estate' in name.lower() else
                'mill' if 'mill' in name.lower() else
                'kcp' if 'kcp' in name.lower() else
                'bulking' if 'bulking' in name.lower() else
                'others'
        )
        df['tag'] = 'main'


        #mapping reference data
        query_reference_data_mb51_us = f"""
            with cte as(
                select
                    'TRANS_MB51US' task_name, 0 total_data_clean, id,
                    0 id_inc, name1, batch, plant, material, storagelocation,
                    valuationtype, purchaseorder, precedingdocument,
                    ordername, qtyinunofentry::float, movementtype,
                    postingdate, purchaseordernumber, reference, documentheadertext,
                    materialdocument, ordertype, text, entry_date::date
                from trans_mb51_us 
                where
                    entry_date::date < '{date_param}'::date and
                    '{date_param}'::date - interval '5 days' < entry_date::date and
                    right(movementtype, 1)::int %% 2 = 1 and
                    id not in %s
            )
            select
                cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
                r.status_event1 status, null as issue_code
            from cte
            left join restan_mb51_us_new r
            on cte.id = r.unique_id
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_reference_data_mb51_us, (tuple(list_id_mb51_us + list_id_mb51_us_processed),))
            reference_data_mb51_us = cur.fetchall()

        reference_mb51_us_comp = pd.DataFrame(reference_data_mb51_us,
            columns=[
                'task_name', 'total_data_clean', 'id', 'id_inc',
                'name1', 'batch', 'plant', 'material', 'storagelocation',
                'valuationtype', 'purchaseorder', 'precedingdocument',
                'ordername', 'qtyinunofentry', 'movementtype', 'postingdate',
                'purchaseordernumber', 'reference', 'documentheadertext',
                'materialdocument', 'ordertype', 'text', 'entry_date',
                'qty_entry', 'qty_out', 'qty_restan', 'status', 'issue_code'
            ])

        for col in ['batch', 'storagelocation', 'valuationtype', 'purchaseorder', 'precedingdocument',
                    'ordername', 'purchaseordernumber', 'reference', 'documentheadertext']:
            reference_mb51_us_comp[col] = reference_mb51_us_comp[col].fillna('-')

        reference_mb51_us_comp['material'] = reference_mb51_us_comp['material'].astype(int)
        reference_mb51_us_comp['type'] = reference_mb51_us_comp['name1'].apply(
            lambda name:
                'estate' if 'estate' in name.lower() else
                'mill' if 'mill' in name.lower() else
                'kcp' if 'kcp' in name.lower() else
                'bulking' if 'bulking' in name.lower() else
                'others'
        )
        reference_mb51_us_comp['tag'] = 'additional'


        #create hash for cols noise comp
        col_hash_noise1 = ['plant', 'batch', 'storagelocation', 'material', 'valuationtype', 'purchaseorder', 'precedingdocument', 'ordername', 'postingdate']
        col_hash_noise2 = ['plant', 'batch', 'storagelocation', 'material', 'valuationtype', 'reference', 'ordername', 'postingdate']
        col_hash_noise3 = ['plant', 'batch', 'storagelocation', 'material', 'valuationtype', 'ordername']

        for source in [df, reference_mb51_us_comp]:
            source['hash_noise1'] = source[col_hash_noise1].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)
            source['hash_noise2'] = source[col_hash_noise2].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)
            source['hash_noise3'] = source[col_hash_noise3].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)
            source['movementtypeint'] = source['movementtype'].astype(str).str[-1].astype(int)


        #check noise 1
        data_unprocessed_mb51_us_even = df[
            (df['movementtype'].str[-1].astype(int) % 2 == 0) &
            (df['type'].isin(['mill', 'estate', 'bulking', 'kcp']))
            ]
        data_unprocessed_mb51_us_even_ = data_unprocessed_mb51_us_even[~(
            (data_unprocessed_mb51_us_even['type']=='kcp') &
            (data_unprocessed_mb51_us_even['movementtype']=='914')
            )]

        data_unprocessed_mb51_us_odd = df[
            (df['movementtype'].str[-1].astype(int) % 2 == 1) &
            (df['type'].isin(['mill', 'estate', 'bulking', 'kcp']))
            ]
        data_unprocessed_mb51_us_odd_ = data_unprocessed_mb51_us_odd[~(
            (data_unprocessed_mb51_us_odd['type']=='kcp') &
            (data_unprocessed_mb51_us_odd['movementtype']=='913')
            )]
        data_unprocessed_mb51_us_odd_ = pd.concat([data_unprocessed_mb51_us_odd_, reference_mb51_us_comp], axis=0).reset_index(drop=True)

        data_unprocessed_mb51_us_additional = df[
            (~df['type'].isin(['mill', 'estate', 'bulking', 'kcp'])) |
            ((df['type']=='kcp') & (df['movementtype'].isin(['913','914'])))
            ]


        df1_pairs = []
        df2_pairs = []

        i = 1
        print(f'>> Total Unprocessed MB51US Even Part 1 to check: {len(data_unprocessed_mb51_us_even_)}')
        for idx, row in data_unprocessed_mb51_us_even_.iterrows():
            print(f'>>>> Progress Check Noise MB51US Even Part 1: {i}/{len(data_unprocessed_mb51_us_even_)}')
            match = data_unprocessed_mb51_us_odd_[
                (data_unprocessed_mb51_us_odd_['hash_noise1'] == row['hash_noise1']) &
                (data_unprocessed_mb51_us_odd_['movementtypeint'] == row['movementtypeint'] - 1) &
                (data_unprocessed_mb51_us_odd_['qtyinunofentry'] == -row['qtyinunofentry'])
            ]

            if match.empty:
                match = data_unprocessed_mb51_us_odd_[
                    (data_unprocessed_mb51_us_odd_['hash_noise2'] == row['hash_noise2']) &
                    (data_unprocessed_mb51_us_odd_['movementtypeint'] == row['movementtypeint'] - 1) &
                    (data_unprocessed_mb51_us_odd_['qtyinunofentry'] == -row['qtyinunofentry'])
                ]

            if match.empty:
                match = data_unprocessed_mb51_us_odd_[
                    (data_unprocessed_mb51_us_odd_['hash_noise3'] == row['hash_noise3']) &
                    (data_unprocessed_mb51_us_odd_['movementtypeint'] == row['movementtypeint'] - 1) &
                    (data_unprocessed_mb51_us_odd_['qtyinunofentry'] == -row['qtyinunofentry'])
                ]


            filtered_match = match[
                (match['documentheadertext'] == row['documentheadertext']) &
                (match['purchaseordernumber'] == row['purchaseordernumber']) &
                (match['materialdocument'] <= row['materialdocument'])
            ]

            if filtered_match.empty:
                filtered_match = match[
                    (match['documentheadertext'] == row['documentheadertext']) &
                    (match['purchaseordernumber'] == row['purchaseordernumber']) &
                    (match['materialdocument'] >= row['materialdocument'])
                ]

            if filtered_match.empty:
                filtered_match = match.copy()

            if not filtered_match.empty:
                df1_pairs.append(row)
                df2_pairs.append(filtered_match.sort_values(by='id', ascending=False).head(1).iloc[0])
                data_unprocessed_mb51_us_odd_ = data_unprocessed_mb51_us_odd_.drop(filtered_match.sort_values(by='id', ascending=False).head(1).index)
            
            i = i+1


        df1_pair_df = pd.DataFrame(df1_pairs)
        df2_pair_df = pd.DataFrame(df2_pairs)
        dataframe_pair = pd.concat([df1_pair_df, df2_pair_df], axis=0)
        if dataframe_pair.empty:
            dataframe_pair = pd.DataFrame(columns=data_unprocessed_mb51_us_even_.columns)
        
        dataframe_pair = dataframe_pair[dataframe_pair['tag']=='main']

        dataframe_single = pd.concat([
            data_unprocessed_mb51_us_even_[~data_unprocessed_mb51_us_even_.index.isin(df1_pair_df.index)],
            data_unprocessed_mb51_us_odd_
            ])

        dataframe_pair['issue_code'] = dataframe_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51US02'])


        #check noise 2
        df_kcp_potential_noise_even = data_unprocessed_mb51_us_additional[
            (data_unprocessed_mb51_us_additional['type']=='kcp') &
            (data_unprocessed_mb51_us_additional['movementtype']=='914') &
            (
            (data_unprocessed_mb51_us_additional['text'].str.contains('CANCEL')) |
            (data_unprocessed_mb51_us_additional['documentheadertext'].str.contains('/')) |
            (data_unprocessed_mb51_us_additional['documentheadertext']=='-')
            )
        ]
        df_reference_kcp_noice_check = data_unprocessed_mb51_us_additional[~(
            (data_unprocessed_mb51_us_additional['type']=='kcp') &
            (data_unprocessed_mb51_us_additional['movementtype']=='914') &
            (
            (data_unprocessed_mb51_us_additional['text'].str.contains('CANCEL')) |
            (data_unprocessed_mb51_us_additional['documentheadertext'].str.contains('/')) |
            (data_unprocessed_mb51_us_additional['documentheadertext']=='-')
            )
        )]
        df_reference_kcp_noice_check = pd.concat([df_reference_kcp_noice_check, dataframe_single], axis=0).reset_index(drop=True)

        df1_additional_pairs = []
        df2_additional_pairs = []

        i = 1
        print(f'>> Total Unprocessed MB51US Even Part 2 to check: {len(df_kcp_potential_noise_even)}')
        for idx, row in df_kcp_potential_noise_even.iterrows():
            print(f'>>>> Progress Check Noise MB51US Even Part 2: {i}/{len(df_kcp_potential_noise_even)}')
            match = df_reference_kcp_noice_check[
                (df_reference_kcp_noice_check['documentheadertext'] == row['documentheadertext']) &
                # (df_reference_kcp_noice_check['reference'] == row['reference']) &
                (df_reference_kcp_noice_check['qtyinunofentry'] == -row['qtyinunofentry']) &
                (df_reference_kcp_noice_check['movementtype'].astype(str).str[-1].astype(int) == int(row['movementtype'][-1]) - 1) &
                (df_reference_kcp_noice_check['material'] == row['material']) &
                (df_reference_kcp_noice_check['postingdate'] == row['postingdate']) &
                (df_reference_kcp_noice_check['batch'] == row['batch']) &
                (df_reference_kcp_noice_check['plant'] == row['plant'])
                
            ]

            if not match.empty:
                df1_additional_pairs.append(row)
                df2_additional_pairs.append(match.sort_values(by='id', ascending=False).head(1).iloc[0])
                df_reference_kcp_noice_check = df_reference_kcp_noice_check.drop(match.sort_values(by='id', ascending=False).head(1).index)
            
            i = i+1


        df1_additional_pairs_df = pd.DataFrame(df1_additional_pairs)
        df2_additional_pairs_df = pd.DataFrame(df2_additional_pairs)
        dataframe_additional_pair = pd.concat([df1_additional_pairs_df, df2_additional_pairs_df])
        if dataframe_additional_pair.empty:
            dataframe_additional_pair = pd.DataFrame(columns=df_kcp_potential_noise_even.columns)

        dataframe_additional_single = pd.concat([
            df_kcp_potential_noise_even[~df_kcp_potential_noise_even.index.isin(df1_additional_pairs_df.index)],
            df_reference_kcp_noice_check[df_reference_kcp_noice_check['tag']=='main']
            ])

        dataframe_additional_pair['issue_code'] = dataframe_additional_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51US02'])

        data_unprocessed_mb51_us_ = pd.concat([dataframe_pair, dataframe_additional_pair, dataframe_additional_single], axis=0)


        #check mill at zmb us
        list_mill_mb51_us = data_unprocessed_mb51_us_[data_unprocessed_mb51_us_['type'] == 'mill']['plant'].unique().tolist()
        if len(list_mill_mb51_us) > 0:
            list_mill_mb51_us = str(list_mill_mb51_us)
            query_check_mill_zmb_us = f"""
                with cte as(
                    select unnest(array{list_mill_mb51_us}) plant
                )
                select
                    cte.plant,
                    case 
                        when exists (select 0 from trans_zmb_us t where t.plant = cte.plant) then 'available' 
                        else 'not available' 
                    end as mill_zmb_us_status
                from cte
                ;
            """
            result_mill_zmb_us = pd.read_sql(query_check_mill_zmb_us, connection_db)
        else:
            result_mill_zmb_us = pd.DataFrame(columns=['plant', 'mill_zmb_us_status'])

        data_unprocessed_mb51_us_ = data_unprocessed_mb51_us_.merge(result_mill_zmb_us, how='left', on='plant')


        #summary remark
        task = data_unprocessed_mb51_us_['task_name'].iloc[0]
        data_unprocessed_mb51_us_['issue_code'] = data_unprocessed_mb51_us_.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = data_unprocessed_mb51_us_.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_mb51_us = data_unprocessed_mb51_us_[data_unprocessed_mb51_us_['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_mb51_us = pd.DataFrame({
            'unprocessed_trans_to_event': data_unprocessed_mb51_us_.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': data_unprocessed_mb51_us_[data_unprocessed_mb51_us_['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(data_unprocessed_mb51_us_[data_unprocessed_mb51_us_['plant']!='-']['plant'].astype(str).unique())) if len(data_unprocessed_mb51_us_[data_unprocessed_mb51_us_['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_mb51_us),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_mb51_us['id'].unique())) if not disconnected_mb51_us.empty else '-'
        }).reset_index()

        agg_result_mb51_us['unprocessed_total_transaction'] = f"{agg_result_mb51_us['unprocessed_trans_to_event'].values[0]} Row; {agg_result_mb51_us['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_mb51_us = agg_result_mb51_us[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_mb51_us

    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans Openlink
def trace_unprocessed_openlink(connection_db, df, date_param):
    if not df.empty:
        list_id_openlink = [x for x in df['id']]

        info_openlink_map = {}
        query_info_openlink = f"""
            select
                id, backstop_id::bigint, contract_type,
                sap_so_po_number::bigint, delivery_status,
                status, bl_date::date
            from trans_openlink
            where id = any(%s)
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_openlink, (list_id_openlink,))
            info_openlink = cur.fetchall()
            for record in info_openlink:
                id, *details = record
                info_openlink_map[id] = details
        
        columns_to_add = [
            'backstop_id', 'contract_type', 'sap_so_po_number',
            'delivery_status', 'status', 'bl_date'
            ]

        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_openlink_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )


        #cek related data DSI Interco Sales
        sap_so_po_numbers = df[df['contract_type']=='DSI Interco Sales']['sap_so_po_number'].unique()
        sap_so_po_numbers = [str(so_po) for so_po in sap_so_po_numbers if so_po != 0 and so_po is not None]
        if len(sap_so_po_numbers)>0:
            list_sap_so_po_numbers = str(sap_so_po_numbers)
            query_check_sap_so_po_numbers = f"""
                with cte as(
                    select unnest(array{list_sap_so_po_numbers}) sap_so_po_number
                ),
                cte2 as(
                    select
                        cte.sap_so_po_number::bigint,
                        case
                            when exists (
                                select 0
                                from trans_mb51_ds t
                                where t.precedingdocument = cte.sap_so_po_number::varchar
                                )
                            then 1
                            else 0
                        end as is_mb51ds_available,
                        case 
                            when exists (
                                select 0
                                from trans_sodogi t
                                where t.sales_order_no = cte.sap_so_po_number::bigint
                                )
                            then 1
                            else 0
                        end as is_sodogi_available,
                        case 
                            when exists (
                                select 0
                                from trans_sodogi t
                                where
                                    t.sales_order_no = cte.sap_so_po_number::bigint and
                                    t.status = 'valid'
                                )
                            then 1
                            else 0
                        end as is_valid_sodogi_available,
                        (
                        select array_agg(id)
                        from trans_sodogi t
                        where
                            t.sales_order_no = cte.sap_so_po_number::bigint and
                            t.status = 'valid'
                        ) list_valid_id_sodogi,
                        (
                        select array_agg(id)
                        from trans_sodogi t
                        where
                            t.sales_order_no = cte.sap_so_po_number::bigint and
                            t.status = 'invalid'
                        ) list_invalid_id_sodogi
                    from cte
                )
                select
                    cte2.sap_so_po_number, cte2.is_mb51ds_available, cte2.is_sodogi_available, cte2.is_valid_sodogi_available,
                    case
                        when exists (
                            select 0
                            from events_bc eb
                            where eb.unique_id = any(cte2.list_valid_id_sodogi)
                            )
                        then 1
                        else 0
                    end as is_valid_sodogi_in_events,
                    case
                        when exists (
                            select 0
                            from events_bc eb
                            where eb.unique_id = any(cte2.list_invalid_id_sodogi)
                            )
                        then 1
                        else 0
                    end as is_invalid_sodogi_in_events,
                    case
                        when exists (
                            select 0
                            from events_bc eb
                            where
                                eb.sales_order = cte2.sap_so_po_number::varchar and
                                eb.production_label = 'Shipment DSI'
                            )
                        then 1
                        else 0
                    end as is_sap_so_po_number_dsi_already_in_events
                from cte2
                ;
            """
            result_sap_so_po_numbers = pd.read_sql(query_check_sap_so_po_numbers, connection_db)
        else:
            result_sap_so_po_numbers = pd.DataFrame(columns=[
                'sap_so_po_number', 'is_mb51ds_available', 'is_sodogi_available', 'is_valid_sodogi_available',
                'is_valid_sodogi_in_events', 'is_invalid_sodogi_in_events', 'is_sap_so_po_number_dsi_already_in_events'
            ])


        #cek related data 3rd Party Sales
        tps_backstop_ids = df[df['contract_type']=='3rd Party Sales']['backstop_id'].unique()
        tps_backstop_ids = [str(backstop) for backstop in tps_backstop_ids if backstop != 0]
        if len(tps_backstop_ids) > 0:
            list_backstop_ids = str(tps_backstop_ids)
            query_check_backstop_ids = f"""
                with cte as(
                    select unnest(array{list_backstop_ids}) backstop_id
                ),
                cte2 as(
                    select
                        cte.backstop_id::bigint,
                        '3rd Party Sales'::varchar contract_type,
                        case 
                            when exists (
                                select 0
                                from trans_openlink t
                                where
                                    t.contract_type = 'DSI Interco Sales' and
                                    t.backstop_id = cte.backstop_id::bigint
                                )
                            then 1
                            else 0
                        end as is_dsi_available,
                        case 
                            when exists (
                                select 0
                                from trans_openlink t
                                where
                                    t.contract_type = 'DSI Interco Sales' and
                                    t.backstop_id = cte.backstop_id::bigint and
                                    t.status = 'valid'
                                )
                            then 1
                            else 0
                        end as is_valid_dsi_available,
                        (
                        select array_agg(t.id)
                        from trans_openlink t
                        where
                            t.contract_type = 'DSI Interco Sales' and
                            t.backstop_id = cte.backstop_id::bigint and
                            t.sap_so_po_number <> 0 and
                            t.status = 'valid'
                        ) list_valid_id_dsi,
                        (
                        select array_agg(t.id)
                        from trans_openlink t
                        where
                            t.contract_type = 'DSI Interco Sales' and
                            t.backstop_id = cte.backstop_id::bigint and
                            t.sap_so_po_number <> 0 and
                            t.status = 'invalid'
                        ) list_invalid_id_dsi,
                        (
                        select array_agg(distinct t.sap_so_po_number::bigint)
                        from trans_openlink t
                        where
                            t.contract_type = 'DSI Interco Sales' and
                            t.backstop_id = cte.backstop_id::bigint and
                            t.sap_so_po_number <> 0 and
                            t.status = 'valid'
                        ) list_valid_sap_so_po_number_dsi
                    from cte
                ),
                cte3 as(
                    select
                        cte2.backstop_id, cte2.contract_type, cte2.is_dsi_available, cte2.is_valid_dsi_available,
                        case
                            when exists (
                                select 0
                                from events_bc eb
                                where eb.unique_id = any(cte2.list_valid_id_dsi)
                                )
                            then 1
                            else 0
                        end as is_valid_id_dsi_in_events,
                        case
                            when exists (
                                select 0
                                from events_bc eb
                                where eb.unique_id = any(cte2.list_invalid_id_dsi)
                                )
                            then 1
                            else 0
                        end as is_invalid_id_dsi_in_events,
                        case
                            when exists (
                                select 0
                                from trans_sodogi ts
                                where
                                    ts.sales_order_no = any(cte2.list_valid_sap_so_po_number_dsi) and
                                    ts.status = 'valid'
                                )
                            then 1
                            else 0
                        end as is_valid_sodogi_available_2,
                        (
                        select array_agg(ts.id)
                        from trans_sodogi ts
                        where
                            ts.sales_order_no = any(cte2.list_valid_sap_so_po_number_dsi) and
                            ts.status = 'valid'
                        ) list_valid_id_sodogi,
                        (
                        select array_agg(ts.id)
                        from trans_sodogi ts
                        where
                            ts.sales_order_no = any(cte2.list_valid_sap_so_po_number_dsi) and
                            ts.status = 'invalid'
                        ) list_invalid_id_sodogi
                    from cte2
                )
                select
                    backstop_id, contract_type, is_dsi_available, is_valid_dsi_available,
                    is_valid_id_dsi_in_events, is_invalid_id_dsi_in_events, is_valid_sodogi_available_2,
                    case
                        when exists (
                            select 0
                            from events_bc eb
                            where eb.unique_id = any(cte3.list_valid_id_sodogi)
                            )
                        then 1
                        else 0
                    end as is_valid_sodogi_in_events_2,
                    case
                        when exists (
                            select 0
                            from events_bc eb
                            where eb.unique_id = any(cte3.list_invalid_id_sodogi)
                            )
                        then 1
                        else 0
                    end as is_invalid_sodogi_in_events_2,
                    case
                        when exists (
                            select 0
                            from events_bc eb
                            where
                                eb.backstop_id = cte3.backstop_id::varchar and
                                eb.production_label = 'Shipment Contract'::varchar
                            )
                        then 1
                        else 0
                    end as is_backstop_3ps_already_in_events
                from cte3
                ;
            """
            result_backstop_ids = pd.read_sql(query_check_backstop_ids, connection_db)
        else:
            result_backstop_ids = pd.DataFrame(columns=[
                'backstop_id', 'contract_type', 'is_dsi_available', 'is_valid_dsi_available',
                'is_valid_id_dsi_in_events', 'is_invalid_id_dsi_in_events', 'is_valid_sodogi_available_2',
                'is_valid_sodogi_in_events_2', 'is_invalid_sodogi_in_events_2', 'is_backstop_3ps_already_in_events'
            ])

        df = df.merge(result_sap_so_po_numbers, how='left', on='sap_so_po_number')
        df = df.merge(result_backstop_ids, how='left', on=['backstop_id', 'contract_type'])


        #summary remark
        task = df['task_name'].iloc[0]
        df['issue_code'] = df.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = df.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_openlink = df[df['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_openlink = pd.DataFrame({
            'unprocessed_trans_to_event': df.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': df[df['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(df[df['plant']!='-']['plant'].unique())) if len(df[df['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_openlink),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_openlink['id'].unique())) if not disconnected_openlink.empty else '-'
        }).reset_index()

        agg_result_openlink['unprocessed_total_transaction'] = f"{agg_result_openlink['unprocessed_trans_to_event'].values[0]} Row; {agg_result_openlink['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_openlink = agg_result_openlink[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_openlink

    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans SODOGI
def trace_unprocessed_sodogi(connection_db, df, date_param):
    if not df.empty:
        list_id_sodogi = [x for x in df['id']]

        info_sodogi_map = {}
        query_info_sodogi = f"""
            select
                id, actual_gi_date::date, sales_order_no::bigint,
                do_qty::bigint, do_number::bigint, batch_no, status
            from trans_sodogi
            where id = any(%s)
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_sodogi, (list_id_sodogi,))
            info_sodogi = cur.fetchall()
            for record in info_sodogi:
                id, *details = record
                info_sodogi_map[id] = details
        
        columns_to_add = [
            'actual_gi_date', 'sales_order_no',
            'do_qty', 'do_number', 'batch_no', 'status'
            ]

        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_sodogi_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )


        #check so in mb51ds
        sales_order_nos = df['sales_order_no'].unique()
        sales_order_nos = [str(so) for so in sales_order_nos if so != 0]
        if len(sales_order_nos) > 0:
            list_sales_order_nos = str(sales_order_nos)
            query_check_mb51_ds = f"""
                with cte as(
                    select unnest(array{list_sales_order_nos}) sales_order_no
                ),
                cte2 as(
                    select
                        cte.sales_order_no,
                        t.id as mb51ds_id,
                        t.movementtype as mb51ds_mvt
                    from cte
                    left join trans_mb51_ds t
                    on cte.sales_order_no::varchar = t.precedingdocument
                ),
                cte3 as(
                    select
                        cte2.sales_order_no,
                        cte2.mb51ds_id,
                        cte2.mb51ds_mvt,
                        case 
                            when cte2.mb51ds_mvt = 601 and exists (
                                select 0 from trans_mb51_ds t2
                                where
                                    t2.movementtype = 101 and
                                    id in (
                                        select lb.event2_id from links_bc lb
                                        where lb.event1_id = cte2.mb51ds_id
                                    )
                            ) then 1
                            else 0
                        end as mb51ds601_connected_to_mb51ds101
                    from cte2
                )
                select
                    cte3.sales_order_no::bigint,
                    count(cte3.mb51ds_id) as cnt_mb51ds_id,
                    max(cte3.mb51ds601_connected_to_mb51ds101) as mb51ds601_connected_to_mb51ds101
                from cte3
                group by cte3.sales_order_no
                ;
            """
            result_mb51_ds = pd.read_sql(query_check_mb51_ds, connection_db)
        else:
            result_mb51_ds = pd.DataFrame(columns=['sales_order_no', 'cnt_mb51ds_id', 'mb51ds601_connected_to_mb51ds101'])


        #check so and batch in events
        so_batch_pairs = df[
            (~df['sales_order_no'].isna()) &
            (~df['batch_no'].isna())
        ][['sales_order_no', 'batch_no']].drop_duplicates()

        so_to_events = so_batch_pairs['sales_order_no'].to_list()
        batch_to_events = so_batch_pairs['batch_no'].to_list()

        if not so_batch_pairs.empty:
            list_so_to_events = str(so_to_events)
            list_batch_to_events = str(batch_to_events)

            query_check_batch_so_events = f"""
                with cte as(
                    select
                        unnest(array{list_so_to_events}) sales_order_no,
                        unnest(array{list_batch_to_events}) batch_no
                ),
                cte2 as(
                    select distinct
                        cte.sales_order_no::bigint,
                        cte.batch_no,
                        case 
                            when eb.sales_order is null and eb.product_id is null then 'not available'
                            else 'available'
                        end as so_and_batch_in_events_status
                    from cte
                    left join events_bc eb
                    on
                        cte.sales_order_no::varchar = eb.sales_order and
                        cte.batch_no = eb.product_id and
                        (eb.production_label = 'Sodogi' or eb.unique_id like 'TRANS_SODOGIDS%')
                )
                select
                    cte2.sales_order_no::bigint,
                    cte2.batch_no,
                    cte2.so_and_batch_in_events_status,
                    case 
                        when eb2.sales_order is null and eb2.product_id is null then 'not available'
                        else 'available'
                    end as so_in_events_status
                from cte2
                left join events_bc eb2
                on
                    cte2.sales_order_no::varchar = eb2.sales_order and
                    (eb2.production_label = 'Sodogi' or eb2.unique_id like 'TRANS_SODOGIDS%')
                ;
            """
            result_check_batch_so_events = pd.read_sql(query_check_batch_so_events, connection_db).drop_duplicates()
        else:
            result_check_batch_so_events = pd.DataFrame(columns=['sales_order_no', 'batch_no', 'so_and_batch_in_events_status', 'so_in_events_status'])

        df = df.merge(result_mb51_ds, how='left', on='sales_order_no')
        df = df.merge(result_check_batch_so_events, how='left', on=['sales_order_no', 'batch_no'])


        #connect plant MB51DS
        list_so_unprocessed_sodogi = [str(x) for x in df['sales_order_no'].unique() if x!=None]
        if len(list_so_unprocessed_sodogi)>0:
            info_plant_sodogi_map = {}
            query_info_plant_sodogi = f"""
                select
                    precedingdocument::bigint sales_order_no,
                    string_agg(distinct plant, '; ')
                from trans_mb51_ds
                where precedingdocument = any(%s)
                group by precedingdocument
            """
            with connection_db.cursor() as cur:
                cur.execute(query_info_plant_sodogi, (list_so_unprocessed_sodogi,))
                info_plant_sodogi = cur.fetchall()
                for precedingdocument, plant in info_plant_sodogi:
                    info_plant_sodogi_map[precedingdocument] = plant

            df['plant'] = df.apply(lambda row: info_plant_sodogi_map.get(row['sales_order_no'], '-'), axis=1)
        else:
            pass

        #summary remark
        task = df['task_name'].iloc[0]
        df['issue_code'] = df.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = df.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_sodogi = df[df['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_sodogi = pd.DataFrame({
            'unprocessed_trans_to_event': df.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': df[df['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(df[df['plant']!='-']['plant'].unique())) if len(df[df['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_sodogi),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_sodogi['id'].unique())) if not disconnected_sodogi.empty else '-'
        }).reset_index()

        agg_result_sodogi['unprocessed_total_transaction'] = f"{agg_result_sodogi['unprocessed_trans_to_event'].values[0]} Row; {agg_result_sodogi['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_sodogi = agg_result_sodogi[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_sodogi
    
    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans ZMB US
def trace_unprocessed_zmb_us(connection_db, df, date_param):
    if not df.empty:
        list_id_zmb_us = [x for x in df['id']]

        info_zmb_us_map = {}
        query_info_zmb_us = f"""
            with cte as(
                select
                    id, date::date, plant, process_information,
                    total::float, input_date::date
                from trans_zmb_us
                where id = any(%s)
            )
            select
                cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
                r.status_event1 status, null as issue_code
            from cte
            left join restan_zmb_us_new r
            on cte.id = r.unique_id
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_zmb_us, (list_id_zmb_us,))
            info_zmb_us = cur.fetchall()
            for record in info_zmb_us:
                id, *details = record
                info_zmb_us_map[id] = details
        
        columns_to_add = [
            'date', 'plant', 'process_information', 'total',
            'input_date', 'qty_entry', 'qty_out', 'qty_restan',
            'status', 'issue_code'
            ]

        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_zmb_us_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )
        df['range_date'] = df['input_date'] - df['date']

        #summary remark
        task = df['task_name'].iloc[0]
        df['issue_code'] = df.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = df.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_zmb_us = df[df['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_zmb_us = pd.DataFrame({
            'unprocessed_trans_to_event': df.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': df[df['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(df[df['plant']!='-']['plant'].unique())) if len(df[df['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_zmb_us),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_zmb_us['id'].unique())) if not disconnected_zmb_us.empty else '-'
        }).reset_index()

        agg_result_zmb_us['unprocessed_total_transaction'] = f"{agg_result_zmb_us['unprocessed_trans_to_event'].values[0]} Row; {agg_result_zmb_us['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_zmb_us = agg_result_zmb_us[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_zmb_us

    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans MB51 ESM
def trace_unprocessed_mb51_esm(connection_db, df, df_2, date_param):
    if not df.empty:
        list_id_mb51_esm = [x for x in df['id']]
        list_id_mb51_esm_processed = [x for x in df_2['id']]

        #mapping main data
        info_mb51_esm_map = {}
        query_info_mb51_esm = f"""
            with cte as(
                select
                    id, plant, movement_type::int, quantity::float, batch,
                    sloc::int, material::int, entry_date::date
                from trans_mb51_esm where id = any(%s)
            )
            select
                cte.*, r.qty_entry, r.qty_out, r.qty_restan,
                r.status, null as issue_code
            from cte
            left join restan_mb51_esm r
            on cte.id = r.unique_id
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_mb51_esm, (list_id_mb51_esm,))
            info_mb51_esm = cur.fetchall()
            for record in info_mb51_esm:
                id, *details = record
                info_mb51_esm_map[id] = details

        columns_to_add = [
            'plant', 'movement_type', 'quantity', 'batch',
            'sloc', 'material', 'entry_date', 'qty_entry',
            'qty_out', 'qty_restan', 'status', 'issue_code'
            ]
        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_mb51_esm_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )

        df['batch'] = df['batch'].fillna('-')
        df['tag'] = 'main'


        #mapping reference data
        query_reference_data_mb51_esm = f"""
            with cte as(
                select
                    'TRANS_MB51_ESM' task_name, 0 total_data_clean,
                    id, 0 id_inc, plant, movement_type::int, quantity::float,
                    batch, sloc::int, material::int, entry_date::date
                from trans_mb51_esm
                where
                    entry_date::date < '{date_param}'::date and
                    '{date_param}'::date - interval '10 days' < entry_date::date and
                    movement_type::int %% 2 = 1 and
                    id not in %s
            )
            select
                cte.*, r.qty_entry, r.qty_out, r.qty_restan,
                r.status, null as issue_code
            from cte
            left join restan_mb51_esm r
            on cte.id = r.unique_id
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_reference_data_mb51_esm, (tuple(list_id_mb51_esm + list_id_mb51_esm_processed),))
            reference_data_mb51_esm = cur.fetchall()

        reference_mb51_esm_comp = pd.DataFrame(reference_data_mb51_esm,
            columns=[
                'task_name', 'total_data_clean', 'id', 'id_inc',
                'plant', 'movement_type', 'quantity', 'batch',
                'sloc', 'material', 'entry_date', 'qty_entry',
                'qty_out', 'qty_restan', 'status', 'issue_code'
            ])
        
        reference_mb51_esm_comp['batch'] = reference_mb51_esm_comp['batch'].fillna('-')
        reference_mb51_esm_comp['tag'] = 'additional'


        #create hash for cols noise comp
        col_hash = ['plant', 'batch', 'sloc', 'material']
        for source in [df, reference_mb51_esm_comp]:
            source['hash_noise'] = source[col_hash].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)


        #check noise
        data_unprocessed_mb51_esm_even = df[
            (df['movement_type'].astype(str).str[-1].astype(int) % 2 == 0) &
            (df['movement_type'] != 344)
            ]

        data_unprocessed_mb51_esm_odd = df[
            (df['movement_type'].astype(str).str[-1].astype(int) % 2 == 1) &
            (df['movement_type'] != 343)
            ]
        data_unprocessed_mb51_esm_odd = pd.concat([data_unprocessed_mb51_esm_odd, reference_mb51_esm_comp], axis=0).reset_index(drop=True)

        data_unprocessed_mb51_esm_additional = df[df['movement_type'].isin([343,344])]

        df1_pairs = []
        df2_pairs = []

        i = 1
        print(f'>> Total Unprocessed MB51 ESM Even to check: {len(data_unprocessed_mb51_esm_even)}')
        for idx, row in data_unprocessed_mb51_esm_even.iterrows():
            print(f'>>>> Progress Check Noise MB51DS Even: {i}/{len(data_unprocessed_mb51_esm_even)}')
            match = data_unprocessed_mb51_esm_odd[
                (data_unprocessed_mb51_esm_odd['hash_noise'] == row['hash_noise']) &
                (data_unprocessed_mb51_esm_odd['movement_type'] == row['movement_type'] - 1) &
                (data_unprocessed_mb51_esm_odd['quantity'] == -row['quantity'])
            ]

            if not match.empty:
                df1_pairs.append(row)
                df2_pairs.append(match.sort_values(by='id', ascending=False).head(1).iloc[0])
                data_unprocessed_mb51_esm_odd = data_unprocessed_mb51_esm_odd.drop(match.sort_values(by='id', ascending=False).head(1).index)
            
            i = i+1


        df1_pair_df = pd.DataFrame(df1_pairs)
        df2_pair_df = pd.DataFrame(df2_pairs)
        dataframe_pair = pd.concat([df1_pair_df, df2_pair_df])
        if dataframe_pair.empty:
            dataframe_pair = pd.DataFrame(columns=data_unprocessed_mb51_esm_even.columns)
        dataframe_pair = dataframe_pair[dataframe_pair['tag']=='main']

        dataframe_single = pd.concat([
            data_unprocessed_mb51_esm_even[~data_unprocessed_mb51_esm_even.index.isin(df1_pair_df.index)],
            data_unprocessed_mb51_esm_odd[data_unprocessed_mb51_esm_odd['tag']=='main']
            ])
        dataframe_single_2 = data_unprocessed_mb51_esm_additional

        dataframe_pair['issue_code'] = dataframe_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51_ESM01'])
        dataframe_single_2['issue_code'] = dataframe_single_2['issue_code'].apply(lambda x: x if pd.notna(x) else ['L-MB51_ESM01'])

        data_unprocessed_mb51_esm_new = pd.concat([dataframe_pair, dataframe_single, dataframe_single_2], axis=0)


        #summary remark
        task = data_unprocessed_mb51_esm_new['task_name'].iloc[0]
        data_unprocessed_mb51_esm_new['issue_code'] = data_unprocessed_mb51_esm_new.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = data_unprocessed_mb51_esm_new.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_mb51_esm = data_unprocessed_mb51_esm_new[data_unprocessed_mb51_esm_new['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_mb51_esm = pd.DataFrame({
            'unprocessed_trans_to_event': data_unprocessed_mb51_esm_new.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': data_unprocessed_mb51_esm_new[data_unprocessed_mb51_esm_new['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(data_unprocessed_mb51_esm_new[data_unprocessed_mb51_esm_new['plant']!='-']['plant'].astype(str).unique())) if len(data_unprocessed_mb51_esm_new[data_unprocessed_mb51_esm_new['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_mb51_esm),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_mb51_esm['id'].unique())) if not disconnected_mb51_esm.empty else '-'
        }).reset_index()

        agg_result_mb51_esm['unprocessed_total_transaction'] = f"{agg_result_mb51_esm['unprocessed_trans_to_event'].values[0]} Row; {agg_result_mb51_esm['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_mb51_esm = agg_result_mb51_esm[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_mb51_esm
    
    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans Openlink ESM
def trace_unprocessed_openlink_esm(connection_db, df, date_param):
    if not df.empty:
        list_id_openlink_esm = [x for x in df['id']]

        info_openlink_esm_map = {}
        query_info_openlink_esm = f"""
            select
                id, contract_type, sap_so_po_number::bigint,
                bl_date::date, bl_number
            from trans_openlink_esm
            where id = any(%s)
            ;
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_openlink_esm, (list_id_openlink_esm,))
            info_openlink_esm = cur.fetchall()
            for record in info_openlink_esm:
                id, *details = record
                info_openlink_esm_map[id] = details
        
        columns_to_add = [
            'contract_type', 'sap_so_po_number', 'bl_date', 'bl_number'
            ]

        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_openlink_esm_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )


        #check so in sodogi
        sap_so_po_numbers = df['sap_so_po_number'].unique()
        sap_so_po_numbers = [str(so_po) for so_po in sap_so_po_numbers if so_po != 0 and so_po is not None]
        if len(sap_so_po_numbers)>0:
            list_sap_so_po_numbers = str(sap_so_po_numbers)
            query_check_sap_so_po_numbers = f"""
                with cte as(
                    select unnest(array{list_sap_so_po_numbers}) sap_so_po_number
                ),
                cte2 as(
                    select
                        cte.sap_so_po_number::bigint,
                        case
                            when exists (select 0 from trans_sodogi_esm t where t.sales_order_no = cte.sap_so_po_number::bigint) then 'available'
                            else 'not available' 
                        end as so_po_in_sodogi_status,
                        (
                        select array_agg(distinct t.do_number::varchar)
                        from trans_sodogi_esm t
                        where t.sales_order_no = cte.sap_so_po_number::bigint
                        ) do_numbers
                    from cte
                )
                select
                    cte2.*,
                    case
                        when exists (select 0 from trans_mb51_esm t where t.reference = any(cte2.do_numbers)) then 'available'
                        else 'not available'
                    end as do_number_sodogi_in_mb51_status
                from cte2
                ;
            """
            result_sap_so_po_numbers = pd.read_sql(query_check_sap_so_po_numbers, connection_db)
        else:
            result_sap_so_po_numbers = pd.DataFrame(columns=['sap_so_po_number', 'so_po_in_sodogi_status', 'do_numbers', 'do_number_sodogi_in_mb51_status'])

        df = df.merge(result_sap_so_po_numbers, how='left', on='sap_so_po_number')


        #summary remark
        task = df['task_name'].iloc[0]
        df['issue_code'] = df.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = df.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_openlink_esm = df[df['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_openlink_esm = pd.DataFrame({
            'unprocessed_trans_to_event': df.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': 0,
            'list_unprocessed_plant_trans_to_event': '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_openlink_esm), 
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_openlink_esm['id'].unique())) if not disconnected_openlink_esm.empty else '-'
        }).reset_index()

        agg_result_openlink_esm['unprocessed_total_transaction'] = f"{agg_result_openlink_esm['unprocessed_trans_to_event'].values[0]} Row; {agg_result_openlink_esm['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_openlink_esm = agg_result_openlink_esm[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_openlink_esm
    
    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#tracing unprocessed trans SODOGI ESM
def trace_unprocessed_sodogi_esm(connection_db, df, date_param):
    if not df.empty:
        list_id_sodogi_esm = [x for x in df['id']]

        info_sodogi_esm_map = {}
        query_info_sodogi_esm = f"""
            select
                id, sales_order_no::bigint, do_number::bigint,
                issued_qty, good_issued_date::date
            from trans_sodogi_esm
            where id = any(%s)
        """
        with connection_db.cursor() as cur:
            cur.execute(query_info_sodogi_esm, (list_id_sodogi_esm,))
            info_sodogi_esm = cur.fetchall()
            for record in info_sodogi_esm:
                id, *details = record
                info_sodogi_esm_map[id] = details
        
        columns_to_add = [
            'sales_order_no', 'do_number', 'issued_qty', 'good_issued_date'
            ]

        df[columns_to_add] = pd.DataFrame(
            df.apply(lambda row: info_sodogi_esm_map.get(row['id'], [None] * len(columns_to_add)), axis=1).tolist(),
            index=df.index
        )


        #check do in mb51 esm
        do_numbers = df['do_number'].unique()
        do_numbers = [str(do) for do in do_numbers if do != 0 and do is not None]
        if len (do_numbers)>0:
            list_do_numbers = str(do_numbers)
            query_check_do_numbers = f"""
                with cte as(
                    select unnest(array{list_do_numbers}) do_number
                )
                select
                    cte.do_number::bigint,
                    case
                        when exists (select 0 from trans_mb51_esm t where t.reference = cte.do_number) then 'available'
                        else 'not available'
                    end as do_number_sodogi_in_mb51_status
                from cte
                ;
            """
            result_check_do_numbers = pd.read_sql(query_check_do_numbers, connection_db)
        else:
            result_check_do_numbers = pd.DataFrame(columns=['do_number', 'do_number_sodogi_in_mb51_status'])

        df = df.merge(result_check_do_numbers, how='left', on='do_number')


        #summary remark
        task = df['task_name'].iloc[0]
        df['issue_code'] = df.apply(lambda x: notes_trace(task, x, date_param), axis=1)
        exploded_data = df.explode('issue_code')
        row_counts = exploded_data.groupby('issue_code')['issue_code'].count()
        unique_plant_counts = exploded_data[exploded_data['plant'] != '-'].groupby('issue_code')['plant'].nunique()
        summary = '\n'.join([f">> {note} ({row_counts[note]} Row, {unique_plant_counts.get(note, 0)} Plant)" 
                            for note in row_counts.index])

        disconnected_sodogi_esm = df[df['issue_code'].apply(lambda x: 'H-UNKNOWN' in x)]

        agg_result_sodogi_esm = pd.DataFrame({
            'unprocessed_trans_to_event': df.groupby('task_name')['id'].count(),
            'unprocessed_plant_trans_to_event': df[df['plant']!='-']['plant'].nunique(),
            'list_unprocessed_plant_trans_to_event': '; '.join(sorted(df[df['plant']!='-']['plant'].unique())) if len(df[df['plant']!='-']['plant']) > 0 else '-',
            'remark_unprocessed_trans_to_event': summary,
            'total_disconnected_trans_id': len(disconnected_sodogi_esm),
            'list_disconnected_trans_id': '; '.join(sorted(disconnected_sodogi_esm['id'].unique())) if not disconnected_sodogi_esm.empty else '-'
        }).reset_index()

        agg_result_sodogi_esm['unprocessed_total_transaction'] = f"{agg_result_sodogi_esm['unprocessed_trans_to_event'].values[0]} Row; {agg_result_sodogi_esm['unprocessed_plant_trans_to_event'].values[0]} Plant"
        agg_result_sodogi_esm = agg_result_sodogi_esm[[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ]]

        return agg_result_sodogi_esm
    
    else:
        return pd.DataFrame(columns=[
            'task_name', 'unprocessed_total_transaction', 'list_unprocessed_plant_trans_to_event',
            'remark_unprocessed_trans_to_event', 'total_disconnected_trans_id', 'list_disconnected_trans_id'
        ])

#daily monitoring partial
def daily_monitoring_partial(connection_db, list_task, list_id):
    query_monitoring_detail = f"""
        with cte as(
            select
                unnest(array{list_task}) task_name,
                unnest(array{list_id}) id
        ),
        cte2 as(
            select
                cte.task_name,
                cte.id,
                case
                    when exists (select 0 from events_bc eb where eb.unique_id::text = cte.id::text) then 1
                    else 0
                end as status_events_bc,
                coalesce(rmdn.qty_entry, rmun.qty_entry, rzun.qty_entry, rmen.qty_entry) as qty_entry,
                coalesce(rmdn.qty_out_event1, rmun.qty_out_event1, rzun.qty_out_event1, rmen.qty_out) as qty_out,
                coalesce(rmdn.qty_restan_event1, rmun.qty_restan_event1, rzun.qty_restan_event1, rmen.qty_restan) as qty_restan,
                coalesce(rmdn.status_event1, rmun.status_event1, rzun.status_event1, rmen.status) as status,
                coalesce(rmdn.update_date, rmun.update_date, rzun.update_date, rmen.update_date) as update_date_restan,
                case
                    when exists (select 0 from links_bc lb where lb.event2_id::text = cte.id::text) then 1
                    else 0
                end as availability_backward,
                case
                    when exists (select 0 from links_bc lb where lb.event1_id::text = cte.id::text) then 1
                    else 0
                end as availability_forward
            from cte
                left join restan_mb51_ds_new rmdn on cte.id::text = rmdn.unique_id::text
                left join restan_mb51_us_new rmun on cte.id::text = rmun.unique_id::text
                left join restan_zmb_us_new rzun on cte.id::text = rzun.unique_id::text
                left join restan_mb51_esm rmen on cte.id::text = rmen.unique_id::text
        ),
        cte3 as(
            select
                cte2.task_name,
                cte2.id,
                case
                    when cte2.qty_entry is not null then 1
                    else 0
                end as status_restan,
                cte2.status_events_bc,
                case
                    when cte2.status_events_bc = 1 and cte2.status::text = 'Yes'::text and cte2.qty_entry <> 0::numeric then 'Partial'::text
                    when cte2.status_events_bc = 1 and cte2.status::text = 'No'::text and cte2.qty_entry <> 0::numeric and round(cte2.qty_restan, 2) <> 0::numeric then 'Partial'::text
                    when cte2.status_events_bc = 1 and cte2.status::text = 'No'::text and cte2.qty_entry <> 0::numeric and round(cte2.qty_restan, 2) = 0::numeric then 'Full'::text
                    else 'Unprocessed'::text
                end as status_traceability,
                round(cte2.qty_restan, 2) as qty_restan,
                cte2.update_date_restan,
                case
                    when cte2.status_events_bc = 0 then '-1'::integer
                    when cte2.status_events_bc = 1 and cte2.availability_backward = 1 and cte2.availability_forward = 1 then 0
                    else 1
                end as is_disconnected
            from cte2
        )
        select
            cte3.task_name,
            cte3.id,
            cte3.status_restan,
            cte3.status_events_bc,
            case
                when cte3.is_disconnected = 1 and cte3.status_traceability <> 'Unprocessed' then 'Partial'::text
                else cte3.status_traceability
            end as status_traceability,
            cte3.qty_restan,
            cte3.update_date_restan,
            cte3.is_disconnected
        from cte3
        ;
    """
    monitoring_detail = pd.read_sql(query_monitoring_detail, connection_db)
    monitoring_detail_partial = monitoring_detail[monitoring_detail['status_traceability']=='Partial']
    
    return monitoring_detail_partial


#Import the required libraries
import pandas as pd
import hashlib
from psycopg2.extras import execute_values
from src import modules_report_traceability as mrt


#MONITORING FUNCTIONS ----------------------------------------------------------------------------------------

#tracing monitoring zmb us
def trace_monitoring_zmb_us(connection_db, date_param):

    #get transaction data
    print('\n[ZMB US] Get Transaction and Restan Data...')
    query_reference_zmb_us = f"""
        with cte as(
            select
                'TRANS_ZMB_US' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                t.date::date, t.plant, t.process_information, t.total::float, t.input_date::date,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                row_number() over(partition by t.id) priority
            from trans_zmb_us t
            left join events_bc eb
            on t.id = eb.unique_id
        )
        select
            cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
            r.status_event1 status, null issue_code
        from cte
        left join restan_zmb_us_new r
        on cte.id = r.unique_id
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_zmb_us, )
        reference_data_zmb_us = cur.fetchall()

    reference_zmb_us = pd.DataFrame(reference_data_zmb_us,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'date', 'plant', 'process_information', 'total',
            'input_date', 'events_bc', 'priority', 'qty_entry',
            'qty_out', 'qty_restan', 'status', 'issue_code'
        ])

    reference_zmb_us['range_date'] = reference_zmb_us['input_date'] - reference_zmb_us['date']
    reference_zmb_us['process_date'] = reference_zmb_us['date']

    #summary remark
    print('[ZMB US] Create Remark...')
    task = reference_zmb_us['task_name'].iloc[0]
    reference_zmb_us['issue_code'] = reference_zmb_us.apply(lambda x: mrt.notes_trace(task, x, date_param) if x['events_bc']==0 else None, axis=1)
    reference_zmb_us = reference_zmb_us[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[ZMB US] Generate Hash...')
    reference_zmb_us['hash'] = reference_zmb_us.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in reference_zmb_us.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return reference_zmb_us

#tracing monitoring mb51 us
def trace_monitoring_mb51_us(connection_db, date_param):

    #mapping reference data
    print('\n[MB51US] Get Transaction and Restan Data...')
    query_reference_data_mb51_us = f"""
        with cte as(
            select
                'TRANS_MB51US' task_name, t.id, t.recnum::bigint,
                t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.name1, t.batch, t.plant, t.material, t.storagelocation,
                t.valuationtype, t.purchaseorder, t.precedingdocument,
                t.ordername, t.qtyinunofentry::float, t.movementtype,
                t.postingdate, t.purchaseordernumber, t.reference, t.documentheadertext,
                t.materialdocument, t.ordertype, t.text, t.entry_date::date,
                row_number() over(partition by t.id) priority
            from trans_mb51_us t
            left join events_bc eb
            on t.id = eb.unique_id
            where
                t.entry_date::date <= '{date_param}'::date and
                '{date_param}'::date - interval '90 days' <= t.entry_date::date
        )
        select
            cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
            r.status_event1 status, null issue_code
        from cte
        left join restan_mb51_us_new r
        on cte.id = r.unique_id
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_data_mb51_us, )
        reference_data_mb51_us = cur.fetchall()

    reference_mb51_us_comp = pd.DataFrame(reference_data_mb51_us,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw', 'events_bc',
            'name1', 'batch', 'plant', 'material', 'storagelocation',
            'valuationtype', 'purchaseorder', 'precedingdocument',
            'ordername', 'qtyinunofentry', 'movementtype', 'postingdate',
            'purchaseordernumber', 'reference', 'documentheadertext',
            'materialdocument', 'ordertype', 'text', 'entry_date', 'priority',
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

    reference_mb51_us_comp['movementtypeint'] = reference_mb51_us_comp['movementtype'].astype(str).str[-1].astype(int)

    #create hash for cols noise comp
    col_hash_noise1 = ['plant', 'batch', 'storagelocation', 'material', 'valuationtype', 'purchaseorder', 'precedingdocument', 'ordername', 'postingdate']
    col_hash_noise2 = ['plant', 'batch', 'storagelocation', 'material', 'valuationtype', 'reference', 'ordername', 'postingdate']
    col_hash_noise3 = ['plant', 'batch', 'storagelocation', 'material', 'valuationtype', 'ordername']
    reference_mb51_us_comp['hash_noise1'] = reference_mb51_us_comp[col_hash_noise1].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)
    reference_mb51_us_comp['hash_noise2'] = reference_mb51_us_comp[col_hash_noise2].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)
    reference_mb51_us_comp['hash_noise3'] = reference_mb51_us_comp[col_hash_noise3].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)

    #check noise 1
    print('[MB51US] Checking Noise Part 1...')
    data_unprocessed_mb51_us_even = reference_mb51_us_comp[
        (reference_mb51_us_comp['movementtype'].str[-1].astype(int) % 2 == 0) &
        (reference_mb51_us_comp['type'].isin(['mill', 'estate', 'bulking', 'kcp']))
        ]
    data_unprocessed_mb51_us_even_ = data_unprocessed_mb51_us_even[~(
        (data_unprocessed_mb51_us_even['type']=='kcp') &
        (data_unprocessed_mb51_us_even['movementtype']=='914')
        )]

    data_unprocessed_mb51_us_odd = reference_mb51_us_comp[
        (reference_mb51_us_comp['movementtype'].str[-1].astype(int) % 2 == 1) &
        (reference_mb51_us_comp['type'].isin(['mill', 'estate', 'bulking', 'kcp']))
        ]
    data_unprocessed_mb51_us_odd_ = data_unprocessed_mb51_us_odd[~(
        (data_unprocessed_mb51_us_odd['type']=='kcp') &
        (data_unprocessed_mb51_us_odd['movementtype']=='913')
        )]


    data_unprocessed_mb51_us_additional = reference_mb51_us_comp[
        (~reference_mb51_us_comp['type'].isin(['mill', 'estate', 'bulking', 'kcp'])) |
        ((reference_mb51_us_comp['type']=='kcp') & (reference_mb51_us_comp['movementtype'].isin(['913','914'])))
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

    dataframe_single = pd.concat([
        data_unprocessed_mb51_us_even_[~data_unprocessed_mb51_us_even_.index.isin(df1_pair_df.index)],
        data_unprocessed_mb51_us_odd_
        ])

    dataframe_pair['issue_code'] = dataframe_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51US02'])


    #check noise 2
    print('[MB51US] Checking Noise Part 2...')
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
        df_reference_kcp_noice_check
        ])

    dataframe_additional_pair['issue_code'] = dataframe_additional_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51US02'])

    data_unprocessed_mb51_us_ = pd.concat([dataframe_pair, dataframe_additional_pair, dataframe_additional_single], axis=0)


    #check mill at zmb us
    print('[MB51US] Connect to ZMB US...')
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
    data_unprocessed_mb51_us_['process_date'] = data_unprocessed_mb51_us_['entry_date']


    #add notes for unprocessed trans to events
    print('[MB51US] Create Notes...')
    task = data_unprocessed_mb51_us_['task_name'].iloc[0]
    data_unprocessed_mb51_us_.loc[data_unprocessed_mb51_us_['events_bc']==1, 'issue_code'] = None
    data_unprocessed_mb51_us_['issue_code'] = data_unprocessed_mb51_us_.apply(lambda x: mrt.notes_trace(task, x, date_param) if pd.isna(x['issue_code']) and x['events_bc']==0 else x['issue_code'], axis=1)
    data_unprocessed_mb51_us_ = data_unprocessed_mb51_us_[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[MB51US] Generate Hash...')
    data_unprocessed_mb51_us_['hash'] = data_unprocessed_mb51_us_.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in data_unprocessed_mb51_us_.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return data_unprocessed_mb51_us_

#tracing monitoring mb51 ds
def trace_monitoring_mb51_ds(connection_db, date_param):

    #mapping reference data
    print('\n[MB51DS] Get Transaction and Restan Data...')
    query_reference_data_mb51_ds = f"""
        with cte as(
            select
                'TRANS_MB51DS' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.plant, t.movementtype::int, t.qtyinunofentry::float, t.batch, t.storagelocation,
                t.material::int, t.entry_date::date,
                row_number() over(partition by t.id) priority
            from trans_mb51_ds t
            left join events_bc eb
            on t.id = eb.unique_id
            where
                t.entry_date::date <= '{date_param}'::date and
                '{date_param}'::date - interval '90 days' <= t.entry_date::date
        )
        select
            cte.*, r.qty_entry, r.qty_out_event1 qty_out, r.qty_restan_event1 qty_restan,
            r.status_event1 status, null issue_code
        from cte
        left join restan_mb51_ds_new r
        on cte.id = r.unique_id
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_data_mb51_ds, )
        reference_data_mb51_ds = cur.fetchall()

    reference_mb51_ds_comp = pd.DataFrame(reference_data_mb51_ds,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'events_bc', 'plant', 'movementtype', 'qtyinunofentry',
            'batch', 'storagelocation', 'material', 'entry_date',
            'priority', 'qty_entry', 'qty_out', 'qty_restan',
            'status', 'issue_code'
        ])

    for col in ['batch', 'storagelocation']:
        reference_mb51_ds_comp[col] = reference_mb51_ds_comp[col].fillna('-')

    col_hash = ['plant', 'batch', 'storagelocation', 'material']
    reference_mb51_ds_comp['hash_noise'] = reference_mb51_ds_comp[col_hash].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)

    #check noise
    print('[MB51DS] Checking Noise...')
    data_unprocessed_mb51_ds_even = reference_mb51_ds_comp[
        (reference_mb51_ds_comp['movementtype'].astype(str).str[-1].astype(int) % 2 == 0) &
        (reference_mb51_ds_comp['movementtype'] != 344)
        ]

    data_unprocessed_mb51_ds_odd = reference_mb51_ds_comp[
        (reference_mb51_ds_comp['movementtype'].astype(str).str[-1].astype(int) % 2 == 1) &
        (reference_mb51_ds_comp['movementtype'] != 343)
        ]

    data_unprocessed_mb51_ds_additional = reference_mb51_ds_comp[reference_mb51_ds_comp['movementtype'].isin([343,344])]

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

    dataframe_single = pd.concat([
        data_unprocessed_mb51_ds_even[~data_unprocessed_mb51_ds_even.index.isin(df1_pair_df.index)],
        data_unprocessed_mb51_ds_odd
        ])
    dataframe_single_2 = data_unprocessed_mb51_ds_additional

    dataframe_pair['issue_code'] = dataframe_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51DS02'])
    dataframe_single_2['issue_code'] = dataframe_single_2['issue_code'].apply(lambda x: x if pd.notna(x) else ['L-MB51DS01'])

    data_unprocessed_mb51_ds_new = pd.concat([dataframe_pair, dataframe_single, dataframe_single_2], axis=0)
    data_unprocessed_mb51_ds_new['process_date'] = data_unprocessed_mb51_ds_new['entry_date']


    #add notes for unprocessed trans to events
    print('[MB51DS] Create Notes...')
    task = data_unprocessed_mb51_ds_new['task_name'].iloc[0]
    data_unprocessed_mb51_ds_new.loc[data_unprocessed_mb51_ds_new['events_bc']==1, 'issue_code'] = None
    data_unprocessed_mb51_ds_new['issue_code'] = data_unprocessed_mb51_ds_new.apply(lambda x: mrt.notes_trace(task, x, date_param) if pd.isna(x['issue_code']) and x['events_bc']==0 else x['issue_code'], axis=1)
    data_unprocessed_mb51_ds_new = data_unprocessed_mb51_ds_new[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[MB51DS] Generate Hash...')
    data_unprocessed_mb51_ds_new['hash'] = data_unprocessed_mb51_ds_new.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in data_unprocessed_mb51_ds_new.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return data_unprocessed_mb51_ds_new

#tracing monitoring sodogi
def trace_monitoring_sodogi(connection_db, date_param):
    
    #get transaction data
    print('\n[SODOGI] Get Transaction Data...')
    query_reference_sodogi = f"""
        with cte as(
            select
                'TRANS_SODOGIDS' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.actual_gi_date::date, t.sales_order_no::bigint, t.do_qty::bigint,
                t.do_number::bigint, t.batch_no, t.status,
                row_number() over(partition by t.id) priority
            from trans_sodogi t
            left join events_bc eb
            on t.id = eb.unique_id
        )
        select *
        from cte
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_sodogi, )
        reference_data_sodogi = cur.fetchall()

    reference_sodogi = pd.DataFrame(reference_data_sodogi,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'events_bc', 'actual_gi_date', 'sales_order_no',
            'do_qty', 'do_number', 'batch_no', 'status', 'priority'
        ])


    #check so in mb51ds
    print('[SODOGI] Check SO in MB51DS...')
    sales_order_nos = reference_sodogi['sales_order_no'].unique()
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
    print('[SODOGI] Check SO and Batch in Events...')
    so_batch_pairs = reference_sodogi[
        (~reference_sodogi['sales_order_no'].isna()) &
        (~reference_sodogi['batch_no'].isna())
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


    reference_sodogi = reference_sodogi.merge(result_mb51_ds, how='left', on='sales_order_no')
    reference_sodogi = reference_sodogi.merge(result_check_batch_so_events, how='left', on=['sales_order_no', 'batch_no'])
    reference_sodogi['process_date'] = reference_sodogi['actual_gi_date']


    #summary remark
    print('[SODOGI] Create Remark...')
    task = reference_sodogi['task_name'].iloc[0]
    reference_sodogi['issue_code'] = reference_sodogi.apply(lambda x: mrt.notes_trace(task, x, date_param) if x['events_bc']==0 else None, axis=1)
    reference_sodogi = reference_sodogi[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[SODOGI] Generate Hash...')
    reference_sodogi['hash'] = reference_sodogi.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in reference_sodogi.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return reference_sodogi

#tracing monitoring openlink
def trace_monitoring_openlink(connection_db, date_param):

    #get transaction data
    print('\n[OPENLINK] Get Transaction Data...')
    query_reference_openlink = f"""
        with cte as(
            select
                'TRANS_OPENLINK' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.bl_date::date, t.backstop_id::bigint, t.contract_type,
                t.sap_so_po_number::bigint, t.delivery_status, t.status,
                row_number() over(partition by t.id) priority
            from trans_openlink t
            left join events_bc eb
            on t.id = eb.unique_id
        )
        select *
        from cte
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_openlink, )
        reference_data_openlink = cur.fetchall()

    reference_openlink = pd.DataFrame(reference_data_openlink,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'events_bc', 'bl_date', 'backstop_id', 'contract_type',
            'sap_so_po_number', 'delivery_status', 'status', 'priority'
        ])


    #cek related data DSI Interco Sales
    print('[OPENLINK] Get Related Data DSI Interco Sales...')
    sap_so_po_numbers = reference_openlink[reference_openlink['contract_type']=='DSI Interco Sales']['sap_so_po_number'].unique()
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
    print('[OPENLINK] Get Related Data 3rd Party Sales...')
    tps_backstop_ids = reference_openlink[reference_openlink['contract_type']=='3rd Party Sales']['backstop_id'].unique()
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

    df = reference_openlink.merge(result_sap_so_po_numbers, how='left', on='sap_so_po_number')
    df = df.merge(result_backstop_ids, how='left', on=['backstop_id', 'contract_type'])
    df['process_date'] = df['bl_date']

    #summary remark
    print('[OPENLINK] Create Remark...')
    task = df['task_name'].iloc[0]
    df['issue_code'] = df.apply(lambda x: mrt.notes_trace(task, x, date_param) if x['events_bc']==0 else None, axis=1)
    df = df[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[OPENLINK] Generate Hash...')
    df['hash'] = df.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in df.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return df

#tracing monitoring mb51 esm
def trace_monitoring_mb51_esm(connection_db, date_param):

    #mapping reference data
    print('\n[MB51 ESM] Get Transaction and Restan Data...')
    query_reference_data_mb51_esm = f"""
        with cte as(
            select
                'TRANS_MB51_ESM' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.plant, t.movement_type::int, t.quantity::float, t.batch, t.sloc,
                t.material::int, t.entry_date::date,
                row_number() over(partition by t.id) priority
            from trans_mb51_esm t
            left join events_bc eb
            on t.id = eb.unique_id
            where
                t.entry_date::date <= '{date_param}'::date and
                '{date_param}'::date - interval '90 days' <= t.entry_date::date
        )
        select
            cte.*, r.qty_entry, r.qty_out, r.qty_restan,
            r.status, null issue_code
        from cte
        left join restan_mb51_esm r
        on cte.id = r.unique_id
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_data_mb51_esm, )
        reference_data_mb51_esm = cur.fetchall()

    reference_mb51_esm_comp = pd.DataFrame(reference_data_mb51_esm,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'events_bc', 'plant', 'movement_type', 'quantity',
            'batch', 'sloc', 'material', 'entry_date',
            'priority', 'qty_entry', 'qty_out', 'qty_restan',
            'status', 'issue_code'
        ])

    reference_mb51_esm_comp['batch'] = reference_mb51_esm_comp['batch'].fillna('-')


    col_hash = ['plant', 'batch', 'sloc', 'material']
    reference_mb51_esm_comp['hash_noise'] = reference_mb51_esm_comp[col_hash].apply(lambda x: hashlib.sha256(b''.join(x.values.astype(str))).hexdigest(), axis=1)

    #check noise
    print('[MB51 ESM] Checking Noise...')
    data_unprocessed_mb51_esm_even = reference_mb51_esm_comp[
        (reference_mb51_esm_comp['movement_type'].astype(str).str[-1].astype(int) % 2 == 0) &
        (reference_mb51_esm_comp['movement_type'] != 344)
        ]

    data_unprocessed_mb51_esm_odd = reference_mb51_esm_comp[
        (reference_mb51_esm_comp['movement_type'].astype(str).str[-1].astype(int) % 2 == 1) &
        (reference_mb51_esm_comp['movement_type'] != 343)
        ]

    data_unprocessed_mb51_esm_additional = reference_mb51_esm_comp[reference_mb51_esm_comp['movement_type'].isin([343,344])]

    df1_pairs = []
    df2_pairs = []

    i = 1
    print(f'>> Total Unprocessed MB51 ESM Even to check: {len(data_unprocessed_mb51_esm_even)}')
    for idx, row in data_unprocessed_mb51_esm_even.iterrows():
        print(f'>>>> Progress Check Noise MB51 ESM Even: {i}/{len(data_unprocessed_mb51_esm_even)}')
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

    dataframe_single = pd.concat([
        data_unprocessed_mb51_esm_even[~data_unprocessed_mb51_esm_even.index.isin(df1_pair_df.index)],
        data_unprocessed_mb51_esm_odd
        ])
    dataframe_single_2 = data_unprocessed_mb51_esm_additional

    dataframe_pair['issue_code'] = dataframe_pair['issue_code'].apply(lambda x: x if pd.notna(x) else ['M-MB51_ESM01'])
    dataframe_single_2['issue_code'] = dataframe_single_2['issue_code'].apply(lambda x: x if pd.notna(x) else ['L-MB51_ESM01'])

    data_unprocessed_mb51_esm_new = pd.concat([dataframe_pair, dataframe_single, dataframe_single_2], axis=0)
    data_unprocessed_mb51_esm_new['process_date'] = data_unprocessed_mb51_esm_new['entry_date']


    #add notes for unprocessed trans to events
    print('[MB51 ESM] Create Notes...')
    task = data_unprocessed_mb51_esm_new['task_name'].iloc[0]
    data_unprocessed_mb51_esm_new.loc[data_unprocessed_mb51_esm_new['events_bc']==1, 'issue_code'] = None
    data_unprocessed_mb51_esm_new['issue_code'] = data_unprocessed_mb51_esm_new.apply(lambda x: mrt.notes_trace(task, x, date_param) if pd.isna(x['issue_code']) and x['events_bc']==0 else x['issue_code'], axis=1)
    data_unprocessed_mb51_esm_new = data_unprocessed_mb51_esm_new[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[MB51 ESM] Generate Hash...')
    data_unprocessed_mb51_esm_new['hash'] = data_unprocessed_mb51_esm_new.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in data_unprocessed_mb51_esm_new.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return data_unprocessed_mb51_esm_new

#tracing monitoring sodogi esm
def trace_monitoring_sodogi_esm(connection_db, date_param):
    
    #get transaction data
    print('\n[SODOGI ESM] Get Transaction Data...')
    query_reference_sodogi_esm = f"""
        with cte as(
            select
                'TRANS_SODOGI_ESM' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.good_issued_date::date, t.sales_order_no::bigint, t.issued_qty,
                t.do_number::bigint, row_number() over(partition by t.id) priority
            from trans_sodogi_esm t
            left join events_bc eb
            on t.id = eb.unique_id
        )
        select *
        from cte
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_sodogi_esm, )
        reference_data_sodogi_esm = cur.fetchall()

    reference_sodogi_esm = pd.DataFrame(reference_data_sodogi_esm,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'events_bc', 'good_issued_date', 'sales_order_no',
            'issued_qty', 'do_number', 'priority'
        ])


    #check do in mb51 esm
    print('[SODOGI ESM] Check DO in MB51 ESM...')
    do_numbers = reference_sodogi_esm['do_number'].unique()
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

    reference_sodogi_esm = reference_sodogi_esm.merge(result_check_do_numbers, how='left', on='do_number')
    reference_sodogi_esm['process_date'] = reference_sodogi_esm['good_issued_date']


    #summary remark
    print('[SODOGI ESM] Create Remark...')
    task = reference_sodogi_esm['task_name'].iloc[0]
    reference_sodogi_esm['issue_code'] = reference_sodogi_esm.apply(lambda x: mrt.notes_trace(task, x, date_param) if x['events_bc']==0 else None, axis=1)
    reference_sodogi_esm = reference_sodogi_esm[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[SODOGI ESM] Generate Hash...')
    reference_sodogi_esm['hash'] = reference_sodogi_esm.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in reference_sodogi_esm.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return reference_sodogi_esm

#tracing monitoring openlink esm
def trace_monitoring_openlink_esm(connection_db, date_param):

    #get transaction data
    print('\n[OPENLINK ESM] Get Transaction Data...')
    query_reference_openlink_esm = f"""
        with cte as(
            select
                'TRANS_OPENLINK_ESM' task_name, t.id, t.recnum::bigint, t.source_recnum_raw::bigint,
                case when eb.unique_id is not null then 1 else 0 end as events_status,
                t.bl_date::date, t.bl_number, t.contract_type, t.sap_so_po_number::bigint, 
                row_number() over(partition by t.id) priority
            from trans_openlink_esm t
            left join events_bc eb
            on t.id = eb.unique_id
        )
        select *
        from cte
        where cte.priority = 1
        ;
    """
    with connection_db.cursor() as cur:
        cur.execute(query_reference_openlink_esm, )
        reference_data_openlink_esm = cur.fetchall()

    reference_openlink_esm = pd.DataFrame(reference_data_openlink_esm,
        columns=[
            'task_name', 'id', 'recnum', 'source_recnum_raw',
            'events_bc', 'bl_date', 'bl_number', 'contract_type',
            'sap_so_po_number', 'priority'
        ])
    

    #check so in sodogi
    print('[OPENLINK ESM] Check SO in SODOGI ESM...')
    sap_so_po_numbers = reference_openlink_esm['sap_so_po_number'].unique()
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

    reference_openlink_esm = reference_openlink_esm.merge(result_sap_so_po_numbers, how='left', on='sap_so_po_number')
    reference_openlink_esm['process_date'] = reference_openlink_esm['bl_date']

    #summary remark
    print('[OPENLINK ESM] Create Remark...')
    task = reference_openlink_esm['task_name'].iloc[0]
    reference_openlink_esm['issue_code'] = reference_openlink_esm.apply(lambda x: mrt.notes_trace(task, x, date_param) if x['events_bc']==0 else None, axis=1)
    reference_openlink_esm = reference_openlink_esm[['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code']]

    #generate hash
    print('[OPENLINK ESM] Generate Hash...')
    reference_openlink_esm['hash'] = reference_openlink_esm.apply(
        lambda x: hashlib.sha256(
            b''.join(
                [
                    str(x[col]).encode('utf-8') if col != 'issue_code'
                    else ''.join(map(str, x['issue_code'])).encode('utf-8') if isinstance(x['issue_code'], list) 
                    else b''
                    for col in reference_openlink_esm.columns
                ]
            )
        ).hexdigest(),
        axis=1
    )

    return reference_openlink_esm

#proses data monitoring ke db
def process_monitoring_to_db(connection_db, df):

    #kategorisasi setiap record
    task_name = df['task_name'][0]
    query_check_id_hash_monitoring = f'''
        select id, hash from monitoring_trans
        where task_name = '{task_name}'
    '''
    reference_id_hash_monitoring = pd.read_sql(query_check_id_hash_monitoring, connection_db)

    df_flag = df.merge(reference_id_hash_monitoring, on='id', how='left', suffixes=('_init', '_db'))
    df_flag['flag'] = df_flag.apply(
        lambda row:
            'insert' if pd.isna(row['hash_db']) else
            'update' if row['hash_init'] != row['hash_db'] else
            'skip',
        axis=1
    )

    df_insert = df_flag[df_flag['flag']=='insert'][['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code', 'hash_init']].drop_duplicates(subset='id')
    df_update = df_flag[df_flag['flag']=='update'][['task_name', 'id', 'process_date', 'recnum', 'source_recnum_raw', 'issue_code', 'hash_init']].drop_duplicates(subset='id')

    df_insert.rename(columns={'hash_init': 'hash'}, inplace=True)
    df_update.rename(columns={'hash_init': 'hash'}, inplace=True)


    #bulk insert
    print(f'>> Total insert record monitoring for {task_name}: {len(df_insert)}')
    if not df_insert.empty:
        try:
            insert_cols = ', '.join(df_insert.columns)
            insert_data = [tuple(row) for row in df_insert.to_numpy()]
            insert_query = f"""
                insert into monitoring_trans ({insert_cols})
                values %s
            """
            with connection_db.cursor() as cursor:
                execute_values(cursor, insert_query, insert_data)
            connection_db.commit()
            print('>>>> Record monitoring has been inserted')
        except Exception as e:
            connection_db.rollback()
            print(f'>>>> Error while inserting record monitoring: {e}')
    else:
        pass


    #bulk update
    print(f'>> Total update record monitoring for {task_name}: {len(df_update)}')
    if not df_update.empty:
        try:
            update_cols = ', '.join([
                f"{col} = source.{col}::text[]" if col == 'issue_code' else f"{col} = source.{col}" 
                for col in df_update.columns if col not in ['task_name', 'id']
            ])
            update_data = [tuple(row) for row in df_update.to_numpy()]
            update_query = f"""
                update monitoring_trans as target
                set {update_cols}, last_update = now()
                from (values %s) as source({', '.join(df_update.columns)})
                where target.id = source.id
            """
            with connection_db.cursor() as cursor:
                execute_values(cursor, update_query, update_data)
            connection_db.commit()
            print('>>>> Record monitoring has been updated')
        except Exception as e:
            connection_db.rollback()
            print(f'>>>> Error while updating record monitoring: {e}')
    else:
        pass

    print()

#refresh materialized views
def refresh_matview_monitoring(connection_db):
    view_name = 'monitoring_trans_detail'
    try:
        connection_db.cursor().execute('begin;')
        query_refresh_monitoring = f'refresh materialized view {view_name};'
        connection_db.cursor().execute(query_refresh_monitoring, )
        connection_db.commit()
        print(f"\nMaterialized view '{view_name}' has been refreshed successfully.")
    except Exception as e:
        connection_db.cursor().rollback()
        print(f'\nError message while refresh materialized view: {e}')

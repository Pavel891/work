import pandas as pd
import datetime as dt
from conn_sql.pgsql_connect import conn_postgres
from config.cfg import stations
from psycopg2.extras import RealDictCursor
from dispatcher.upload_daily_funk import *

def report_gen(date):
    conn = conn_postgres.getconn()
    upload_date = pd.to_datetime(date)
    upload_date = dt.datetime.strftime(upload_date, '%Y-%m-%d')

    all_df = []

    # параметр выводит имена столбцов вместо индексов
    cur = conn.cursor(cursor_factory=RealDictCursor) 

    for i in stations:
        cur.execute("SELECT * FROM {} \
                    WHERE date = '{}'".format(i, upload_date))
        pgs_df = pd.DataFrame(cur.fetchall())
        # print(f'ИЗ БД: \n\t {pgs_df}')
        all_df.append(pgs_df)
        # print(f'ПОСЛЕ ДОБАВЛЕНИЯ: \n\t {all_df}')
    
    upload_df = pd.concat(all_df, ignore_index=True)
    # print(f'КОНЕЧНЫЙ DF: \n\t {upload_df}')

    cur.close()
    
    convert_dict = {
    'civil_wallet_connect_sum': float,
    'civil_wallet_refill_sum': float,
    'all_trans_card_connect_sum': float,
    'all_trans_card_refill_sum': float,
    'transf_trans_card_connect_sum': float,
    'transf_trans_card_refill_sum': float,
    'benefic_tariff_refill_sum': float,
    'stud_card_refill_sum': float,
    'stud_wallet_refill_sum': float,
    'school_card_refill_sum': float,
    'school_wallet_refill_sum': float,
    'benefic_wallet_refill_sum': float}

    try:
        upload_df = upload_df.astype(convert_dict)

        upload_df = upload_df[[
    'hall_id',
    'date',
    'trans_plan',
    'nikel_plan',
    'nikel_akp_pass',
    'nikel_sold',
    'civil_wallet_plan',
    'civil_wallet_pass',
    'civil_wallet_connect_count',
    'civil_wallet_connect_sum',
    'civil_wallet_refill_count',
    'civil_wallet_refill_sum',
    'all_trans_card_plan',
    'all_trans_card_pass',
    'all_trans_card_connect_count',
    'all_trans_card_connect_sum',
    'all_trans_card_refill_count',
    'all_trans_card_refill_sum',
    'transf_trans_card_plan',
    'transf_trans_card_pass',
    'transf_trans_card_connect_count',
    'transf_trans_card_connect_sum',
    'transf_trans_card_refill_count',
    'transf_trans_card_refill_sum',
    'benefic_tariff_plan',
    'benefic_tariff_pass',
    'benefic_tariff_refill_count',
    'benefic_tariff_refill_sum',
    'stud_card_plan',
    'stud_card_pass',
    'stud_card_refill_count',
    'stud_card_refill_sum',
    'stud_wallet_plan',
    'stud_wallet_pass',
    'stud_wallet_refill_count',
    'stud_wallet_refill_sum',
    'school_card_plan',
    'school_card_pass',
    'school_card_refill_count',
    'school_card_refill_sum',
    'school_wallet_plan',
    'school_wallet_pass',
    'school_wallet_refill_count',
    'school_wallet_refill_sum',
    'benefic_wallet_plan',
    'benefic_wallet_pass',
    'benefic_wallet_reliff_count',
    'benefic_wallet_refill_sum',
    'temp_card_release',
    'temp_card_pass',
    'company_card_plan',
    'company_card_pass',
    'credit_card_plan',
    'credit_card_pass',
    'service_card_pass',
    'manual_control_pass',
    'apn_sale_cash_count',
    'apn_sale_nocash_count']]

        upload_df.insert(3, 'all_paid_passed', transported_pass(upload_df))
        upload_df.insert(4, 'plan_more_less', transported_plan(upload_df))
        upload_df.insert(7, 'nikel_plan_diff', nikel_plan(upload_df))
        upload_df.insert(9, 'nikel_lost', nikel_loss(upload_df))
        upload_df.insert(12, 'civil_wallet_plan_diff', cw_plan(upload_df))
        upload_df.insert(19, 'all_trans_card_plan_diff', tc_plan(upload_df))
        upload_df.insert(26, 'transf_trans_card_plan_diff', ttc_plan(upload_df))
        upload_df.insert(33, 'benefic_tariff_plan_diff', bt_plan(upload_df))
        upload_df.insert(38, 'stud_card_plan_diff', sc_plan(upload_df))
        upload_df.insert(43, 'stud_wallet_plan_diff', sw_plan(upload_df))
        upload_df.insert(48, 'school_card_plan_diff', scc_plan(upload_df))
        upload_df.insert(53, 'school_wallet_plan_diff', swc_plan(upload_df))
        upload_df.insert(58, 'benefic_wallet_plan_diff', bw_plan(upload_df))
        upload_df.insert(65, 'company_card_plan_diff', company_plan(upload_df))
        upload_df.insert(68, 'credit_card_plan_diff', cc_plan(upload_df))
        upload_df.insert(69, 'total_connect_count', all_connect_count(upload_df))
        upload_df.insert(70, 'total_connect_sum', all_connect_sum(upload_df))
        upload_df.insert(71, 'total_refill_count', all_refill_count(upload_df))
        upload_df.insert(72, 'total_refill_sum', all_refill_sum(upload_df))
        upload_df.insert(73, 'total_card_pass', all_card_pass(upload_df))
        upload_df.insert(76, 'total_passed', all_pass_count(upload_df))
        upload_df.insert(77, 'all_apn_sale', all_apn_sale_count(upload_df))

        conn_postgres.putconn(conn)
        # print(upload_df)
        return upload_df

    except KeyError:
        upload_df = None
        conn_postgres.putconn(conn)
        return upload_df
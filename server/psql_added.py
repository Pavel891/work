import psycopg2 as ps2
from df_refactor import list_for_sql

conn = ps2.connect(dbname='postgres',
                   user='postgres',
                   password='12349876',
                   host='localhost',
                   port='5433')

station = 'place_report'

try:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO {} (date, trans_plan, nikel_plan, civil_wallet_plan, all_trans_card_plan, \
                             transf_trans_card_plan, benefic_tariff_plan, stud_card_plan, \
                             stud_wallet_plan, school_card_plan, school_wallet_plan, \
                             benefic_wallet_plan, company_card_plan, credit_card_plan, \
                             manual_control_pass, apn_sale_cash_count, apn_sale_nocash_count, \
                             nikel_akp_pass, nikel_sold, civil_wallet_pass, \
                             civil_wallet_connect_count, civil_wallet_connect_sum, civil_wallet_refill_count,\
                             all_trans_card_pass, all_trans_card_connect_count, \
                             all_trans_card_connect_sum, all_trans_card_refill_count, \
                             transf_trans_card_pass, transf_trans_card_connect_count, \
                             transf_trans_card_connect_sum, transf_trans_card_refill_count, \
                             benefic_tariff_pass, benefic_tariff_refill_count, \
                             stud_card_pass, stud_card_refill_count, \
                             stud_wallet_pass, stud_wallet_refill_count, \
                             school_card_pass, school_card_refill_count, \
                             school_wallet_pass, school_wallet_refill_count, \
                             benefic_wallet_pass, benefic_wallet_reliff_count, \
                             temp_card_pass, temp_card_release, company_card_pass, credit_card_pass, \
                             service_card_pass, civil_wallet_refill_sum, all_trans_card_refill_sum, \
                             transf_trans_card_refill_sum, benefic_tariff_refill_sum, \
                             stud_card_refill_sum, stud_wallet_refill_sum, \
                             school_card_refill_sum, school_wallet_refill_sum, benefic_wallet_refill_sum) \
                     VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, \
                             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(station), list_for_sql)
        conn.commit()
except (Exception, ps2.Error) as error:
    conn.rollback()
    print(error)
    print(type(error))
import itertools
import numpy as np
import stations.connection_price as cp
import psycopg2 as ps2

from stations.ms_output import csc, ucsp, ula
from stations.plan_task_output import month_plan
from conn_sql.pgsql_connect import conn_postgres
from conn_sql.mssql_connect import conn_mssql

class LoaderData:
    def __init__(self, date, station, passed, sel_cash, nocash_sold):
        self.stat = station
        self.no_db_data = [passed, sel_cash, nocash_sold]
        self.conn_postgres = conn_postgres
        self.report_date = date
        
        self.ucsp = ucsp(conn_mssql(station), date)

        self.csc = csc(conn_mssql(station), date)

        self.ula = ula(conn_mssql(station), date)

        self.plan = month_plan(conn_postgres, date, station)

        self.nikel_list = self.nikel_values(self.ula)

        self.electro_payments = self.civil_pay(self.ucsp, self.csc, cp.civil_wallet, cp.all_trans_card, cp.transfer_card)

        self.refill_sums = self.refill_cards(self.ucsp)
        
        self.list_for_sql = list(itertools.chain(self.plan, self.no_db_data, self.nikel_list, self.electro_payments, self.refill_sums))
        self.list_for_sql = [str(v) for v in self.list_for_sql]
        self.list_for_sql.insert(0, self.report_date)
        self.psql_added_info(self.stat, self.list_for_sql)

    # таблица ula_df
    def nikel_values(self, nikel_df):
        nik_pass = nikel_df['AKP_Gain_nickel_cnt'].iloc[0]
        nik_sold = nikel_df['Saled_nickel_cnt'].iloc[0]

        return nik_pass, nik_sold

    # таблица ucsp_df, csc_df
    def civil_pay(self, card_df, cash_df, cw_price, atc_price, tc_price):
        cw_pass = card_df['CountUse'].iloc[5] + card_df['CountUse'].iloc[2]
        cw_conn = card_df['CountSale'].iloc[5]
        cw_conn_sum = card_df['CountSale'].iloc[5] * cw_price
        cw_refill = card_df['CountAdd'].iloc[5]

        at_pass = card_df['CountUse'].iloc[6]
        at_conn = card_df['CountSale'].iloc[6]
        at_conn_sum = card_df['CountSale'].iloc[6] * atc_price
        at_refill = card_df['CountAdd'].iloc[6]

        tc_pass = card_df['CountUse'].iloc[8]
        tc_conn = card_df['CountSale'].iloc[8]
        tc_conn_sum = card_df['CountSale'].iloc[8] * tc_price
        tc_refill = card_df['CountAdd'].iloc[8]

        bt_pass = card_df['CountUse'].iloc[3]
        bt_refill = card_df['CountAdd'].iloc[3]

        sc_pass = card_df['CountUse'].iloc[10]
        sc_refill = card_df['CountAdd'].iloc[10]

        sw_pass = card_df['CountUse'].iloc[9]
        sw_refill = card_df['CountAdd'].iloc[9]

        scc_pass = card_df['CountUse'].iloc[12]
        scc_refill = card_df['CountAdd'].iloc[12]

        scw_pass = card_df['CountUse'].iloc[11]
        scw_refill = card_df['CountAdd'].iloc[11]

        bw_pass = card_df['CountUse'].iloc[4]
        bw_refill = card_df['CountAdd'].iloc[4]

        te_pass = card_df['CountUse'].iloc[1]
        te_conn = card_df['CountSale'].iloc[1]

        cc_pass = card_df['CountUse'].iloc[7]

        cbc_pass = card_df['CountUse'].iloc[0]

        try:
            count_pass = cash_df[cash_df['Series'] == 99].agg({'Count': np.sum})
            serv_card_pass = count_pass[0]
        except:
            count_pass = 0
            serv_card_pass = count_pass

        return cw_pass, cw_conn, cw_conn_sum, cw_refill, \
            at_pass, at_conn, at_conn_sum, at_refill, \
            tc_pass, tc_conn, tc_conn_sum, tc_refill, \
            bt_pass, bt_refill, sc_pass, sc_refill, \
            sw_pass, sw_refill, scc_pass, scc_refill, \
            scw_pass, scw_refill, bw_pass, bw_refill, \
            te_pass, te_conn, cc_pass, cbc_pass, \
            serv_card_pass

    # таблица ucsp_df
    def refill_cards(self, ref_df):
        cw_refill_sum = ref_df['CasheAdd'].iloc[5] - ref_df['CasheMoneyReturn'].iloc[5]
        at_refill_sum = ref_df['CasheAdd'].iloc[6] - ref_df['CasheMoneyReturn'].iloc[6]
        tc_refill_sum = ref_df['CasheAdd'].iloc[8] - ref_df['CasheMoneyReturn'].iloc[8]
        bt_refill_sum = ref_df['CasheAdd'].iloc[3] - ref_df['CasheMoneyReturn'].iloc[3]
        sc_refill_sum = ref_df['CasheAdd'].iloc[10] - ref_df['CasheMoneyReturn'].iloc[10]
        sw_refill_sum = ref_df['CasheAdd'].iloc[9] - ref_df['CasheMoneyReturn'].iloc[9]
        scc_refill_sum = ref_df['CasheAdd'].iloc[12] - ref_df['CasheMoneyReturn'].iloc[10]
        scw_refill_sum = ref_df['CasheAdd'].iloc[11] - ref_df['CasheMoneyReturn'].iloc[11]
        bw_refill_sum = ref_df['CasheAdd'].iloc[4] - ref_df['CasheMoneyReturn'].iloc[4]

        return cw_refill_sum, at_refill_sum, tc_refill_sum, \
            bt_refill_sum, sc_refill_sum, sw_refill_sum, \
            scc_refill_sum, scw_refill_sum, bw_refill_sum

    def psql_added_info(self, stat, list):
        conn = self.conn_postgres.getconn()

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
                             %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)".format(stat), list)
                conn.commit()
        except (Exception, ps2.Error) as error:
            conn.rollback()
            print(error)
            print(type(error))
        finally:
            self.conn_postgres.putconn(conn)
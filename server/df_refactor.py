import numpy as np
from plan_task_output import plan_task
import datetime as dt
from main import input_date, ucsp_df, ula_df, csc_df
import connection_price as cp
import itertools

report_date = dt.datetime.strftime(input_date, '%Y-%m-%d')

'''
 - информация о жетонах берется из upd_e.dbo.UnloadAdd (ula_df)
 - информация об электронных платежах берется из upd_e.dbo.UnloadCardSeriesPage2 (ucsp_df)
 - информация о служебной карте (service_card) находится в таблице upd_e.dbo.CardSeriesCount(csc_df)
   (sum(count), series==99)
 - информация о картах банков (bank_card) находится в таблице upd_e.dbo.CardSeriesCount(csc_df) 
   (sum(count), series==[61, 62, 63, 64, 66, 68, 69])
 - кол-во проходов через ручной контроль(manual_control_pass) и кол-во жетонов проданных
   из автомата за наличный расчет(APN_sale_cahs_count) необходимо вносить вручную при
   составлении каждого нового дневного отчета на станциях
 - все плановые показатели так же необходимо вводить вручную
'''


# таблица ula_df
def nikel_values(nikel_df):
    nik_pass = nikel_df['AKP_Gain_nickel_cnt'].iloc[0]
    nik_sold = nikel_df['Saled_nickel_cnt'].iloc[0]
    return nik_pass, nik_sold #, nik_nocash_sold

# таблица ucsp_df, csc_df
def civil_pay(card_df, cash_df, cw_price, atc_price, tc_price):
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

    count_pass = cash_df[cash_df['Series'] == 99].agg({'Count': np.sum})
    serv_card_pass = count_pass[0]

    return cw_pass, cw_conn, cw_conn_sum, cw_refill, \
        at_pass, at_conn, at_conn_sum, at_refill, \
        tc_pass, tc_conn, tc_conn_sum, tc_refill, \
        bt_pass, bt_refill, sc_pass, sc_refill, \
        sw_pass, sw_refill, scc_pass, scc_refill, \
        scw_pass, scw_refill, bw_pass, bw_refill, \
        te_pass, te_conn, cc_pass, cbc_pass, \
        serv_card_pass

# таблица ucsp_df
def refill_cards(ref_df):
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

mc_pass = input('Количество проходов через ручной контроль: ') # manual_control_pass
nikel_sel_cash = input('Количество жетонов проданных в АПЖ по наличному расчету: ') # apn_sale_cash_count
nik_nocash_sold = input('Количество жетонов проданных в АПЖ по безналичному расчету: ') # apn_sale_nocash_count

no_db_data = [mc_pass, nikel_sel_cash, nik_nocash_sold]
# print(entered_var_list)
# print('*' * 60)
nikel_list = nikel_values(ula_df)
# print(nikel_list)
# print('*' * 60)
electro_payments = civil_pay(ucsp_df, csc_df, cp.civil_wallet, cp.all_trans_card, cp.transfer_card)
# print(electro_payments)
# print('*' * 60)
refill_sums = refill_cards(ucsp_df)
# print(refill_sums)
# print('*' * 60)

list_for_sql = list(itertools.chain(plan_task, no_db_data, nikel_list, electro_payments, refill_sums))
list_for_sql = [str(v) for v in list_for_sql]
list_for_sql.insert(0, report_date)
# print(list_for_sql)
# print('*' * 60)
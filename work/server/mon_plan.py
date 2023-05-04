from sql_connect import conn_postgres
import datetime as dt
import pandas as pd

in_date = input('Дата начала действия плана в формате гггг-мм-дд: ')
in_date = pd.to_datetime(in_date)
plane_date = dt.datetime.strftime(in_date, '%Y%m%d')

nikel_plan = int(input('План перевозок по жетонам: ')) # nikel_plan 2350
cw_plan = int(input('План перевозок по гражданскому кошельку: ')) # civil_wallet_plan 1190
atc_plan = int(input('План перевозок по проездному на все виды транспорта: ')) # all_trans_card_plan 105
ttc_plan = int(input('План перевозок по пересадочному проездному на все виды транспорта: ')) # transf_trans_card_plan 20
bt_plan = int(input('План перевозок по тарифу для льготников(не более 50 поездок в мес.): ')) # benefic_tariff_plan 1245
sc_plan = int(input('План перевозок по студенческому проездному на все виды транспорта: ')) # stud_card_plan 200
sw_plan = int(input('План перевозок по студенческому кошельку: ')) # stud_wallet_plan 135
scc_plan = int(input('План перевозок по школьному проездному на все виды транспорта: ')) # school_card_plan 75
scw_plan = int(input('План перевозок по школьному кошельку: ')) # school_wallet_plan 75
bw_plan = int(input('План перевозок по льготному электронному кошельку: ')) # benefic_wallet_plan 200
cc_plan = int(input('План перевозок по проездному на все виды транспорта для ЮЛ: ')) # company_card_plan 5
cbc_plan = int(input('План перевозок по бесконтактной банковской карте(ББК): ')) # credit_card_plan 10165

trans_plan = nikel_plan + cw_plan + atc_plan + ttc_plan + bt_plan + sc_plan + sw_plan + scc_plan + scw_plan + bw_plan + cc_plan + cbc_plan

plan_list = [plane_date, trans_plan, nikel_plan, cw_plan, atc_plan, ttc_plan, bt_plan, sc_plan,
             sw_plan, scc_plan, scw_plan, bw_plan, cc_plan, cbc_plan]
plan_list = [str(v) for v in plan_list]

cur = conn_postgres.cursor()

cur.execute("INSERT INTO month_planned_task (date, trans_plan, nikel_plan, civil_wallet_plan, all_trans_card_plan, \
                             transf_trans_card_plan, benefic_tariff_plan, stud_card_plan, \
                             stud_wallet_plan, school_card_plan, school_wallet_plan, \
                             benefic_wallet_plan, company_card_plan, credit_card_plan) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", plan_list)

conn_postgres.commit()

cur.close()
conn_postgres.close()
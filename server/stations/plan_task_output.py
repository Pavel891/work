import datetime as dt
import pandas as pd

# достаем плановые задачи из базы PostgreSQL, таблица month_planned_task
def month_plan(conn, start_date, stat_name):
  rep_date = dt.datetime.strptime(str(start_date), '%Y-%m-%d')
  rep_date = rep_date.replace(day = 1)

  stat_dict = {'prospect_report':'2',
              'uralmash_report':'3',
              'mash_report':'5', 
              'uralskay_report':'7',
              'dinamo_report':'8',
              'place_report':'9',
              'geolog_report':'10',
              'botan_report':'11',
              'chkalov_report':'12'}
  stat_id = stat_dict.get(stat_name)

  connect = conn.getconn()
  cur = connect.cursor()

  cur.execute("SELECT trans_plan, nikel_plan, civil_wallet_plan, all_trans_card_plan, \
                      transf_trans_card_plan, benefic_tariff_plan, stud_card_plan, \
                      stud_wallet_plan, school_card_plan, school_wallet_plan, \
                      benefic_wallet_plan, company_card_plan, credit_card_plan \
              FROM month_planned_task \
              WHERE date = %(P)s AND station_id = %(D)s", {'P': rep_date, 'D': stat_id})

  plan_df = pd.DataFrame(cur.fetchall())

  cur.close()
  conn.putconn(connect)

  plan_task = list(plan_df.iloc[0])

  return plan_task
import pandas as pd
import datetime as dt

from sql_connect import conn_postgres
from main import input_date

report_date = dt.datetime.strftime(input_date, '%Y-%m-%d')

# достаем плановые задачи из базы PostgreSQL, таблица month_planned_task
def month_plan(conn, start_date):

  start_date = dt.datetime.strptime(start_date, '%Y-%m-%d')
  start_date = start_date.replace(day = 1)

  cur = conn.cursor()

  cur.execute("SELECT trans_plan, nikel_plan, civil_wallet_plan, all_trans_card_plan, \
                      transf_trans_card_plan, benefic_tariff_plan, stud_card_plan, \
                      stud_wallet_plan, school_card_plan, school_wallet_plan, \
                      benefic_wallet_plan, company_card_plan, credit_card_plan \
              FROM month_planned_task \
              WHERE date = %(P)s", {'P': start_date})
  plan_df = pd.DataFrame(cur.fetchall())

  cur.close()
  conn.close()

  return plan_df

plan_df = month_plan(conn_postgres, report_date)

# print(plan_df.head())
# print('*' * 60)

plan_task = list(plan_df.iloc[0])
# print(plan_task)
# print('*' * 60)
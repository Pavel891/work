import pandas as pd
import datetime as dt

import warnings
warnings.filterwarnings('ignore')

def ucsp(conn, start_date):

    start_date = pd.to_datetime(start_date)
    start_date = dt.datetime.strftime(start_date, '%Y%m%d')

    cursor = conn.cursor(as_dict=True)

    cursor.execute("SELECT Data, SeriesName, CountUse, CountSale, CountAdd, \
                           CountMoneyReturn, CasheUse, CasheSale, CasheAdd, \
                           CasheMoneyReturn, CasheBank \
                    FROM dbo.UnloadCardSeriesPage2 \
                    WHERE CAST(Data AS DATE) = %(P)s", {'P': start_date})
        
    ucsp_df = pd.DataFrame(cursor.fetchall())

    cursor.close()

    return ucsp_df

def csc(conn, start_date):

    start_date = pd.to_datetime(start_date)
    start_date = dt.datetime.strftime(start_date, '%Y%m%d')

    cursor = conn.cursor(as_dict=True)

    cursor.execute("SELECT Data, Series, Count, Tariff \
                    FROM dbo.CardSeriesCount \
                    WHERE CAST(Data AS DATE) = %(P)s", {'P': start_date})
    
    csc_df = pd.DataFrame(cursor.fetchall())

    cursor.close()

    return csc_df


def uct(conn, start_date):

    start_date = pd.to_datetime(start_date)
    start_date = dt.datetime.strftime(start_date, '%Y%m%d')

    cursor = conn.cursor(as_dict=True)

    cursor.execute("SELECT Hall, DevName, Data, Count_pass, Count_discount, \
                           Count_student, Count_school, Count_personal, \
                           Count_discount_all, Delta_nickel, Delta_card, \
                           Delta_pass, Delta_discount, Delta_student, \
                           Delta_school, Delta_personal, Delta_discount_all, \
                           Fact_nickel, Fact_pay, Fact_notpay, Nickel_deviation \
                    FROM dbo.UnloadCountTran \
                    WHERE CAST(Data AS DATE) = %(P)s", {'P': start_date})
        
    uct_df = pd.DataFrame(cursor.fetchall())
    
    cursor.close()

    return uct_df

def ula(conn, start_date):

    start_date = pd.to_datetime(start_date)
    start_date = dt.datetime.strftime(start_date, '%Y%m%d')

    cursor = conn.cursor(as_dict=True)

    cursor.execute("SELECT * \
                    FROM dbo.UnloadAdd \
                    WHERE CAST(Data AS DATE) = %(P)s", {'P': start_date})
        
    ula_df = pd.DataFrame(cursor.fetchall())

    cursor.close()
    conn.close()

    return ula_df

#------------------------------------------------------------------------------------

# csc_df = csc(conn_mssql, start_date)
# print(csc_df.head())
# print('*' * 60)
# print(csc_df.info())
# print('*' * 60)
# print(csc_df.describe())
# print('*' * 100)

# ucsp_df = ucsp(conn_mssql, start_date)
# print(ucsp_df.head())
# print('*' * 60)
# print(ucsp_df.info())
# print('*' * 60)
# print(UnloadCardSeriesPage.describe())
# print('*' * 100)

# uct_df = uct(conn_mssql, start_date)
# print(uct_df.head())
# print('*' * 60)
# print(uct_df.info())
# print('*' * 60)
# print(UnloadCountTran.describe())
# print('*' * 100)

# ula_df = ula(conn_mssql, start_date)
# print(uct_df.head())
# print('*' * 60)
# print(uct_df.info())
# print('*' * 60)
# print(UnloadCountTran.describe())
# print('*' * 100)

# conn_mssql.close()

#------------------------------------------------------------------------------------
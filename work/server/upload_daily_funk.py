def transported_pass(df):

    transport_sum = df[['nikel_akp_pass', 'civil_wallet_pass', 'all_trans_card_pass',
    'transf_trans_card_pass', 'benefic_tariff_pass', 'stud_card_pass',
    'stud_wallet_pass', 'school_card_pass', 'school_wallet_pass', 'benefic_wallet_pass',
    'temp_card_pass', 'company_card_pass', 'credit_card_pass']].sum(axis=1)

    return transport_sum

def transported_plan(df):
    trans_plan_diff = df['all_paid_passed'] - df['trans_plan']

    return trans_plan_diff

def nikel_plan(df):
    nikel_plan_diff = df['nikel_akp_pass'] - df['nikel_plan']

    return nikel_plan_diff

def nikel_loss(df):
    lost = df['nikel_sold'] - df['nikel_akp_pass']

    return lost

def cw_plan(df):
    cw_diff = df['civil_wallet_pass'] - df['civil_wallet_plan']

    return cw_diff

def tc_plan(df):
    tc_diff = df['all_trans_card_pass'] - df['all_trans_card_plan']

    return tc_diff

def ttc_plan(df):
    ttc_diff = df['transf_trans_card_pass'] - df['transf_trans_card_plan']

    return ttc_diff

def bt_plan(df):
    bt_diff = df['benefic_tariff_pass'] - df['benefic_tariff_plan']

    return bt_diff

def sc_plan(df):
    sc_diff = df['stud_card_pass'] - df['stud_card_plan']

    return sc_diff

def sw_plan(df):
    sw_diff = df['stud_wallet_pass'] - df['stud_wallet_plan']

    return sw_diff

def scc_plan(df):
    scc_diff = df['school_card_pass'] - df['school_card_plan']

    return scc_diff

def swc_plan(df):
    swc_diff = df['school_wallet_pass'] - df['school_wallet_plan']

    return swc_diff

def bw_plan(df):
    bw_diff = df['benefic_wallet_pass'] - df['benefic_wallet_plan']

    return bw_diff

def company_plan(df):
    company_diff = df['company_card_pass'] - df['company_card_plan']

    return company_diff

def cc_plan(df):
    cc_diff = df['credit_card_pass'] - df['credit_card_plan']

    return cc_diff

def all_connect_count(df):
    all_conn = df[['civil_wallet_connect_count', 'all_trans_card_connect_count', 'transf_trans_card_connect_count']].sum(axis=1)

    return all_conn

def all_connect_sum(df):
    all_conn_sum = df[['civil_wallet_connect_sum', 'all_trans_card_connect_sum', 'transf_trans_card_connect_sum']].sum(axis=1).round(2)

    return all_conn_sum

def all_refill_count(df):
    all_ref_count = df[['civil_wallet_refill_count', 'all_trans_card_refill_count', 'transf_trans_card_refill_count',
                       'benefic_tariff_refill_count', 'stud_card_refill_count', 'stud_wallet_refill_count',
                       'school_card_refill_count', 'school_wallet_refill_count', 'benefic_wallet_reliff_count']].sum(axis=1)

    return all_ref_count

def all_refill_sum(df):
    all_ref_sum = df[['civil_wallet_refill_sum', 'all_trans_card_refill_sum', 'transf_trans_card_refill_sum',
                       'benefic_tariff_refill_sum', 'stud_card_refill_sum', 'stud_wallet_refill_sum',
                       'school_card_refill_sum', 'school_wallet_refill_sum', 'benefic_wallet_refill_sum']].sum(axis=1).round(2)

    return all_ref_sum

def all_card_pass(df):
    card_pass = df[['civil_wallet_pass', 'all_trans_card_pass', 'transf_trans_card_pass',
                    'benefic_tariff_pass', 'stud_card_pass', 'stud_wallet_pass',
                    'school_card_pass', 'school_wallet_pass', 'benefic_wallet_pass',
                    'temp_card_pass', 'company_card_pass', 'credit_card_pass']].sum(axis=1)

    return card_pass

def all_pass_count(df):
    all_passed = df[['all_paid_passed', 'manual_control_pass']].sum(axis=1)

    return all_passed

def all_apn_sale_count(df):
    all_apn_sale = df[['apn_sale_cash_count', 'apn_sale_nocash_count']].sum(axis=1)

    return all_apn_sale
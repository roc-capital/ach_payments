import pandas as pd
import datetime
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import psycopg2
from dateutil.relativedelta import relativedelta

current = datetime.datetime.now()
today = datetime.date(current.year, current.month, current.day)
prior = {}
for i in range(1,7):
    prior['prior'+str(i)] = datetime.date((current-relativedelta(months=i)).year,(current - relativedelta(months=i)).month, 1)
last_day = datetime.date(2020,current.month,1) - relativedelta(days=1)
first_day = datetime.date(2020,current.month, 1)
host = ''
dates = list(range(1,32))

def tds_sql_extract(last_day):
    
    
    
    
    if last_day == datetime.date(current.year, current.month, current.day):
        #Connect to the roc_portal_db to extract information from the loans table

        try:
            conn = psycopg2.connect(host = host, database = 'roc_portal_db', user = '', password = '')
        except psycopg2.DatabaseError:
            print('failed to connect to database')
            exit (1)

        cursor = conn.cursor()


    
        sql_tds_today = """select
        tds_loans.account, 
        tds_loans.ach_account_number,
        tds_loans.ach_routing_number,
        tds_loans.ach_debit_amount, 
        tds_loans.ach_next_debit_date,
        tds_loans.ach_stop_date,
        tds_loans.next_due_date,
        tds_loans.ach_debit_due_day,
        tds_loans.ach_service_status,
        tds_loans.ach_frequency
        from tds_loans"""
    
        cursor.execute(sql_tds_today)
    
        tds_today_result = cursor.fetchall()
    
        df_tds = pd.DataFrame.from_records(tds_today_result, columns = ['Servicer ID', 'ACH Account Nmber', 'Routing Number', 'Debit Amount', 'Next Debit Date',
                                                 'Stop Date', 'Next Due Date', 'ACH Debit Due Day', 'ACH Service Status',
                                                  'ACH Frequency'])
        
    else:
        #Create a connection to the slifdb
        try:
            conn_slif = psycopg2.connect(host = host, database = 'slifdb', user = '', password = '')
        except psycopg2.DatabaseError:
            print('failed to connect to database')
            exit (1)

        cursor_slif = conn_slif.cursor()


        sql_snapshot = """select * 
        from sls_snapshots"""
        cursor_slif.execute(sql_snapshot)

        result_snapshot = cursor_slif.fetchall()

        df_snapshot = pd.DataFrame.from_records(result_snapshot, columns = ['SLS Snapshot ID', 'Date', 'Created'])
        df_snapshot = df_snapshot[((df_snapshot['Date']>pd.Timestamp(last_day)) & 
                           (df_snapshot['Date']<pd.Timestamp(last_day+datetime.timedelta(days=1))))]

        #Extract the snapshot ID of the day we are looking for
        sls_snapshot_id = df_snapshot.iloc[0,0].astype(int).item()


        #The SQL code to pull out the ACH information from the historical TDS loans table
        sql_tds = """select
        tds_loans.account, 
        tds_loans.ach_account_number,
        tds_loans.ach_routing_number,
        tds_loans.ach_debit_amount, 
        tds_loans.ach_next_debit_date,
        tds_loans.ach_stop_date,
        tds_loans.next_due_date,
        tds_loans.ach_debit_due_day,
        tds_loans.ach_service_status,
        tds_loans.ach_frequency
        from tds_loans
        where sls_snapshot_id = %(int)s"""
        cursor_slif.execute(sql_tds, {'int':sls_snapshot_id})

        result_tds = cursor_slif.fetchall()

        df_tds = pd.DataFrame.from_records(result_tds, columns = ['Servicer ID', 'ACH Account Nmber', 'Routing Number', 'Debit Amount', 'Next Debit Date',
                                                 'Stop Date', 'Next Due Date', 'ACH Debit Due Day', 'ACH Service Status',
                                                  'ACH Frequency'])


    
    return df_tds

def loans_sql_extract():
#Connect to the roc_portal_db to extract information from the loans table

    try:
        conn = psycopg2.connect(host = host, database = 'roc_portal_db', user = '', password = '')
    except psycopg2.DatabaseError:
        print('failed to connect to database')
        exit (1)

    cursor = conn.cursor()


    sql = """select
    loans.loan_status_id,
    loans.closing_date, 
    loans.payoff_date,
    loans.loan_id,
    servicer_id,
    loans.loan_subtype, 
    v_loan_summaries.holding_entity
    from (loans
    inner join v_loan_summaries
    on loans.loan_id = v_loan_summaries.loan_id)
    where loans.loan_subtype = 'Fix and Flip (1-4)' 
    and loans.loan_status_id in (3,4)
    """
    cursor.execute(sql)

    result = cursor.fetchall()

    df = pd.DataFrame.from_records(result, columns = ['Loan Status ID', 'Closing Date', 'Payoff Date', 'Loan ID', 'Servicer ID',
                                                 'Loan_Subtype', 'Holding Entity'])
    
    return df

def y_generator(index, values, dates):
    amounts = []
    index_value = 0
    for day in dates:
        if index_value>=len(index):
            amounts.append(0)
        else:
        
            if index[index_value]==day:
                amounts.append(values[day])
                index_value+=1
            else:
                amounts.append(0)
    
    return amounts


def ach_payment(df, df_tds, date, holding_entity = None, today= False):
    #Read in and Rename the fields and merge the dataframes and recast columns
    df['Servicer ID']=df['Servicer ID'].astype('object')
    combined_df = pd.merge(df_tds, df, on='Servicer ID')
    combined_df['Payoff Date'] = pd.to_datetime(combined_df['Payoff Date'])
    combined_df['Next Debit Date'] = pd.to_datetime(combined_df['Next Debit Date'])
    combined_df['Next Due Date'] = pd.to_datetime(combined_df['Next Due Date'])
    combined_df['Stop Date'] = pd.to_datetime(combined_df['Stop Date'])
    combined_df['Closing Date'] = pd.to_datetime(combined_df['Closing Date'])
    combined_df['ACH Debit Due Day'] = combined_df['ACH Debit Due Day'].astype('float64')
    combined_df['ACH Service Status'] = combined_df['ACH Service Status'].astype('float64')
    combined_df['ACH Frequency'] = combined_df['ACH Frequency'].astype('float64')
    combined_df['Loan Status ID'] = combined_df['Loan Status ID'].astype('int64')
    combined_df['Loan ID'] = combined_df['Loan ID'].astype('int64')
    combined_df['Debit Amount'] = combined_df['Debit Amount'].astype('float64')


    dates_denom = []

    #Creating Denominator for Seasoning of the ACH Payments over the month
    x=0
    while pd.Timestamp(date + relativedelta(days=x))< pd.Timestamp(date + relativedelta(months=1)):
        placeholder_day = pd.Timestamp(date + relativedelta(days=x))
        denom_df = combined_df[((combined_df['Loan Status ID']==3) | (combined_df['Payoff Date']>pd.Timestamp(placeholder_day)))]
        dates_denom.append(denom_df['Debit Amount'].sum())
        x+=1

    #Select just the loans that have not paid off yet at the beginning of the month
    combined_df = combined_df[((combined_df['Loan Status ID']==3) | (combined_df['Payoff Date']>pd.Timestamp(date)))]

    #Remove the loans that just closed and do not owe a payment yet
    combined_df = combined_df[combined_df['Closing Date'] < pd.Timestamp(date-relativedelta(months=1))]

    #All the loans that are late
    combined_late = combined_df[combined_df['Next Due Date']< pd.Timestamp(date-relativedelta(months=1))]

    #These are all of the recurring ACH payment accounts
    ach = combined_df[((combined_df['ACH Service Status']==1) & (combined_df['ACH Frequency']==1) & 
                   (combined_df['Stop Date']>= pd.Timestamp(date+relativedelta(months=1))))]

    #ACH accounts broken out into current and late
    ach_late = ach[ach['Next Due Date']<pd.Timestamp(date - relativedelta(months=1))]
    current = ach[ach['Next Due Date']>= pd.Timestamp(date - relativedelta(months=1))]

    #When payments for March will come in on ACH accounts, and make sure that IR accounts are taken out. 
    if today:
        month_ach = current[((current['Next Due Date'] < pd.Timestamp(date + relativedelta(months=1))) &(current['Next Debit Date']<pd.Timestamp(date+relativedelta(months=2))))]
    else:
        month_ach = current[((current['Next Due Date'] <= pd.Timestamp(date + relativedelta(months=1))) &(current['Next Debit Date']<pd.Timestamp(date+relativedelta(months=2))))]
    #Filter by Holding Entity
    
    if holding_entity == None:
        pass
    else:
        month_ach = month_ach[month_ach['holding_entity']==holding_entity]

    #Create the output for the chart
    e = month_ach.groupby(['ACH Debit Due Day']).sum()
    dates = list(range(1,32))
    output_values = y_generator(e.index, e['Debit Amount'], dates)

    #Create a cumulative sum of payments instead
    output_values = pd.Series(output_values).cumsum()
    
    for i in range(len(dates_denom)):
        dates_denom[i] = output_values[i]/dates_denom[i]
    
    while len(dates_denom)<31:
        dates_denom.append(dates_denom[-1])

    return dates_denom


df = loans_sql_extract()
values = {}

for i in range(1,7):
    last_day = prior['prior'+str(i)] + relativedelta(months=1) - relativedelta(days=1)
    df_tds = tds_sql_extract(last_day)
    values['prior'+str(i)] = ach_payment(df, df_tds, prior['prior'+str(i)])
    
df_tds = tds_sql_extract(first_day)
may_values = ach_payment(df, df_tds, first_day)


dates = list(range(1,32))
for i in range(1,7):
    label_month = (current - relativedelta(months=i)).month
    plt.plot(dates, values['prior'+str(i)], label=calendar.month_abbr[label_month])

plt.plot(dates, may_values, label = calendar.month_abbr[today.month], linewidth = 3.5)

plt.legend()
plt.title('ACH Scheduled Payments by Day')
plt.savefig('Scheduled Payments by Day.png')


#With seasoning curves as they are this is redundant
"""for i in range(today.day, len(current_values)):
    current_values[i]=0
april_diff = np.array(current_values)

plt.plot(dates, april_diff, label='Differences')
plt.title('Late Payments by Day')
plt.savefig('Late Payments by Day')"""
import pandas as pd
def remove_duplicate_transactions_func(transactions):
    # Drop duplicate transactions based on all columns
    clean_transactions = transactions.drop_duplicates()
    return clean_transactions
	
def remove_duplicate_accounts_func(accounts):
    # Drop duplicate accounts based on all columns
    clean_accounts = accounts.drop_duplicates()
    return clean_accounts

def calculate_received_amount_per_account_func(clean_transactions):
    # Group clean_transactions by the 'to_account' column and calculate the sum of 'euro' received per account
    received_amount_per_account = clean_transactions.groupby(['to_account'], sort=True, as_index = False)['euro'].sum()
    received_amount_per_account.rename(columns={"euro": "Added_Euro"}, errors="raise", inplace=True)
    return received_amount_per_account

def calculate_sent_amount_per_account_func(clean_transactions):
    # Group clean_transactions by the 'to_account' column and calculate the sum of 'euro' sent per account
    sent_amount_per_account = clean_transactions.groupby(['from_account'], sort=True, as_index = False)['euro'].sum()
    sent_amount_per_account.rename(columns={"euro": "Sent_Euro"}, errors="raise", inplace=True)
    return sent_amount_per_account

def calculate_balance_end_of_day_per_account_func(clean_account_balance, received_amount_per_account, sent_amount_per_account):
    # Join clean_account_balance and transactions on 'account_number' column and calculate the end of day balance per account
	    
    result = pd.merge(clean_account_balance, received_amount_per_account,how ='left', left_on='account_number', right_on='to_account')
    result_1 = pd.merge(result, sent_amount_per_account,how ='left', left_on='account_number', right_on='from_account')
    
    result_2=result_1[["account_number","balance","Added_Euro","Sent_Euro"]]
    result_2.fillna(0,inplace=True)
    
    result_2['Final_Euro'] = result_2['balance'] + result_2['Added_Euro'] - result_2['Sent_Euro']
    day_end_balance=result_2[["account_number","Final_Euro"]]
    
    return day_end_balance

def get_customers_with_negative_balance_func(day_end_balance):
    # Filter accounts with negative balance
    accounts_with_negative_balance = day_end_balance[day_end_balance['Final_Euro']<0]

    # Collect the customer account numbers with negative balance as a dictionary
    return accounts_with_negative_balance
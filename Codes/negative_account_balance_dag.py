#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pprint import pprint
import pandas as pd
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.operators.email_operator import EmailOperator
from classes.transaction_process_adapter import remove_duplicate_transactions_func
from classes.transaction_process_adapter import remove_duplicate_accounts_func
from classes.transaction_process_adapter import calculate_received_amount_per_account_func
from classes.transaction_process_adapter import calculate_sent_amount_per_account_func
from classes.transaction_process_adapter import calculate_balance_end_of_day_per_account_func
from classes.transaction_process_adapter import get_customers_with_negative_balance_func

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()


def x():
    pass


with DAG(
    dag_id="find_negative_closing_account_balance",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    # 
    @task(task_id="start")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "starting dag to find negative closing account balance"

    start_tsk = print_context()
	

	#
    @task(task_id="remove_duplicate_transactions_task")
    def remove_duplicate_transactions(**kwargs):
        transactions = pd.read_csv('/home/ubuntu/data/transactions.csv')
        clean_transactions=remove_duplicate_transactions_func(transactions)
        clean_transactions.to_csv('/home/ubuntu/data/clean_transactions.csv')
		
	#
    @task(task_id="remove_duplicate_accounts_task")
    def remove_duplicate_accounts(**kwargs):
        accounts = pd.read_csv('/home/ubuntu/data/accounts.csv')
        clean_accounts=remove_duplicate_accounts_func(accounts)
        clean_accounts.to_csv('/home/ubuntu/data/clean_accounts.csv')
		
	#
    @task(task_id="calculate_received_amount_per_account_task")
    def calculate_received_amount_per_account(**kwargs):
        clean_transactions = pd.read_csv('/home/ubuntu/data/clean_transactions.csv')
        received_amount_per_account=calculate_received_amount_per_account_func(clean_transactions)
        received_amount_per_account.to_csv('/home/ubuntu/data/received_amount_per_account.csv')

   
	#
    @task(task_id="calculate_sent_amount_per_account_task")
    def calculate_sent_amount_per_account(**kwargs):
        clean_transactions = pd.read_csv('/home/ubuntu/data/clean_transactions.csv')
        sent_amount_per_account=calculate_sent_amount_per_account_func(clean_transactions)
        sent_amount_per_account.to_csv('/home/ubuntu/data/sent_amount_per_account.csv')
		
	#
    @task(task_id="calculate_balance_end_of_day_per_account_task")
    def calculate_balance_end_of_day_per_account(**kwargs):
        clean_account_balance = pd.read_csv('/home/ubuntu/data/clean_accounts.csv')
        received_amount_per_account = pd.read_csv('/home/ubuntu/data/received_amount_per_account.csv')
        sent_amount_per_account = pd.read_csv('/home/ubuntu/data/sent_amount_per_account.csv')
        day_end_balance = calculate_balance_end_of_day_per_account_func(clean_account_balance, received_amount_per_account, sent_amount_per_account)
        day_end_balance.to_csv('/home/ubuntu/data/day_end_balance.csv')
		
	#
    @task(task_id="get_customers_with_negative_balance_task")
    def get_customers_with_negative_balance(**kwargs):
        day_end_balance = pd.read_csv('/home/ubuntu/data/day_end_balance.csv')
        accounts_with_negative_balance = get_customers_with_negative_balance_func(day_end_balance)
        accounts_with_negative_balance = accounts_with_negative_balance[['account_number','Final_Euro']]
        accounts_with_negative_balance.to_csv('/home/ubuntu/data/accounts_with_negative_balance.csv')
        print(accounts_with_negative_balance)
		
	#
    notify_email_support_team = EmailOperator( 
				task_id='send_email_support_team', 
				to='test@gmail.com', 
				subject='ingestion complete', 
				html_content="Hi test, Daily ETL process has been successfully completed for negative closing account balance.")
    
    #
    notify_email_business_stackholder = EmailOperator( 
				task_id='send_email_business_stackholder',
				to='test@gmail.com', 
				subject='Negative closing account balance', 
				html_content="Hello stackholder, your negative closing account balance are attached in the csv file.",
                files=['/home/ubuntu/data/accounts_with_negative_balance.csv'])	
	
    
    remove_duplicate_transactions_tsk = remove_duplicate_transactions()
    remove_duplicate_accounts_tsk = remove_duplicate_accounts()
    calculate_received_amount_per_account_tsk = calculate_received_amount_per_account()
    calculate_sent_amount_per_account_tsk = calculate_sent_amount_per_account()
    calculate_balance_end_of_day_per_account_tsk = calculate_balance_end_of_day_per_account()
    get_customers_with_negative_balance_tsk = get_customers_with_negative_balance()
    
    start_tsk >> [remove_duplicate_transactions_tsk, remove_duplicate_accounts_tsk]
    remove_duplicate_transactions_tsk >> [calculate_received_amount_per_account_tsk, calculate_sent_amount_per_account_tsk]
    remove_duplicate_accounts_tsk >> calculate_balance_end_of_day_per_account_tsk
    calculate_received_amount_per_account_tsk >> calculate_balance_end_of_day_per_account_tsk
    calculate_sent_amount_per_account_tsk >> calculate_balance_end_of_day_per_account_tsk
    calculate_balance_end_of_day_per_account_tsk >> get_customers_with_negative_balance_tsk >> [notify_email_support_team, notify_email_business_stackholder]
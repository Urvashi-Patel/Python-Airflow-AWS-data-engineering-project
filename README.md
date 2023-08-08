# Python-Airflow-AWS-data-engineering-project
## Project Title: Find Negative Closing Account Balance using Airflow - Data Engineering Project

## Project Description: 
In this project, we are using two datafiles accounts.csv and transactions.csv. We are trying to find negative closing account balance on daily basis using Airflow with AWS cloud platform.

## Project Workflow:

![Project_Workflow](https://github.com/Urvashi-Patel/Python-Airflow-AWS-data-engineering-project-/assets/34763147/0063a7b3-e6e3-4b0a-baa5-203d0b0c6241)

## Tech Stack:
➔ Language: Python3
➔ Services: Apache Airflow, Amazon EC2, AWS S3

## Implementation Steps:

**step1:** for this project, First to colne the git repository.
**step2:** After cloning the Github repository we have to launch EC2 instance on AWS.
**step3:** Install below command on new EC2 instance.
  ➔ sudo apt-get update
	➔ sudo apt install python3-pip
	➔ sudo pip install apache-airflow
	➔ sudo pip install pandas
	➔ sudo pip install s3fs
	➔ sudo pip install kubernetes

**step4:** create the dags folder in the airflow and classes folder in the dags. Also, create data folder on the same path of airflow.
**step5:** Copy your local files to EC2 instance using below commands.
  ➔ scp -i airflow_ec2_key.pem my_class.py ubuntu@ec2-52-23-208-132.compute-1.amazonaws.com:airflow/dags/classes/
  ➔ scp -i airflow_ec2_key.pem my_first_dags.py ubuntu@ec2-52-23-208-132.compute-1.amazonaws.com:airflow/dags/
  ➔ scp -i airflow_ec2_key.pem accounts.csv ubuntu@ec2-52-23-208-132.compute-1.amazonaws.com:airflow/dags/data/
  ➔ scp -i airflow_ec2_key.pem transactions.csv ubuntu@ec2-52-23-208-132.compute-1.amazonaws.com:airflow/dags/data/

**step6:** Run the command to start the airflow
   ➔ airflow db reset
	 ➔ airflow db init
	 ➔ airflow users create --username test --firstname test --lastname 123 --password test1234 --role Admin --email test@gmail.com
	 ➔ airflow standalone

**step7:** Trigger the dag and run it, get the email when dags run successfully.

## Email Configuration steps:

➔ Change the airflow.cfg file with below commands and add your configure email id and password:

smtp_host = smtp.gmail.com 
smtp_starttls = True 
smtp_ssl = False 
smtp_user = *********
smtp_password = ********** 
smtp_port = 587 
smtp_mail_from = ***********

## Airflow Dag:

![Airflow_local_dag](https://github.com/Urvashi-Patel/Python-Airflow-AWS-data-engineering-project-/assets/34763147/3377a8d2-150d-4bd7-a6c7-9512f73934a8)

## Email Functionality:

![email_snap](https://github.com/Urvashi-Patel/Python-Airflow-AWS-data-engineering-project-/assets/34763147/44601c38-c40c-4979-b8fb-c1ee4eba4c26)


#!/usr/bin/env python
# coding: utf-8

# In[5]:


#!pip install google-auth


# In[1]:


import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery


# In[2]:


def load_departments(csv_path, column_names):
    return pd.read_csv(csv_path, names=column_names)

def load_jobs(csv_path, column_names):
    return pd.read_csv(csv_path, names=column_names)

def load_employees(csv_path, column_names):
    """Load jobs CSV into a DataFrame."""
    return pd.read_csv(csv_path, names=column_names)


# In[3]:


def process_employee(employee_df):
    """Process the employee DataFrame by formatting the datetime column."""
    # Convert datetime column to string for processing (if needed)
    employee_df['datetime'] = employee_df['datetime'].astype(str)
    # Truncate time
    employee_df['datetime'] = employee_df['datetime'].apply(lambda x: x[0:10])
    # Convert string dates to datetime objects
    employee_df['datetime'] = pd.to_datetime(employee_df['datetime'])
    return employee_df

def load_to_bigquery(df, destination_table, table_schema, credentials, if_exists='replace'):
    """Load DataFrame to BigQuery using the pandas to_gbq method."""
    # Here we assume to_gbq is imported from pandas_gbq
    # (or you could call df.to_gbq if that's how you’re using it)
    df.to_gbq(
        destination_table=destination_table,
        project_id=credentials.project_id,
        if_exists=if_exists,
        table_schema=table_schema,
        credentials=credentials
    )


# In[4]:


if __name__ == '__main__':
    department_csv = 'https://raw.githubusercontent.com/lucianosantosg3/Globlant_Challange/refs/heads/main/departments.csv'
    job_csv = 'https://raw.githubusercontent.com/lucianosantosg3/Globlant_Challange/refs/heads/main/jobs.csv'
    employee_csv = 'https://raw.githubusercontent.com/lucianosantosg3/Globlant_Challange/refs/heads/main/hired_employees.csv'
    
    # Read CSVs
    department = load_departments(department_csv, ['id','department'])
    job = load_jobs(job_csv, ['id','job'])
    employee = load_employees(employee_csv,['id','name','datetime','department_id','job_id'])
    employee = process_employee(employee)
    
   # Credentials:
    credentials = service_account.Credentials.from_service_account_file(
        'C:\\Users\\lucia\\Documents\\GBQ.json',
        scopes=['https://www.googleapis.com/auth/bigquery']
    )
    
    table_schema = [
    {'name': 'id', 'type': 'INTEGER'},
    {'name': 'department', 'type': 'STRING'}  # Explicitly setting as DATE
    ]

    department.to_gbq(credentials=credentials,
                                 destination_table='dataset.department',
                                 if_exists='replace',
                                 table_schema=table_schema)

    table_schema = [
    {'name': 'id', 'type': 'INTEGER'},
    {'name': 'name', 'type': 'STRING'},
    {'name': 'datetime', 'type': 'DATE'},
    {'name': 'department_id', 'type': 'INTEGER'},
    {'name': 'job_id', 'type': 'INTEGER'}
    ]

    employee.to_gbq(credentials=credentials,
                                 destination_table='dataset.employee',
                                 if_exists='replace',
                                 table_schema=table_schema)

    table_schema = [
    {'name': 'id', 'type': 'INTEGER'},
    {'name': 'job', 'type': 'STRING'}  # Explicitly setting as DATE
    ]

    job.to_gbq(credentials=credentials,
                                 destination_table='dataset.job',
                                 if_exists='replace',
                                  table_schema=table_schema)


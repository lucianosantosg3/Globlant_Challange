#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import pytest
from io import StringIO
from unittest.mock import patch, MagicMock
from BigQueryAPI import load_departments, load_jobs, load_employees, process_employee, load_to_bigquery


# In[5]:


# Test load_departments using a StringIO object to simulate a CSV file
def test_load_departments(tmp_path):
    # Create a temporary CSV file
    csv_content = "1,Product Management\n2,Marketing"
    file_path = tmp_path / "departments.csv"
    file_path.write_text(csv_content)
    
    df = load_departments(str(file_path), ['id', 'department'])
    # Check DataFrame shape and values
    assert df.shape == (2, 2)
    assert df.iloc[0]['department'] == "Product Management"



# In[6]:


# Test load_departments using a StringIO object to simulate a CSV file
def test_load_jobs(tmp_path):
    # Create a temporary CSV file
    csv_content = "1,Marketing Assistant\n2,VP Sales"
    file_path = tmp_path / "jobs.csv"
    file_path.write_text(csv_content)
    
    df = load_departments(str(file_path), ['id', 'job'])
    # Check DataFrame shape and values
    assert df.shape == (2, 2)
    assert df.iloc[0]['job'] == "Marketing Assistant"


# In[7]:


# Test process_employee function
def test_process_employee():
    # Create a sample DataFrame similar to what you expect for employee data
    data = {
        'datetime': ['2021-11-07T02:48:42Z', '2021-12-08T03:50:00Z']
    }
    df = pd.DataFrame(data)
    processed_df = process_employee(df)
    
    # Check that the datetime is truncated to 'YYYY-MM-DD'
    assert processed_df['datetime'].iloc[0].strftime('%Y-%m-%d') == '2021-11-07'
    assert processed_df['datetime'].iloc[1].strftime('%Y-%m-%d') == '2021-12-08'



# In[8]:


# Test load_to_bigquery function by mocking the to_gbq method
@patch('BigQueryAPI.pd.DataFrame.to_gbq')
def test_load_to_bigquery(mock_to_gbq):
    # Create a sample DataFrame
    df = pd.DataFrame({'id': [1, 2], 'department': ['A', 'B']})
    
    # Create a fake credentials object with a dummy project_id attribute
    fake_credentials = MagicMock()
    fake_credentials.project_id = 'fake_project'
    
    table_schema = [
        {'name': 'id', 'type': 'INTEGER'},
        {'name': 'department', 'type': 'STRING'}
    ]
    
    # Call the function under test
    load_to_bigquery(df, destination_table='dataset.department', table_schema=table_schema, credentials=fake_credentials)
    
    # Assert that the to_gbq function was called with the expected parameters
    mock_to_gbq.assert_called_once()
    args, kwargs = mock_to_gbq.call_args
    assert kwargs['destination_table'] == 'dataset.department'
    assert kwargs['if_exists'] == 'replace'
    assert kwargs['table_schema'] == table_schema
    # Check that credentials and project_id are properly passed
    assert kwargs['credentials'] == fake_credentials
    assert kwargs['project_id'] == 'fake_project'


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





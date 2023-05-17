from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
import pandas as pd
import glob

file = "/home/airflow/data/raw_data.csv"
raw = pd.read_csv(file)
to_df = pd.DataFrame(raw)

# Remove Special Characters
to_df['First_name'] = to_df['First_name'].str.replace('\W', '', regex=True)
to_df['Last_name'] = to_df['Last_name'].str.replace('\W', '', regex=True)
#to_df['Age'] = to_df['Age'].str.replace('\W', '', regex=True)
to_df['Sex'] = to_df['Sex'].str.replace('\W', '', regex=True)

# Remove whitespace
to_df['First_name'] = to_df['First_name'].str.replace(' ','', regex=True)
to_df['Last_name'] = to_df['Last_name'].str.replace(' ', '', regex=True)
#to_df['Age'] = to_df['Age'].str.replace(' ', '', regex=True)
to_df['Sex'] = to_df['Sex'].str.replace(' ', '', regex=True)

# Capitalize the First Character 
to_df['First_name'] = to_df['First_name'].str.lower()
to_df['Last_name'] = to_df['Last_name'].str.lower()
to_df['First_name'] = to_df['First_name'].str.title()
to_df['Last_name'] = to_df['Last_name'].str.title()

# Cleansing Age colunms
to_df['Age'] = pd.to_numeric(to_df['Age'],errors='coerce')
# Drop rows with non-numeric 'Age' values
to_df = to_df.dropna(subset=['Age'])
# Convert 'Age' column to integers
to_df['Age'] = to_df['Age'].astype('int')

# Cleansing Sex colunms
to_df['Sex'] = to_df['Sex'].replace(['Female','Girl','f','Man','Male','FM','MF','both','m',''],\
['F','F','F','M','M','LGBT','LGBT','LGBT','M','Not Defined'])
#print("======Clean Successfully======")
to_df.to_csv("/home/airflow/data/Transform_data.csv", index=False)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
import pandas as pd
import glob


paths = "/home/airflow/data/sorce/"
all_files = glob.glob(paths + "*.csv")
all_df = pd.DataFrame(columns=['First_name','Last_name','Age','Sex'])
#print(all_df)
for file in all_files:
#print(file) 
    raw = pd.read_csv(file,sep='[|]',engine='python')
    to_df =  pd.DataFrame(raw)
    to_df = to_df.rename(columns={"First Name": "First_name", "Last Name": "Last_name"})
    #print(to_df)
    all_df = pd.concat([all_df,to_df], axis=0)
#print(all_df)
all_df = all_df.fillna(value="Null")
#print(all_df)
all_df.to_csv("/home/airflow/data/raw_data.csv", index=False)
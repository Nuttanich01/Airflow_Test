# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago 
# import pandas as pd
# import glob

# def extract():
#     paths = "/home/airflow/data/sorce/"
#     all_files = glob.glob(paths + "*.csv")
#     all_df = pd.DataFrame(columns=['First_name','Last_name','Age','Sex'])
#     #print(all_df)
#     for file in all_files:
#         #print(file) 
#         raw = pd.read_csv(file,sep='[|]',engine='python')
#         to_df =  pd.DataFrame(raw)
#         to_df = to_df.rename(columns={"First Name": "First_name", "Last Name": "Last_name"})
#         #print(to_df)
#         all_df = pd.concat([all_df,to_df], axis=0)
#         #print(all_df)
#     all_df = all_df.fillna(value="Null")
#     #print(all_df)
#     all_df.to_csv("/home/airflow/data/raw_data.csv", index=False)

# def Transform():
#     file = "/home/airflow/data/raw_data.csv"
#     raw = pd.read_csv(file)
#     to_df = pd.DataFrame(raw)

#     # Remove Special Characters
#     to_df['First_name'] = to_df['First_name'].str.replace('\W', '', regex=True)
#     to_df['Last_name'] = to_df['Last_name'].str.replace('\W', '', regex=True)
#     #to_df['Age'] = to_df['Age'].str.replace('\W', '', regex=True)
#     to_df['Sex'] = to_df['Sex'].str.replace('\W', '', regex=True)

#     # Remove whitespace
#     to_df['First_name'] = to_df['First_name'].str.replace(' ','', regex=True)
#     to_df['Last_name'] = to_df['Last_name'].str.replace(' ', '', regex=True)
#     #to_df['Age'] = to_df['Age'].str.replace(' ', '', regex=True)
#     to_df['Sex'] = to_df['Sex'].str.replace(' ', '', regex=True)

#     # Capitalize the First Character 
#     to_df['First_name'] = to_df['First_name'].str.lower()
#     to_df['Last_name'] = to_df['Last_name'].str.lower()
#     to_df['First_name'] = to_df['First_name'].str.title()
#     to_df['Last_name'] = to_df['Last_name'].str.title()

#     # Cleansing Age colunms
#     to_df['Age'] = pd.to_numeric(to_df['Age'],errors='coerce')
#     # Drop rows with non-numeric 'Age' values
#     to_df = to_df.dropna(subset=['Age'])
#     # Convert 'Age' column to integers
#     to_df['Age'] = to_df['Age'].astype('int')

#     # Cleansing Sex colunms
#     to_df['Sex'] = to_df['Sex'].replace(['Female','Girl','f','Man','Male','FM','MF','both','m',''],\
#                                         ['F','F','F','M','M','LGBT','LGBT','LGBT','M','Not Defined'])
#     #print("======Clean Successfully======")
#     to_df.to_csv("/home/airflow/data/Transform_data.csv", index=False)

# def load():
#     file = "/home/airflow/data/raw_data.csv"
#     raw = pd.read_csv(file)
#     to_df = pd.DataFrame(raw)
#     to_df.to_csv("/home/airflow/data/Real_data.csv", index=False)

# with DAG(
#         dag_id='Test_airflow',
#         start_date=days_ago(0),
#         schedule_interval="@daily",
#         tags=["Test"]
# ) as dag:
    
#     task1 = PythonOperator(
#     task_id='extract',
#     python_callable=extract,
#     dag=dag,
# )
#     task2 = PythonOperator(
#     task_id='Transform',
#     python_callable=Transform,
#     dag=dag,
# )
#     task3 = PythonOperator(
#     task_id='Load',
#     python_callable=load,
#     dag=dag,
# )

#     task1 >> task2 >> task3



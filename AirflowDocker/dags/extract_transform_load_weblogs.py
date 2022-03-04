from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sqlalchemy as db
import pandas as pd
import re


chunkSize = 1000


def fetch_insert_Target_weblogs():
    engineTest = db.create_engine(
        'mysql+mysqlconnector://airflow:airflow@host.docker.internal:3306/WebServer', echo=False)
    conn = engineTest.connect()
    metadata = db.MetaData()
    dbTable = db.Table('weblogs', metadata,
                       autoload=True, autoload_with=engineTest)
    query = db.select([dbTable])
    dbTableProxy = conn.execution_options(stream_results=True).execute(query)
    while 'fetched data is not empty':

        dbTableData = dbTableProxy.fetchmany(chunkSize)

        if not dbTableData:
            break

        colHeaders = conn.execute(query).keys()
        colHeaders.pop(0)  # get rid of ID column
        df = pd.DataFrame(dbTableData)
        df = df.drop(0, 1)
        #df.columns = colHeaders

        liWeblog = []
        regex = '([(\d\.)]+) (.*) (.*) \[(.*?)\] "(.*?)" (\d+) (.*) "(.*?)" "(.*?)"'
        for row in df.iterrows():
            weblog = list(re.match(regex, row[1][1]).groups())
            liWeblog.append([weblog[0], weblog[2], weblog[3], weblog[8]])
        parsedDf = pd.DataFrame(liWeblog)
        parsedDf.columns = ['clientIP', 'username', 'time', 'userAgent']
        # Insert data into target database
        engineTestTarget = db.create_engine(
            'mysql+mysqlconnector://airflow:airflow@host.docker.internal:3306/Target', echo=False)
        parsedDf.to_sql(con=engineTestTarget, name='weblogs',
                        if_exists='append', index=False, chunksize=chunkSize, method='multi')


default_args = {"owner": "airflow", "start_date": datetime(2022, 3, 2)}
with DAG(dag_id="extract_transform_load_weblogs", default_args=default_args) as dag:

    fetch_insert_Target_weblogs = PythonOperator(
        task_id='fetch_insert_Target_weblogs',
        python_callable=fetch_insert_Target_weblogs
    )

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import sqlalchemy as db


# Define parameter
chunkSize = 1000


def fetch_insert_B2B_data(**kwargs):

    # Fetch data from source database
    tableName = kwargs['tableName']

    engineTest = db.create_engine(
        'mysql+mysqlconnector://airflow:airflow@host.docker.internal:3306/test', echo=False)
    conn = engineTest.connect()
    metadata = db.MetaData()
    dbTable = db.Table(tableName, metadata,
                       autoload=True, autoload_with=engineTest)
    query = db.select([dbTable])
    dbTableProxy = conn.execution_options(stream_results=True).execute(query)
    while 'fetched data is not empty':

        #dbTableData = dbTableProxy.fetchall()
        dbTableData = dbTableProxy.fetchmany(chunkSize)

        if not dbTableData:
            break

        colHeaders = conn.execute(query).keys()
        colHeaders.pop(0)  # get rid of ID column
        df = pd.DataFrame(dbTableData)
        df = df.drop(0, 1)
        df.columns = colHeaders

        # Insert data into target database
        engineTestTarget = db.create_engine(
            'mysql+mysqlconnector://airflow:airflow@host.docker.internal:3306/testTarget', echo=False)
        df.to_sql(con=engineTestTarget, name=tableName,
                  if_exists='append', index=False, chunksize=chunkSize, method='multi')


default_args = {"owner": "airflow", "start_date": datetime(2022, 3, 1)}
with DAG(dag_id="extract_load_B2B_data", default_args=default_args) as dag:

    fetch_insert_B2B_dimCustomer = PythonOperator(
        task_id='fetch_insert_B2B_dimCustomer',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'dimCustomer'}
    )

    fetch_insert_B2B_dimProduct = PythonOperator(
        task_id='fetch_insert_B2B_dimProduct',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'dimProduct'}
    )

    fetch_insert_B2B_dimSupplier = PythonOperator(
        task_id='fetch_insert_B2B_dimSupplier',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'dimSupplier'}
    )

    fetch_insert_B2B_dimCompany = PythonOperator(
        task_id='fetch_insert_B2B_dimCompany',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'dimCompany'}
    )

    fetch_insert_B2B_factSupplierProductPrice = PythonOperator(
        task_id='fetch_insert_B2B_factSupplierProductPrice',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'factSupplierProductPrice'}
    )

    fetch_insert_B2B_factCompanyProductPrice = PythonOperator(
        task_id='fetch_insert_B2B_factCompanyProductPrice',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'factCompanyProductPrice'}
    )

    fetch_insert_B2B_factOrder = PythonOperator(
        task_id='fetch_insert_B2B_factOrder',
        provide_context=True,
        python_callable=fetch_insert_B2B_data,
        op_kwargs={'tableName': 'factOrder'}
    )

    fetch_insert_B2B_dimCustomer >> fetch_insert_B2B_dimProduct >> fetch_insert_B2B_dimCompany >> fetch_insert_B2B_dimSupplier \
        >> fetch_insert_B2B_factSupplierProductPrice >> fetch_insert_B2B_factCompanyProductPrice >> fetch_insert_B2B_factOrder

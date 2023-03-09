# This is a sample Python script.
import io
import pandas
import requests
import zipfile
import numpy
import time
import sqlalchemy
from sqlalchemy import create_engine, text


# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

# Data Sources:
url1 = "https://data.gov.ua/dataset/0ffd8b75-0628-48cc-952a-9302f9799ec0/resource/a7345876-fdd0-4e22-af6a-fc7a7b80acce/download/tz_opendata_z01012022_po01092022.zip"
url2 = "https://data.gov.ua/dataset/0ffd8b75-0628-48cc-952a-9302f9799ec0/resource/c5cb530d-0533-40be-b9ad-f03e06c94b10/download/tz_opendata_z01012021_po01012022.zip"
url3 = "https://data.gov.ua/dataset/0ffd8b75-0628-48cc-952a-9302f9799ec0/resource/ebeb92fe-424c-41d1-aacf-288e91049dc9/download/tz_opendata_z01012020_po01012021.zip"
datasource = [url1, url2, url3]

pandas.options.display.width = 0

# Establishing connection with our database
engine = create_engine(
    "mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 18 for SQL Server};Server={your_server}};Database={your_database}};UID={your_user_id}};PWD={your_password}};TrustServerCertificate=yes;")
# Optional parameter: fast_executemany=True
# connection = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};Server=localhost;Database=Reestr;UID=SA;PWD=reallyStrongPwd123;TrustServerCertificate=yes;")


def time_decorator(function):
    def inner_decorator(*args, **kwargs):
        start_time = time.time()
        counter = function(*args, **kwargs)
        end_time = time.time()
        print(f'Total time for {function.__name__} - ' + str(end_time - start_time))
        if counter:
            print(f'Total processed rows per second ratio - ' + str(counter / (end_time - start_time)))
    return inner_decorator


# Extraction
@time_decorator
def extract():
    for data_link in datasource:
        response = requests.get(data_link, stream=True)
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        zip_file.extractall()


# Engine validation
@time_decorator
def validate_engine():
    # Check whether engine works properly
    try:
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
    except Exception as e:
        print(f'Engine invalid: {str(e)}')


# Consolidating data in the manually-created staging area
def staging_area_load(number_of_rows):

    validate_engine()

    # For each dataset insert data into staging area
    for link in datasource:

        # Dynamically generating csv name
        csv_name = link.split('/')[::-1][0].replace(".zip", ".csv")
        chunk_size = number_of_rows // 10
        iteration_number = 0

        for chunk in pandas.read_csv(csv_name, sep=";", low_memory=False, chunksize=chunk_size):

            if iteration_number == 10:
                break

            iteration_number += 1

            # Fixing VIN anomaly in 2020 register
            if 'VIN' not in chunk.columns:
                chunk['VIN'] = numpy.nan

            # Fix '.0' anomaly
            chunk = chunk.apply(lambda x: x.replace({'.0': ''}))

            # Fixing localization anomalies
            chunk = chunk.replace("'", "", regex=True)
            chunk = chunk.replace(",", ".", regex=True)


            # Converting data to the proper format
            chunk['D_REG'] = pandas.to_datetime(chunk['D_REG'])
            chunk['D_REG'] = chunk['D_REG'].dt.strftime('%Y-%m-%d')

            # Inserting data
            print(f'Insert chunk number {iteration_number}')
            start = time.time()
            chunk.to_sql('reestr', engine, if_exists='append', index=False, schema='stg', chunksize=chunk_size, dtype={
                'PERSON': sqlalchemy.VARCHAR(length=1),
                'REG_ADDR_KOATUU': sqlalchemy.VARCHAR(length=30),
                'OPER_CODE': sqlalchemy.VARCHAR(length=3),
                'OPER_NAME': sqlalchemy.NVARCHAR(length=200),
                'D_REG': sqlalchemy.DATE,
                'DEP_CODE': sqlalchemy.INTEGER,
                'DEP': sqlalchemy.NVARCHAR(length=300),
                'BRAND': sqlalchemy.NVARCHAR(length=300),
                'MODEL': sqlalchemy.NVARCHAR(length=300),
                'VIN': sqlalchemy.NVARCHAR(length=50),
                'MAKE_YEAR': sqlalchemy.INTEGER,
                'COLOR': sqlalchemy.NVARCHAR(length=50),
                'KIND': sqlalchemy.NVARCHAR(length=50),
                'BODY': sqlalchemy.NVARCHAR(length=50),
                'PURPOSE': sqlalchemy.NVARCHAR(length=50),
                'FUEL': sqlalchemy.NVARCHAR(length=50),
                'CAPACITY': sqlalchemy.FLOAT,
                'OWN_WEIGHT': sqlalchemy.FLOAT,
                'TOTAL_WEIGHT': sqlalchemy.FLOAT,
                'N_REG_NEW': sqlalchemy.NVARCHAR(length=16)
            })
            print("Total time per chunk: " + str(time.time() - start))

# Transforming data
@time_decorator
def transform():
    star_schema_tables_with_columns = {
                                          'DimCarInfo': {
                                              'VIN': sqlalchemy.NVARCHAR(length=50),
                                              'CAPACITY': sqlalchemy.FLOAT,
                                              'OWN_WEIGHT': sqlalchemy.FLOAT,
                                              'TOTAL_WEIGHT': sqlalchemy.FLOAT,
                                              'MAKE_YEAR': sqlalchemy.INTEGER
                                          },
                                          'DimCarBrand': {
                                              'BRAND': sqlalchemy.NVARCHAR(length=300),
                                              'MODEL': sqlalchemy.NVARCHAR(length=300)
                                          },
                                          'DimCarColor': {
                                              'COLOR': sqlalchemy.NVARCHAR(length=50)
                                          },
                                          'DimCarKind': {
                                              'KIND': sqlalchemy.NVARCHAR(length=50)
                                          },
                                          'DimCarBody': {
                                              'BODY': sqlalchemy.NVARCHAR(length=50)
                                          },
                                          'DimCarPurpose': {
                                              'PURPOSE': sqlalchemy.NVARCHAR(length=50)
                                          },
                                          'DimCarFuel': {
                                              'FUEL': sqlalchemy.NVARCHAR(length=50)
                                          },
                                          'DimOperation': {
                                              'OPER_CODE': sqlalchemy.VARCHAR(length=3),
                                              'OPER_NAME': sqlalchemy.NVARCHAR(length=200)
                                          },
                                          'DimCustomer': {
                                              'PERSON': sqlalchemy.VARCHAR(length=1),
                                              'REG_ADDR_KOATUU': sqlalchemy.VARCHAR(length=30)
                                          },
                                          'DimDepartment': {
                                              'DEP_CODE': sqlalchemy.INTEGER,
                                              'DEP': sqlalchemy.NVARCHAR(length=300)
                                          },
                                          'DimDate': {
                                              'DAY': sqlalchemy.INTEGER,
                                              'MONTH': sqlalchemy.INTEGER,
                                              'YEAR': sqlalchemy.INTEGER
                                          },
                                          'MeasureCarProperties': {
                                              'CAR_INFO_ID': sqlalchemy.INTEGER,
                                              'CAR_BRAND_ID': sqlalchemy.INTEGER,
                                              'CAR_COLOR_ID': sqlalchemy.INTEGER,
                                              'CAR_KIND_ID': sqlalchemy.INTEGER,
                                              'CAR_BODY_ID': sqlalchemy.INTEGER,
                                              'CAR_PURPOSE_ID': sqlalchemy.INTEGER,
                                              'CAR_FUEL_ID': sqlalchemy.INTEGER,
                                              'OPERATION_ID': sqlalchemy.INTEGER,
                                              'CUSTOMER_ID': sqlalchemy.INTEGER,
                                              'DEP_ID': sqlalchemy.INTEGER,
                                              'DATE_ID': sqlalchemy.INTEGER,
                                              'N_REG_NEW': sqlalchemy.NVARCHAR(length=16)
                                          }
    }
    star_schema = 'star'
    staging_table = 'reestr'
    staging_schema = 'stg'
    for table, columns in star_schema_tables_with_columns.items():
        if table.__contains__('Dim') and not table.__contains__('Date'):
            with engine.connect() as connection:
                query = text('SELECT DISTINCT {0} FROM {1}'.format(', '.join(columns.keys()), staging_schema + '.' + staging_table))
                df = pandas.read_sql(query, connection)
                load(df, engine, table, star_schema, columns)
        elif table.__contains__('Date'):
            dates = pandas.Series(pandas.date_range('2012-01-01', '2040-12-31', freq='D'))
            dataframe = pandas.DataFrame({'DAY': dates.dt.day, 'MONTH': dates.dt.month, 'YEAR': dates.dt.year})
            load(dataframe, engine, table, star_schema, columns)
        else:
            with engine.connect() as connection:
                query = text(
                    '''
SELECT B.ID AS CAR_INFO_ID, C.ID AS CAR_BRAND_ID, 
    D.ID AS CAR_COLOR_ID, E.ID AS CAR_KIND_ID, F.ID AS CAR_BODY_ID, 
    G.ID AS CAR_PURPOSE_ID, H.ID AS CAR_FUEL_ID, I.ID AS OPERATION_ID, J.ID AS CUSTOMER_ID, K.ID AS DEP_ID, L.ID AS DATE_ID, A.N_REG_NEW
    FROM stg.reestr AS A 
        INNER JOIN star.DimCustomer AS J ON (A.PERSON = J.PERSON OR (A.PERSON IS NULL AND J.PERSON IS NULL)) AND (A.REG_ADDR_KOATUU = J.REG_ADDR_KOATUU OR (A.REG_ADDR_KOATUU IS NULL AND J.REG_ADDR_KOATUU IS NULL))
        INNER JOIN star.DimCarInfo AS B ON (A.VIN = B.VIN OR (A.VIN IS NULL AND B.VIN IS NULL)) AND (A.CAPACITY = B.CAPACITY OR (A.CAPACITY IS NULL AND B.CAPACITY IS NULL)) AND (A.OWN_WEIGHT = B.OWN_WEIGHT OR (A.OWN_WEIGHT IS NULL AND B.OWN_WEIGHT IS NULL)) AND (A.TOTAL_WEIGHT = B.TOTAL_WEIGHT OR (A.TOTAL_WEIGHT IS NULL AND B.TOTAL_WEIGHT IS NULL)) AND (A.MAKE_YEAR = B.MAKE_YEAR OR (A.MAKE_YEAR IS NULL AND B.MAKE_YEAR IS NULL))
        INNER JOIN star.DimCarBrand AS C ON (A.BRAND = C.BRAND OR (A.BRAND IS NULL AND C.BRAND IS NULL)) AND (A.MODEL = C.MODEL OR (A.MODEL IS NULL AND C.MODEL IS NULL))
        INNER JOIN star.DimCarColor AS D ON (A.COLOR = D.COLOR OR (A.COLOR IS NULL AND D.COLOR IS NULL))
        INNER JOIN star.DimCarKind AS E ON (A.KIND = E.KIND OR (A.KIND IS NULL AND E.KIND IS NULL))
        INNER JOIN star.DimCarBody AS F ON (A.BODY = F.BODY OR (A.BODY IS NULL AND F.BODY IS NULL))
        INNER JOIN star.DimCarPurpose AS G ON (A.PURPOSE = G.PURPOSE OR (A.PURPOSE IS NULL AND G.PURPOSE IS NULL))
        INNER JOIN star.DimCarFuel AS H ON (A.FUEL = H.FUEL OR (A.FUEL IS NULL AND H.FUEL IS NULL))
        INNER JOIN star.DimOperation AS I ON (A.OPER_NAME = I.OPER_NAME OR (A.OPER_NAME IS NULL AND I.OPER_NAME IS NULL)) AND (A.OPER_CODE = I.OPER_CODE OR (A.OPER_CODE IS NULL AND I.OPER_CODE IS NULL))
        INNER JOIN star.DimDepartment AS K ON (A.DEP = K.DEP OR (A.DEP IS NULL AND K.DEP IS NULL)) AND (A.DEP_CODE = K.DEP_CODE OR (A.DEP_CODE IS NULL AND K.DEP_CODE IS NULL))
        INNER JOIN star.DimDate AS L ON YEAR(A.D_REG) = L.YEAR AND MONTH(A.D_REG) = L.MONTH AND DAY(A.D_REG) = L.DAY
                        '''
                    )
                dataframe = pandas.read_sql(query, connection)
                load(dataframe, engine, table, star_schema, columns)


# Loading data in star schema
def load(df: pandas.DataFrame, engine, table: str, schema: str, columns):
    df.to_sql(table, engine, if_exists='append', index=False, schema=schema, chunksize=1000, dtype=columns)

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    extract()
    staging_area_load(2000000)
    transform()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
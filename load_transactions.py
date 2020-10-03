import sqlite3
import pandas as pd
import boto3
import datetime

def run():
    # Create a small python script/app that loads the data into a sqlite database.

    database_name = 'Postie.db'
    table_name = 'postie_transactions'
    bucket_name = 'postie-testing-assets'
    region = 'us-east-1'

    db_connection = connect_sqllite_database(database_name)
    db_cursor = db_connection.cursor()
    _drop_table(db_cursor, db_connection, table_name)
    create_transaction_table(db_cursor, db_connection, table_name)
    import_transactions_from_bucket(region, bucket_name, db_connection, table_name)
    
    db_connection.close()
     

def connect_sqllite_database(db_name):
    """Creates a connection obection to a local database file, or creates a 
    new one with the database name as `db_name`.
    """
    return sqlite3.connect(db_name)

def _drop_table(db_cursor, db_connection, table_name):
    """Drop table function should likely not happen in production. This is 
    reserved for development only.
    """
    db_cursor.execute(f"""DROP TABLE {table_name}""")
    db_connection.commit()


def create_transaction_table(db_cursor, db_connection, table_name):
    db_cursor.execute(f"""CREATE TABLE {table_name}
    ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
    , timestamp timestamp
    , website_id integer
    , customer_id integer
    , placeholder text
    , checkout_amount float
    , url text
    , file_name text
    , created_at text
    , UNIQUE(timestamp,website_id,customer_id,checkout_amount)
    )""")

    db_connection.commit()


def import_transactions_from_bucket(region, bucket_name, db_connection, table_name):
    s3 = boto3.resource('s3', region)
    bucket_name = 'postie-testing-assets'
    bucket = s3.Bucket(bucket_name)
    for file in bucket.objects.all():
        file_name = file.key
        df = pd.read_csv(f's3://{bucket_name}/{file_name}')
        df['file_name'] = file_name
        df['created_at'] = datetime.datetime.now()
        df = df.rename(columns={k:k.replace(' ', '') for k in df.columns})
        df.to_sql(name=table_name, con=db_connection, if_exists='replace')

if __name__ == "__main__":
    run()
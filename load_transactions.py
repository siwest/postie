import sqlite3
import pandas as pd
import boto3
import datetime
import config
import logging


def run():
    # Create a small python script/app that loads the data into a sqlite database.

    db_connection = connect_sqllite_database(config.database_name)
    db_cursor = db_connection.cursor()

    _drop_table(db_cursor, db_connection, config.table_name)
    create_transaction_table(db_cursor, db_connection, config.table_name)
    import_transactions_from_bucket(
        config.region, config.bucket_name, db_connection, config.table_name
    )

    disconnect_sqllite_database(db_connection)


def connect_sqllite_database(db_name):
    """Creates a connection obection to a local database file, or creates a
    new one with the database name as `db_name`.
    """
    logging.warning("Connecting to sqlite3 database")
    return sqlite3.connect(db_name)


def disconnect_sqllite_database(db_connection):
    """Closes a connection obection."""
    logging.warning("Closing connection to sqlite3 database")
    return db_connection.close()


def _drop_table(db_cursor, db_connection, table_name):
    """Drop table function should likely not happen in production. This is
    reserved for development only.
    """
    logging.warning("Dropping existing Transactions table if exists")
    db_cursor.execute(f"""DROP TABLE {table_name}""")
    db_connection.commit()


def create_transaction_table(db_cursor, db_connection, table_name):
    logging.warning("Creating Transactions table if not exists")
    db_cursor.execute(
        f"""CREATE TABLE {table_name}
    ( id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT
    , "index" integer
    , app_version text
    , timestamp timestamp
    , website_id integer
    , customer_id integer
    , placeholder text
    , checkout_amount float
    , url text
    , file_name text
    , created_at text
    , UNIQUE(timestamp
        , website_id
        , customer_id
        , checkout_amount
        , placeholder)
    )"""
    )

    db_connection.commit()


def import_transactions_from_bucket(region, bucket_name, db_connection, table_name):
    logging.warning(f"Importing new Transactions available in bucket {bucket_name}")
    s3 = boto3.resource("s3", region)
    bucket = s3.Bucket(bucket_name)
    for file in bucket.objects.all():
        file_name = file.key
        logging.warning(f"Importing transaction data from S3 {bucket_name}/{file_name}")
        df = pd.read_csv(f"s3://{bucket_name}/{file_name}")
        df["file_name"] = file_name
        df["created_at"] = datetime.datetime.now()
        df = df.rename(columns={k: k.replace(" ", "") for k in df.columns})
        df.to_sql(name=table_name, con=db_connection, if_exists="append")


if __name__ == "__main__":
    run()

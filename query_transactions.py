import sqlite3
import click
import datetime
import locale
import config
import logging


@click.command()
@click.option("-s", "--start", default=None, help="Start date for pulling transactions")
@click.option("-e", "--end", default=None, help="End date for pulling transactions")
def run(start, end):
    # If no end date specified, use current date as end date
    # If no start date specified, set start date to 5 years before end date
    if not end:
        end_date = datetime.datetime.now()
    else:
        end_date = end
    if not start:
        start_date = end_date - datetime.timedelta(days=365 * 5)
    else:
        start_date = start

    db_connection = connect_sqllite_database(config.database_name)
    db_cursor = db_connection.cursor()

    total_sales = (
        get_total_sales(db_cursor, config.table_name, start_date, end_date)[0] or 0
    )

    locale.setlocale(locale.LC_ALL, "")
    money = locale.currency(total_sales, grouping=True)

    print(f"\nTotal sales from {start_date} to {end_date} is {money}\n")

    db_connection.close()


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


def get_total_sales(db_cursor, table_name, start_date, end_date):
    return db_cursor.execute(
        f"""
        SELECT SUM(checkout_amount) as sales
        FROM {table_name}
        WHERE DATE(timestamp) >= DATE('{start_date}')
          AND DATE(timestamp) <= DATE('{end_date}')
    """
    ).fetchone()


if __name__ == "__main__":
    run()

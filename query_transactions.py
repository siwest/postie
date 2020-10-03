import sqlite3

def run():

	start_date = '2017-01-01'
	end_date = '2020-01-01'
	database_name = 'Postie.db'
	db_connection = connect_sqllite_database(database_name)
	db_cursor = db_connection.cursor()
	table_name = 'postie_transactions'

	total_sales = get_total_sales(db_cursor, table_name, start_date, end_date)[0]
	print(f'Total sales from {start_date} to {end_date} is ${total_sales}')

def connect_sqllite_database(db_name):
    """Creates a connection obection to a local database file, or creates a 
    new one with the database name as `db_name`.
    """
    return sqlite3.connect(db_name)


def get_total_sales(db_cursor, table_name, start_date, end_date):
    return db_cursor.execute(f'''
        SELECT SUM(checkout_amount) as sales
        FROM {table_name}
        WHERE DATE(timestamp) >= DATE('{start_date}')
          AND DATE(timestamp) <= DATE('{end_date}')
    ''').fetchone()

if __name__ == "__main__":
    run()
from run_backup_database import (create_database_connection)
import psycopg2

## commonly used functions for query and update
def get_records_for_query(query):
#	print(query)
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	cursor.execute(query)
#	columns = list(cursor.description)
	records = cursor.fetchall()
	dataverse_db_connection.close()
	return records

def sql_update(query, params):
	print("updating database: "+(query%params))
	dataverse_db_connection = create_database_connection()
	cursor = dataverse_db_connection.cursor()
	cursor.execute(query, params)
	dataverse_db_connection.commit() 
	dataverse_db_connection.close()

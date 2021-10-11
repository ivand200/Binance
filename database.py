from db import USER, PASSWORD, HOST, PORT, DATABASE
import psycopg2

connection = psycopg2.connect(user=USER,
                              password=PASSWORD,
                              host=HOST,
                              port=PORT,
                              database=DATABASE)
cursor = connection.cursor()
print("PostgreSQL")
print(connection.get_dsn_parameters(), "\n")
#cursor.close()
#connection.close()

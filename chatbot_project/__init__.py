import pymysql
pymysql.install_as_MySQLdb()


from django.db import connection
with connection.cursor() as cursor:
    cursor.execute("SELECT VERSION()")
    db_version = cursor.fetchone()
    print(f"Database version: {db_version[0]}")
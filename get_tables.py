"""This service allows to get all table names from database"""
import MySQLdb
from rq import Worker, Queue, Connection
from methods.connection import get_redis, get_cursor

r = get_redis()


def get_tables():
    """Returns new tasks from databse (table tasks)"""
    cursor, db = get_cursor()
    if not cursor or not db:
        # log that failed getting cursor
        return False
    try:
        cursor.execute("select table_name from information_schema.tables where TABLE_SCHEMA='youpar';")
    except MySQLdb.Error as error:
        print(error)
        # Log
        return False
        # sys.exit("Error:Failed getting new tasks from database")
    data = cursor.fetchall()
    cursor.close()
    return data


if __name__ == '__main__':
    q = Queue('get_tables', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r, name='get_tables')
        worker.work()

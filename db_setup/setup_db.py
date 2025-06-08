import os
import requests
import argparse
import traceback
from dotenv import load_dotenv


def run_sql(connection: psycopg2.extensions.connection, sql: str) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        connection.commit()
        return True
    except:
        traceback.print_exc()
        return False


def setup_stack():
    pass

def delete_stack():
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', help='mode create or delete', default='create')
    args = parser.parse_args()
    if not args.mode in ['create', 'delete']:
        print('Invalid mode')
        exit(1)

    load_dotenv()

    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        dbname=os.environ['POSTGRES_DB']
    )

    if args.mode == 'create':
        standup_stack(conn, topics)
    elif args.mode == 'delete':
        delete_stack(conn, topics)
    else:
        print('Invalid mode')
        exit(1)

    print("done")
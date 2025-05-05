import os
import yaml
import requests
import psycopg2
import argparse
import traceback
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic


def create_topic(topic: str, broker: str) -> bool:
    admin_client = KafkaAdminClient(
        bootstrap_servers=broker,
        client_id='topic-creator'
    )

    new_topic = NewTopic(
        name=topic,
        num_partitions=1,
        replication_factor=1
    )
    try:
        # return admin_client.create_topics([new_topic])
        admin_client.create_topics([new_topic], validate_only=False)
        return True
    except:
        return False


def run_sql(connection: psycopg2.extensions.connection, sql: str) -> bool:
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        connection.commit()
        return True
    except:
        traceback.print_exc()
        return False


def register_connector(topic_name: str):
    url = f'http://{os.environ["KAFKA_CONNECT"]}/connectors'
    data = {
        "name": f"{topic_name}-connector",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topic_name,
            "connection.url": f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}?user={os.environ['POSTGRES_USER']}&password={os.environ['POSTGRES_PASSWORD']}",
            "auto.create": "false",
            "auto.evolve": "false",
            "insert.mode": "upsert",
            "pk.mode": "record_value",
            "pk.fields": "id",
            "table.name.format": topic_name,
            # "schemas.enable": "true",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            # "key.converter.schemas.enable": "false"
        }
    }
    response = requests.post(url, json=data)
    if response.status_code == 201:
        return True
    else:
        print(response.status_code)
        print(response.text)
        return False

def delete_connector(topic_name: str):
    url = f'http://{os.environ["KAFKA_CONNECT"]}/connectors/{topic_name}-connector'
    response = requests.delete(url)
    if response.status_code == 204:
        return True
    else:
        return False

def standup_stack(conn: psycopg2.extensions.connection, topics: list[str]) -> bool:
    sql_files = os.listdir('sql')
    try:
        for sql_file in sql_files:
            with open(f'sql/{sql_file}', 'r') as f:
                sql = f.read()
            result = run_sql(conn, sql)
            print(result)
        
        for topic in topics:
            create_topic(topic, os.environ['KAFKA_BROKER'])

        for topic in topics:
            reg = register_connector(topic)
            print(f"Registered connector for {topic}: {reg}")
        
        conn.close()
    except:
        return False

    return True


def delete_stack(conn: psycopg2.extensions.connection, topics: list[str]) -> bool:
    admin_client = KafkaAdminClient(
        bootstrap_servers=os.environ['KAFKA_BROKER'],
        client_id='topic-deleter'
    )

    try:
        admin_client.delete_topics(topics)
    except:
        return False

    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()
    for table in tables:
        sql = f"DROP TABLE IF EXISTS {table[0]} CASCADE;"
        cursor.execute(sql)
    conn.commit()

    for topic in topics:
        delete_connector(topic)

    conn.close()
    return True


if __name__ == '__main__':
    print("here")
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', help='mode create or delete', default='create')
    args = parser.parse_args()
    if not args.mode in ['create', 'delete']:
        print('Invalid mode')
        exit(1)

    load_dotenv()
    print("here2")

    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        dbname=os.environ['POSTGRES_DB']
    )

    with open('topics.yml', 'r') as f:
        topic_config = yaml.safe_load(f)
    topics = topic_config['topics']

    if args.mode == 'create':
        standup_stack(conn, topics)
    elif args.mode == 'delete':
        delete_stack(conn, topics)
    else:
        print('Invalid mode')
        exit(1)

    print("done")
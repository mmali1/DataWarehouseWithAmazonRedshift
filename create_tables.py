import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def parse_config(file):
    """
    Read dwh.cfg file and load required config variables
    """
    config = configparser.ConfigParser()
    config.read_file(open(file))
    return config

def drop_tables(cur, conn):
    """
    Drop existing tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Read config
    Connect to database
    Drop existing tables
    Create tables
    Close connection
    """
    print("Reading config file")
    config_file_name = './dwh.cfg'
    config = parse_config(config_file_name) 
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Dropping tables...")
    drop_tables(cur, conn)
    print("Creating tables...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
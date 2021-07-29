import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def parse_config(file):
    """
    Read dwh.cfg file and load required config variables
    """
    config = configparser.ConfigParser()
    config.read_file(open(file))
    return config

def load_staging_tables(cur, conn):
    """
    Load staging tables with events and song data
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data in fact and dimension tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Read config
    Connect to database
    Load staging tables
    Load fact and dimension tables
    Close connection
    """

    print("Reading config file")
    config_file_name = './dwh.cfg'
    config = parse_config(config_file_name)    
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
  
    print("Loading staging tables...\n")
    load_staging_tables(cur, conn)
    print("Inserting data in fact and dimension tables...\n")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
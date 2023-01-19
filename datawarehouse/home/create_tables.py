import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

## Execute all of the drop table statements for the sparkify data warehouse tables
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

## Execute all of the create table statements for the sparkify datawarehouse tables
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

## Main method for creating the structure of sparkify data warehouse
def main():
    config = configparser.ConfigParser()
    ##Read dwh.cfg file for loading the credentials to connect to Sparkify's redshift cluster 
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
     ##Connect to Sparkify's redshift cluster 
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    ##Close connection to Sparkify's redshift cluster 
    conn.close()


if __name__ == "__main__":
    main()
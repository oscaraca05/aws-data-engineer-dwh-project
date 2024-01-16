import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function runs all the copy commands defined in the list 'copy_table_queries' using the cur and conn in the parameters, the list is previously imported from the file sql_queries.py, these copy commands populate the staging tables staging_events and staging_songs with the data in s3://udacity-dend/log_data and s3://udacity-dend/song_data respectively.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function runs all the insert queries defined in the list 'insert_table_queries' using the cur and conn in the parameters, the list is previously imported from the file sql_queries.py, these insert queries populate the star schema tables songplays, users, songs, artists, and time with the data previously loaded to the staging tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads parameters from dwh.cfg file, creates conn and cur objects and runs load_staging_tables and insert_tables functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
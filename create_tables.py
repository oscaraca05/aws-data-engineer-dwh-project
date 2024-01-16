import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function runs all the queries defined in the list 'drop_table_queries' using the cur and conn in the parameters, the list is previously imported from the file sql_queries.py, these queries delete (if they exist) all the staging tables and star schema tables to ensure an empty database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function runs all the queries defined in the list 'create_table_queries' using the cur and conn in the parameters, the list is previously imported from the file sql_queries.py, these queries create the staging_events and staging_songs tables, they also create the star schema tables songplays, users, songs, artistsn and time.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads parameters from dwh.cfg file, creates conn and cur objects and runs drop_tables and create_tables functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
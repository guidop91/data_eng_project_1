import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Create databases
    Parameters
    ----------
    No parameters

    Returns
    -------
    Tuple of cursor and connection objects
    """
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not connect to DB")
        print(e)

    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to DB")
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error: Could not drop database")
        print(e)

    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Could not create database")
        print(e)

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not connect to DB")
        print(e)
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to DB")
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    """
    Run every `DROP TABLE` query

    Parameters
    ----------
    cur: cursor to database\n
    conn: connection to database

    Returns
    -------
    None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not drop table")
            print(e)

        conn.commit()


def create_tables(cur, conn):
    """
    Run every `CREATE TABLE` query

    Parameters
    ----------
    cur: cursor to database\n
    conn: connection to database

    Returns
    -------
    None
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error: Could not create table")
            print(e)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

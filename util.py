# Jessica Embury, SDSU, GEOG683 final project, Spring 2022
# Functions related to Python package utility

# IMPORTS
from sqlalchemy import create_engine
import psycopg2
import sys


# FUNCTIONS
def psycopg2_connect(dbparams):
    """
    Create a Postgres database connection using psycopg2
    reference: https://github.com/NaysanSaran/pandas2postgresql/blob/master/notebooks/CompleteExample.ipynb
    :param dbparams: database connection parameters
    :return: conn
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**dbparams)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn


def sqlalchemy_engine(dbparams):
    """
    Connect to a PostgreSQL database using SQLAlchemy
    :param dbparams: dictionary containing database log in information (user, password, host, port, dbname)
    :return: SQLAlchemy engine
    """
    # create enging for database connection
    engine = create_engine(
        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            dbparams['user'], dbparams['password'], dbparams['host'], dbparams['port'], dbparams['dbname']))

    return engine


def get_next_routeid(dbparams, dbschema, dbtable, walk=False):
    """
    Return the next routeid for shortest path analysis (first route id in dataframe from get_od_routes())
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param dbschema: String containing database schema name
    :param dbtable: String containing database table name
    :param walk: Boolean, True if walking routes and False for driving routes
    :return: Integer value of next null routeid
    """
    # walking routes
    if walk:
        sql_str = "SELECT routeid from {}.{} WHERE walk_path IS NOT NULL ORDER by routeid DESC LIMIT 1;".format(dbschema, dbtable)
        sql_str2 = "SELECT routeid from {}.{} WHERE walk = 1 ORDER by routeid LIMIT 1;".format(dbschema, dbtable)
    # driving routes
    else:
        sql_str = "SELECT routeid from {}.{} WHERE drive_path IS NULL ORDER by routeid LIMIT 1;".format(dbschema, dbtable)
        sql_str2 = "SELECT routeid from {}.{} ORDER by routeid LIMIT 1;".format(dbschema, dbtable)

    # connect to database
    engine = sqlalchemy_engine(dbparams)
    with engine.connect() as conn:
        try:
            # get next null routeid
            result = conn.execute(sql_str)
            id = result.fetchall()[0][0]
        except Exception as e:
            print(e)
            # get first routeid
            result = conn.execute(sql_str2)
            id = result.fetchall()[0][0]
    return id

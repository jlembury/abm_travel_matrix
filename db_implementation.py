# Jessica Embury, SDSU, GEOG683 final project, Spring 2022
# Functions related to basic database set up

# IMPORTS
import pandas as pd
import geopandas as gpd
import psycopg2
import io
import sqlalchemy
from util import sqlalchemy_engine, psycopg2_connect


# FUNCTIONS
def set_primary_key(dbparams, pkey_schema, pkey_table, pkey_name, pkey_cols_list):
    """
    Create a primary key constraint for a Postgres table
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param pkey_schema: String containing name of database schema
    :param pkey_table: String containing name of database table
    :param pkey_name: String containing name of primary key
    :param pkey_cols_list: List containing names of columns used to create the primary key
    :return: None
    """
    # SQL to create primary key
    pkey_cols_str = ""
    for col in pkey_cols_list:
        pkey_cols_str += "{}, ".format(col)
    pkey_cols_str = pkey_cols_str[:-2]

    pkey_str = "ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({});".format(pkey_schema, pkey_table, pkey_name, pkey_cols_str)
    print(pkey_str)

    # connet to the database
    engine = sqlalchemy_engine(dbparams)
    # create primary key
    with engine.connect() as conn:
        conn.execute(pkey_str)

    return


def set_foreign_key(dbparams, main_schema, main_table, fkey_name, main_cols_list, foreign_schema, foreign_table, foreign_cols_list):
    """
    Create a foreign key constraint for a Postgres table
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param main_schema: String containing name of table schema
    :param main_table: String containing name of table
    :param fkey_name: String containing name of foreign key
    :param main_cols_list: List of columns used to create the foreign key
    :param foreign_schema: String containing name of referenced table's schema
    :param foreign_table: String containing name of referenced table
    :param foreign_cols_list: List of columns in referenced table used to create the foreign key
    :return: None
    """
    # SQL syntaxt to create foreign key
    main_cols_str = ""
    for col in main_cols_list:
        main_cols_str += "{}, ".format(col)
    main_cols_str = main_cols_str[:-2]

    foreign_cols_str = ""
    for col in foreign_cols_list:
        foreign_cols_str += "{}, ".format(col)
    foreign_cols_str = foreign_cols_str[:-2]

    fkey_str = "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({});".format(
        main_schema, main_table, fkey_name, main_cols_str, foreign_schema, foreign_table, foreign_cols_str)
    print(fkey_str)

    # connect to the database
    engine = sqlalchemy_engine(dbparams)
    # create the foreign key
    with engine.connect() as conn:
        conn.execute(fkey_str)

    return


def create_spatial_index(dbparams, schema, table, index_name, geom_col, randomize=False):
    """
    Create a spatial index on a table's geometry column
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param schema: String containing name of schema
    :param table: String containing name of table
    :param index_name: String containing index name
    :param geom_col: String containing name of geometry column
    :param randomize: (optional) Boolean, if True - replace table with identical table but with randomized row order
    :return: None
    """
    # SQL to create spatial index
    sql_str = "CREATE INDEX {} ON {}.{} USING gist({})".format(index_name, schema, table, geom_col)

    # SQL to replace table with identical table with randomized rows
    if randomize is True:
        sql_str1 = "CREATE TABLE {}.{}_temp AS (SELECT * FROM {}.{} ORDER BY random());".format(schema, table, schema, table)
        sql_str2 = "DROP TABLE {}.{}".format(schema, table)
        sql_str3 = "ALTER TABLE {}.{}_temp RENAME TO {};".format(schema, table, table)

    # connect to the database
    engine = sqlalchemy_engine(dbparams)
    with engine.connect() as conn:
        # replace table with identical table with randomized rows
        if randomize is True:
            conn.execute(sql_str1)
            conn.execute(sql_str2)
            conn.execute(sql_str3)
        # create spatial index
        conn.execute(sql_str)

    return


def shp2dbtable(shp_path, dbparams, dbschema, dbtable, pkey=None, fkey=None):
    """
    Create a Postgres table from a shapefile
    :param shp_path: String containing path to shapefile
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param dbschema: String containing database schema name
    :param dbtable: String containing database table name
    :param pkey: (optional) List with primary key arguments [pkey_name, pkey_cols_list]
    :param fkey: (optional) 2D List with foreign key arguments [[fkey_name, fkey_cols_list, ref_schema, ref_table, ref_cols_list],...]
    :return: None
    """
    # read shapefile as a geodataframe
    gdf = gpd.read_file(shp_path)

    # connect to the database
    engine = sqlalchemy_engine(dbparams)

    # create new table from geodataframe (in public) with spatial index
    gdf.to_postgis("{}".format(dbtable), engine)

    # move table to desired schema (from public)
    sql_string = "ALTER TABLE {} SET SCHEMA {};".format(dbtable, dbschema)
    with engine.connect() as conn:
        result = conn.execute(sql_string)
    print("New table created: {}.{}".format(dbschema, dbtable))

    # create primary key
    if pkey is not None:
        set_primary_key(dbparams, dbschema, dbtable, pkey[0], pkey[1])
    # create foreign key(s)
    if fkey is not None:
        for i in range(len(fkey)):
            set_foreign_key(dbparams, dbschema, dbtable, fkey[i][0], fkey[i][1], fkey[i][2], fkey[i][3], fkey[i][4])

    return


def csv2dbtable(csv_path, dbparams, dbschema, dbtable, pkey=None, fkey=None):
    """
    Create a Postgres table from a CSV file
    :param csv_path: String containing path to a CSV file
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param dbschema: String containing database schema name
    :param dbtable: String containing database table
    :param pkey: (optional) List with primary key arguments [pkey_name, pkey_cols_list]
    :param fkey: (optional) 2D List with foreign key arguments [[fkey_name, fkey_cols_list, ref_schema, ref_table, ref_cols_list],...]
    :return: None
    """
    # read csv as a dataframe
    df = pd.read_csv(csv_path)

    # connect to the database
    engine = sqlalchemy_engine(dbparams)
    with engine.connect() as conn:
        # create new empty table from dataframe (drops old table if exists and creates new empty table)
        df.head(0).to_sql(dbtable, engine, schema=dbschema, if_exists='replace', index=False)

    # copy contents of dataframe into Postgres table
    conn = psycopg2_connect(dbparams)
    cur = conn.cursor()

    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, '{}.{}'.format(dbschema, dbtable), null="")  # null values become ''
    print("New table created: {}.{}".format(dbschema, dbtable))

    conn.commit()
    cur.close()
    conn.close()

    # create primary key
    if pkey is not None:
        set_primary_key(dbparams, dbschema, dbtable, pkey[0], pkey[1])
    # create foreign key(s)
    if fkey is not None:
        for i in range(len(fkey)):
            set_foreign_key(dbparams, dbschema, dbtable, fkey[i][0], fkey[i][1], fkey[i][2], fkey[i][3], fkey[i][4])

    return


def create_project_tables(dbparams, old_dbschema, old_dbtable, new_dbschema, new_dbtable, cols_dict, randomize=False, where_clause=None, pkey=None, fkey=None, spatial_index=None):
    """
    Create a basic Postgres table from another Postgres table (SELECT cols FROM table WHERE condition)
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param old_dbschema: String containing database schema name of parent table
    :param old_dbtable: String containing database name of parent table
    :param new_dbschema: String containing database name of new table's schema
    :param new_dbtable: String containing database name of new table
    :param cols_dict: Dictionary containing column names {'parent_col_name': 'new_col_name'}
    :param randomize: (optional) Boolean, if True - add ORDER BY random() to randomize row order
    :param where_clause: (optional) Include conditions in a WHERE clause
    :param pkey: (optional) List with primary key arguments [pkey_name, pkey_cols_list]
    :param fkey: (optional) 2D List with foreign key arguments [[fkey_name, fkey_cols_list, ref_schema, ref_table, ref_cols_list],...]
    :param spatial_index: (optional) List with spatial index arguments [index_name, geometry_col, randomize (boolean)]
    :return: None
    """
    # create SQL query string to create new table
    cols_str = ""
    for col in cols_dict:
        cols_str += "{} as {}, ".format(col, cols_dict[col])
    cols_str = cols_str[:-2]
    select_str = "SELECT {} FROM {}.{}".format(cols_str, old_dbschema, old_dbtable)

    if where_clause is None:
        sql_str = "CREATE TABLE {}.{} AS ({});".format(new_dbschema, new_dbtable, select_str)
    else:
        sql_str = "CREATE TABLE {}.{} AS ({} WHERE {});".format(new_dbschema, new_dbtable, select_str, where_clause)
    if randomize:
        sql_str = "CREATE TABLE {}.{} AS ({} WHERE {}) ORDER BY random();".format(new_dbschema, new_dbtable, select_str, where_clause)
    print(sql_str)

    # connect to the database
    engine = sqlalchemy_engine(dbparams)
    # create new database table
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text(sql_str))

    # create primary key
    if pkey is not None:
        set_primary_key(dbparams, new_dbschema, new_dbtable, pkey[0], pkey[1])
    # create foreign key(s)
    if fkey is not None:
        for i in range(len(fkey)):
            set_foreign_key(dbparams, new_dbschema, new_dbtable, fkey[i][0], fkey[i][1], fkey[i][2], fkey[i][3], fkey[i][4])
    # create spatial index
    if spatial_index is not None:
        create_spatial_index(dbparams, new_dbschema, new_dbtable, spatial_index[0], spatial_index[1], spatial_index[2])

    return

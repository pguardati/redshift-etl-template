import pandas as pd
import psycopg2


def get_log_errors(cur):
    """Get the error log from redshift database
    Args:
        cur(psycopg2.cursor): psycopg2 cursor

    Returns:
        pd.DataFrame
    """
    # get errors
    cur.execute("SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;")
    res = cur.fetchall()
    # build dataframe using query records
    columns = [desc[0] for desc in cur.description]
    lines = [list(line) for line in res]
    df = pd.DataFrame(lines, columns=columns)
    # print only the error column
    print(df["err_reason"].values)
    return df


def get_top_elements_from_table(cur, table, n_elem=5, viz=False):
    """Get first 5 elements from a table of a database
    Args:
        cur(cursor): cursor of psycopg2
        table(str): name of the table to fetch
        n_elem(int): number of elements to be queried
        viz(bool): if True, visualise the extracted elements

    Returns:
        pd.DataFrame
    """
    # get query result
    limit = "" if n_elem == -1 else "LIMIT {}".format(n_elem)
    cur.execute("SELECT * FROM {} {};".format(table, limit))
    res = cur.fetchall()

    # build dataframe using query records
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(res, columns=columns)

    if viz:
        with pd.option_context('display.max_columns', None):
            print("\n\n{}:".format(table))
            print(df)
    return df


def get_res_as_dataframe(cur, viz=False):
    """Get the result of a query as a pandas DataFrame
    Args:
        cur(psycopg2.cursor): psycopg2 cursor
        viz(bool): if True, print the extracted frame

    Returns:
        pd.DataFrame
    """
    res = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(res, columns=columns)
    if viz:
        print(df)
    return df

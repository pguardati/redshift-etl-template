import configparser
import numpy as np
import pandas as pd

from sparkify_redshift_db.src import sql_queries, utils
from sparkify_redshift_db.constants import logger, CONFIG_PATH_DWH_CURRENT

# CONFIG
config = configparser.ConfigParser()
config.read(CONFIG_PATH_DWH_CURRENT)

"""Queries to fill Staging tables from S3"""
staging_events_copy_sample = ("""COPY staging_events FROM '{}/2018/11/2018-11'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON '{}'
REGION 'us-west-2';
""").format(
    config.get("S3", "log_data"),
    config.get("IAM_ROLE", "arn"),
    config.get("S3", "log_jsonpath")
)
staging_songs_copy_sample = ("""COPY staging_songs FROM '{}/A/A/'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
;""").format(
    config.get("S3", "song_data"),
    config.get("IAM_ROLE", "arn")
)
staging_copy_sample = [staging_events_copy_sample, staging_songs_copy_sample]

"""Queries to fill Staging tables from a DataFrame"""
staging_events_table_insert_manual = """INSERT INTO staging_events (
    artist,
    auth,
    firstName,
    gender,
    itemInSession,
    lastName,
    length,
    level,
    location,
    method,
    page,
    registration,
    sessionId,
    song,
    status,
    ts,
    userAgent,
    userId
) 
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
staging_songs_table_insert_manual = """
INSERT INTO staging_songs (
    artist_id,
    artist_latitude,
    artist_location,
    artist_longitude,
    artist_name,
    duration,
    num_songs,
    song_id,
    title,
    year
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

"""Functions to fill staging tables with custom data"""


def create_and_fill_log_staging_from_dataframe(cur, df, viz=True):
    """Create empty table to store event data and insert them line by line"""
    # create event stage
    cur.execute(sql_queries.staging_events_table_drop)
    cur.execute(sql_queries.staging_events_table_create)

    # insert multiple lines
    df = df[[
        "artist",
        "auth",
        "firstName",
        "gender",
        "itemInSession",
        "lastName",
        "length",
        "level",
        "location",
        "method",
        "page",
        "registration",
        "sessionId",
        "song",
        "status",
        "ts",
        "userAgent",
        "userId"
    ]]
    # fix data to stage only admissible userid
    df = df.loc[df.userId != '', :]
    for i, row in df.iterrows():
        cur.execute(staging_events_table_insert_manual, row)
    # check table
    df_table = utils.get_top_elements_from_table(cur, "staging_events", viz=viz)
    return df_table


def create_and_fill_songs_staging_from_dataframe(cur, df, viz=True):
    """Create empty table to store songs data and insert them line by line"""
    # create songs stage
    cur.execute(sql_queries.staging_songs_table_drop)
    cur.execute(sql_queries.staging_songs_table_create)

    # insert multiple lines
    df = df[[
        "artist_id",
        "artist_latitude",
        "artist_location",
        "artist_longitude",
        "artist_name",
        "duration",
        "num_songs",
        "song_id",
        "title",
        "year"
    ]]
    # prevent postgres to store nans
    df = df.where(pd.notnull(df), None)
    for i, row in df.iterrows():
        cur.execute(staging_songs_table_insert_manual, row)

    # check table
    df_table = utils.get_top_elements_from_table(cur, "staging_songs", viz=viz)
    return df_table


"""Functions to create and fill the star schema"""


def create_and_fill_users_from_staged_events(cur, viz=True):
    """Copy data from staged events into a new table of users"""
    cur.execute(sql_queries.user_table_drop)
    cur.execute(sql_queries.user_table_create)
    cur.execute(sql_queries.user_table_insert)
    df_users = utils.get_top_elements_from_table(cur, "users", 10, viz=viz)
    return df_users


def create_and_fill_time_from_staged_events(cur, viz=True):
    """Copy data from staged events into a new table of time"""
    cur.execute(sql_queries.time_table_drop)
    cur.execute(sql_queries.time_table_create)
    cur.execute(sql_queries.time_table_insert)
    df_time = utils.get_top_elements_from_table(cur, "time", 10, viz=viz)
    return df_time


def create_and_fill_songs_from_staged_songs(cur, viz=True):
    """Copy data from staged songs into a new table of songs"""
    cur.execute(sql_queries.song_table_drop)
    cur.execute(sql_queries.song_table_create)
    cur.execute(sql_queries.song_table_insert)
    df_songs = utils.get_top_elements_from_table(cur, "songs", 10, viz=viz)
    return df_songs


def create_and_fill_artist_from_staged_songs(cur, viz=True):
    """Copy data from staged songs into a new table of artists"""
    cur.execute(sql_queries.artist_table_drop)
    cur.execute(sql_queries.artist_table_create)
    cur.execute(sql_queries.artist_table_insert)
    df_artists = utils.get_top_elements_from_table(cur, "artists", 10, viz=viz)
    return df_artists


def create_and_fill_songplays_from_staged_data(cur, viz=True):
    """Copy data from staged songs and events into a new table of songplays"""
    cur.execute(sql_queries.songplay_table_drop)
    cur.execute(sql_queries.songplay_table_create)
    cur.execute(sql_queries.songplay_table_insert)
    df_songsplay = utils.get_top_elements_from_table(cur, "songplays", 10, viz=viz)
    return df_songsplay


def read_test_csv(path_df):
    """Read and clean test datasets
    Args:
        path_df(str): path test dataset

    Returns:
        pd.DataFrame
    """
    df=pd.read_csv(path_df)
    # remove comments from csv files
    df = df.loc[~df.iloc[:, 0].astype(str).str.startswith('#', na=False), :].copy()
    # instead of nans use -1 for numeric types and None for strings
    df.loc[:, df.dtypes != object] = df.loc[:, df.dtypes != object].replace([np.nan], [-1])
    df.loc[:, df.dtypes == object] = df.loc[:, df.dtypes == object].replace([np.nan], [None])
    df = df.reset_index(drop=True)
    return df

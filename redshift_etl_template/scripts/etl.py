import configparser
import psycopg2
import argparse
import sys

from redshift_etl_template.src.sql_queries import copy_table_queries, insert_table_queries
from redshift_etl_template.constants import CONFIG_PATH_DWH_CURRENT, logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_staging_tables(cur, conn):
    logger.info("Copying json files from s3 to redshift..")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    logger.info("Processing staged data to fill analytics tables..")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def parse_input(args):
    parser = argparse.ArgumentParser(description="Script to load data from s3 into staging tables,\
                                                 and from them, into the analytics tables")
    parser.add_argument("--path_config_current",
                        help="path of the configuration file of launched infrastructure",
                        default=CONFIG_PATH_DWH_CURRENT)
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)

    config = configparser.ConfigParser()
    config.read(args.path_config_current)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    logger.info("ETL completed, disconnecting from the database..")
    conn.close()


if __name__ == "__main__":
    main()

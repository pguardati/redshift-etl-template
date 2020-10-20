import configparser
import psycopg2
import argparse
import sys

from redshift_etl_template.src.sql_queries import create_table_queries, drop_table_queries
from redshift_etl_template.constants import CONFIG_PATH_DWH_CURRENT, logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def drop_tables(cur, conn):
    logger.info("Dropping existing tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    logger.info("Creating empty tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def parse_input(args):
    parser = argparse.ArgumentParser(description="Script to create staging and analytics\
                                                 tables inside the data warehouse server")
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

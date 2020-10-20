import configparser
import psycopg2
import argparse
import sys
import pandas as pd

from redshift_etl_template.src.sql_queries import star_tables, staging_tables
from redshift_etl_template.src.utils import get_top_elements_from_table, get_log_errors
from redshift_etl_template.constants import CONFIG_PATH_DWH_CURRENT, logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def check_database_content(cur):
    """Check top 5 elements for each table in the database"""
    for table in staging_tables + star_tables:
        df = get_top_elements_from_table(cur, table)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            logger.info(" --Table : {}\n{}\n".format(table,df))


def parse_input(args):
    parser = argparse.ArgumentParser(description="Script to query head from all the tables \
                                                 inside the data warehouse server")
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

    logger.info("Log of current machine:\n")
    df_error = get_log_errors(cur)
    logger.info("Content of current database")
    check_database_content(cur)

    conn.close()


if __name__ == "__main__":
    main()

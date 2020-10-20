import os
import boto3
import configparser
import unittest
import psycopg2

from sparkify_redshift_db.constants import CONFIG_PATH_DWH_CURRENT, logging
from sparkify_redshift_db.scripts import create_tables
from sparkify_redshift_db.src import utils
from sparkify_redshift_db.tests import utils_tests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
VIZ = False


class TestStagingInsertion(unittest.TestCase):
    """Sample data from s3 and store them into memory.
    Then, perform insertion into Redshift database.
    Note: Redshift cluster has to be online - use create_infrastructure.py"""

    def setUp(self):
        logger.info("Extracting configuration..")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH_DWH_CURRENT)
        KEY = config.get("AWS", "KEY")
        SECRET = config.get("AWS", "SECRET")

        logger.info("Connecting to data source..")
        self.s3 = boto3.resource(
            "s3",
            region_name="us-west-2",
            aws_access_key_id=KEY,
            aws_secret_access_key=SECRET
        )
        logger.info("Connecting to the database..")
        self.conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self.cur = self.conn.cursor()

        logger.info("Creating the tables..")
        create_tables.drop_tables(self.cur, self.conn)
        create_tables.create_tables(self.cur, self.conn)

    def tearDown(self):
        self.cur.close()

    def test_staging_log_parallel(self):
        logger.info("Processing sampled staged events data")
        self.cur.execute(utils_tests.staging_events_copy_sample)
        df_logs = utils.get_top_elements_from_table(self.cur, "staging_events", viz=VIZ)
        pass

    def test_staging_songs_parallel(self):
        logger.info("Processing sampled staged songs data")
        self.cur.execute(utils_tests.staging_songs_copy_sample)
        df_songs = utils.get_top_elements_from_table(self.cur, "staging_songs", viz=VIZ)


if __name__ == "__main__":
    unittest.main()

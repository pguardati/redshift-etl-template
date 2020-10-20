import os
import unittest
import configparser
import psycopg2
import pandas as pd
from pandas.testing import assert_frame_equal

from sparkify_redshift_db.constants import CONFIG_PATH_DWH_CURRENT, DIR_DATA_TEST, logging
from sparkify_redshift_db.scripts import create_tables
from sparkify_redshift_db.tests import utils_tests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

N_ELEM = 20
VIZ = False


class TestStarInsertion(unittest.TestCase):
    """Load data from s3 to redshift,
    Run redshift2redshift insertion
    from staging schema into star schema.
    Note 1: Aws infrastructure has to be available at run time ( use script/create_infrastructure.py )
    Note 2: do not forget to destroy the infrastructure
    if you do not need it anymore ( script/destroy_infrastructure.py )
    """

    def setUp(self):
        logger.info("Connecting to the database..")
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH_DWH_CURRENT)
        self.conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self.cur = self.conn.cursor()

    def tearDown(self):
        create_tables.drop_tables(self.cur, self.conn)
        self.cur.close()

    def test_users(self):
        logger.info("Filling staging events to test users table")
        df_log = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_user_staging_events.csv"))
        df_log_ins = utils_tests.create_and_fill_log_staging_from_dataframe(self.cur, df_log, viz=VIZ)
        logger.info("Filling from staging table..")
        df_users = utils_tests.create_and_fill_users_from_staged_events(self.cur, viz=VIZ)
        logger.info("Comparing with expected results..")
        df_target = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_user_target.csv"))
        df_target.iloc[:, 0] = df_target.iloc[:, 0].astype(int)
        assert_frame_equal(df_users, df_target)

    def test_time(self):
        logger.info("Filling staging events to test time table")
        df_log = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_time_staging_events.csv"))
        df_log_ins = utils_tests.create_and_fill_log_staging_from_dataframe(self.cur, df_log, viz=VIZ)
        logger.info("Filling from staging table..")
        df_time = utils_tests.create_and_fill_time_from_staged_events(self.cur, viz=VIZ)
        logger.info("Comparing with expected results..")
        df_time["weekday"] = df_time["weekday"].str.strip()  # remove weekday have extra spaces
        df_target = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_time_target.csv"))
        df_target.iloc[:, 0] = df_target.iloc[:, 0].astype('datetime64[ns]')
        assert_frame_equal(df_time, df_target)

    def test_songs(self):
        logger.info("Filling staging songs to test songs table")
        df_songs = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songs_staging_songs.csv"))
        df_songs_ins = utils_tests.create_and_fill_songs_staging_from_dataframe(self.cur, df_songs, viz=VIZ)
        logger.info("Filling from staging table..")
        df_songs_star = utils_tests.create_and_fill_songs_from_staged_songs(self.cur, viz=VIZ)
        logger.info("Comparing with expected results..")
        df_target = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songs_target.csv"))
        assert_frame_equal(df_songs_star, df_target)

    def test_artists(self):
        logger.info("Filling staging songs to test songs table")
        df_songs = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_artist_staging_songs.csv"))
        df_songs_ins = utils_tests.create_and_fill_songs_staging_from_dataframe(self.cur, df_songs, viz=VIZ)
        logger.info("Filling from staging table..")
        df_artist = utils_tests.create_and_fill_artist_from_staged_songs(self.cur, viz=VIZ)
        logger.info("Comparing with expected results..")
        df_target = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_artist_target.csv"))
        assert_frame_equal(df_artist, df_target)

    def test_songsplay(self):
        logger.info("Filling staging tables to test songplays table")
        df_log = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_staging_events.csv"))
        df_log_ins = utils_tests.create_and_fill_log_staging_from_dataframe(self.cur, df_log, viz=VIZ)
        df_songs = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_staging_songs.csv"))
        df_songs_ins = utils_tests.create_and_fill_songs_staging_from_dataframe(self.cur, df_songs, viz=VIZ)
        logger.info("Filling from staging tables...")
        df_songsplay = utils_tests.create_and_fill_songplays_from_staged_data(self.cur, viz = VIZ)
        logger.info("Comparing with expected results..")
        df_target = utils_tests.read_test_csv(os.path.join(DIR_DATA_TEST, "df_songplays_target.csv"))
        df_target.iloc[:, 1] = df_target.iloc[:, 1].astype('datetime64[ns]')
        assert_frame_equal(df_songsplay.iloc[:, 1:], df_target.iloc[:, 1:])  # ignore unreliable seed step


if __name__ == "__main__":
    unittest.main()

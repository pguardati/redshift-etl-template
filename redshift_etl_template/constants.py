import os
import logging

# General
PROJECT_NAME = 'redshift_etl_template'
REPOSITORY_PATH = os.path.realpath(__file__)[:os.path.realpath(__file__).find(PROJECT_NAME)]
PROJECT_PATH = os.path.join(REPOSITORY_PATH, PROJECT_NAME)

# Local data
DIR_DATA = os.path.join(PROJECT_PATH, 'data')
DIR_DATA_TEST = os.path.join(PROJECT_PATH, 'tests', 'test_data')

# DWH Config
CONFIG_PATH_DWH_LAUNCH = os.path.join(REPOSITORY_PATH, "credentials", "dwh_launch.cfg")
CONFIG_PATH_DWH_CURRENT = os.path.join(REPOSITORY_PATH, "credentials", "dwh.cfg")

# General purpose logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger.info("Project_path:{}".format(PROJECT_PATH))
logger.info("Data_path:{}".format(DIR_DATA))

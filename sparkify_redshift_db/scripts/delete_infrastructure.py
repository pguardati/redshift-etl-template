import argparse
import sys

from sparkify_redshift_db.src.infrastructure import CloudInfrastructureDestructor
from sparkify_redshift_db.constants import CONFIG_PATH_DWH_CURRENT


def parse_input(args):
    parser = argparse.ArgumentParser(description="Script to delete aws infrastructure using"
                                                 "the configuration file of the current available cluster")
    parser.add_argument("--path_config_current",
                        help="path of the configuration file to destroy the infrastructure",
                        default=CONFIG_PATH_DWH_CURRENT)
    parsed_args = parser.parse_args(args)
    return parsed_args


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)

    cloud_infrastructure = CloudInfrastructureDestructor(args.path_config_current)
    cloud_infrastructure.destroy()


if __name__ == "__main__":
    main()

import argparse
import sys

from sparkify_redshift_db.src.infrastructure import CloudInfrastructureConstructor
from sparkify_redshift_db.constants import CONFIG_PATH_DWH_LAUNCH, CONFIG_PATH_DWH_CURRENT


def parse_input(args):
    parser = argparse.ArgumentParser(description="Create aws infrastructure using a configuration file")
    parser.add_argument("--path_config_launch",
                        help="path of the configuration file to create the infrastructure",
                        default=CONFIG_PATH_DWH_LAUNCH)
    parser.add_argument("--path_config_current",
                        help="path of the configuration file of launched infrastructure",
                        default=CONFIG_PATH_DWH_CURRENT)
    parsed_args = parser.parse_args(args)
    return parsed_args


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)

    cloud_infrastructure = CloudInfrastructureConstructor(args.path_config_launch)
    cloud_infrastructure.create(args.path_config_current)


if __name__ == "__main__":
    main()

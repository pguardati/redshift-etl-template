import boto3
import json
import configparser
from sparkify_redshift_db.constants import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _wait_cluster_switching(redshift, dwh_cluster_identifier, initial_status):
    """Wait until the cluster has switched state
    creating->available
    deleting->error(no cluster found)
    Args:
        redshift(botocore.client.Redshift): client of redshift service
        dwh_cluster_identifier(str): identifier of the cluster
        initial_status(str): initial status before the switch - creating or deleting
    """
    cluster_status = initial_status
    while cluster_status == initial_status:
        try:
            cluster_status = redshift.describe_clusters(
                ClusterIdentifier=dwh_cluster_identifier
            )['Clusters'][0]["ClusterStatus"]
        except Exception as e:
            cluster_status = "deleted"
            break
    logger.info("Cluster is {}".format(cluster_status))


def _get_resources_as_clients(key, secret):
    """Get cloud resources
    Args:
        key(str): aws id key
        secret(str): aws secret password

    Returns:
        tuple
    """
    # iam role needed to grant access from s3 to dwh
    iam = boto3.client(
        "iam",
        region_name="us-west-2",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )
    # set up redshift to store the databases
    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )
    # set up ec2 to allow security groups to enter in dwh
    ec2 = boto3.resource(
        "ec2",
        region_name="us-west-2",
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )
    return iam, redshift, ec2


def _get_security_group(vpc, target_id):
    """Get a specific security group in the virtual private connection (vpc) of the cluster
    Args:
        vpc(ec2.Vpc): object pointing to the cloud vpc
        target_id(str): id of the specific security group

    Returns:
        ec2.SecurityGroup
    """
    security_group = list(vpc.security_groups.all())
    selected_group = []
    for group in security_group:
        if group.id == target_id:
            selected_group.append(group)
    if not selected_group:
        raise Exception("no security group with id {}".format(target_id))
    else:
        return selected_group[0]


class CloudInfrastructureConstructor:
    def __init__(self, config_file):
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        self.key = config.get('AWS', 'KEY')
        self.secret = config.get('AWS', 'SECRET')

        self.cluster_type = config.get("HARDWARE", "CLUSTER_TYPE")
        self.cluster_num_nodes = config.get("HARDWARE", "CLUSTER_NUM_NODES")
        self.cluster_node_type = config.get("HARDWARE", "CLUSTER_NODE_TYPE")
        self.cluster_identifier = config.get("HARDWARE", "CLUSTER_IDENTIFIER")

        self.db_name = config.get("DATABASE", "DB_NAME")
        self.db_user = config.get("DATABASE", "DB_USER")
        self.db_password = config.get("DATABASE", "DB_PASSWORD")
        self.db_port = config.get("DATABASE", "DB_PORT")

        # to authorize s3 to write on dwh
        self.dwh_iam_role_name = config.get("SECURITY", "DWH_IAM_ROLE_NAME")
        self.dwh_security_group_id = config.get("SECURITY", "DWH_SECURITY_GROUP_ID")

        self.iam, self.redshift, self.ec2 = _get_resources_as_clients(self.key, self.secret)

    def create_iam_role_with_s3_access(self):
        dwh_role = self.iam.create_role(
            Path='/',
            RoleName=self.dwh_iam_role_name,
            Description="Allows Redshift clusters to call AWS services on you behalf",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [
                    {'Action': 'sts:AssumeRole',
                     'Effect': 'Allow',
                     'Principal':
                         {'Service': 'redshift.amazonaws.com'}
                     }
                ],
                'Version': '2012-10-17'
            })
        )
        status = self.iam.attach_role_policy(
            RoleName=self.dwh_iam_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']
        role_arn = self.iam.get_role(
            RoleName=self.dwh_iam_role_name
        )['Role']['Arn']
        return role_arn

    def create_cluster(self, role_arn):
        response = self.redshift.create_cluster(

            # hardware
            ClusterType=self.cluster_type,
            NodeType=self.cluster_node_type,
            NumberOfNodes=int(self.cluster_num_nodes),

            # identifiers & credentials
            DBName=self.db_name,
            ClusterIdentifier=self.cluster_identifier,
            MasterUsername=self.db_user,
            MasterUserPassword=self.db_password,

            # role (to allow s3 access)
            IamRoles=[role_arn]
        )

    def enable_communication_s3_with_dwh(self, dwh_vpc_id):
        # get vpc cluster id
        vpc = self.ec2.Vpc(id=dwh_vpc_id)
        security_group = _get_security_group(vpc, self.dwh_security_group_id)

        # enable security groups to access dwh
        security_group.authorize_ingress(
            GroupName=security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(self.db_port),
            ToPort=int(self.db_port)
        )

    def export_dwh_current_config(
            self,
            path_config_output,
            dwh_vpc_id,
            dwh_role_arn,
            dwh_endpoint
    ):
        config_current_machine = configparser.ConfigParser()
        config_current_machine['AWS'] = {
            'KEY': self.key,
            'SECRET': self.secret
        }
        config_current_machine['CLUSTER'] = {
            'HOST': dwh_endpoint,
            'DB_NAME': self.db_name,
            'DB_USER': self.db_user,
            'DB_PASSWORD': self.db_password,
            'DB_PORT': self.db_port
        }
        config_current_machine['IAM_ROLE'] = {
            'ARN': dwh_role_arn
        }
        config_current_machine['S3'] = {
            'LOG_DATA': 's3://udacity-dend/log_data',
            'LOG_JSONPATH': 's3://udacity-dend/log_json_path.json',
            'SONG_DATA': 's3://udacity-dend/song_data'
        }
        config_current_machine['DESTROYER_INFO'] = {
            'DWH_CLUSTER_IDENTIFIER': self.cluster_identifier,
            'DWH_IAM_ROLE_NAME': self.dwh_iam_role_name,
            'DWH_VPC_ID': dwh_vpc_id,
            'DWH_SECURITY_GROUP_ID': self.dwh_security_group_id
        }
        with open(path_config_output, 'w') as configfile:
            config_current_machine.write(configfile)

    def create(self, path_config_output):
        """Create a Cluster and set security configuration to allow reading data from S3.
        Configuration of created machine is saved on a configuration file.
        Args:
            path_config_output(str): output path of configuration file of the created machine
        """
        logger.info("Creating IamRole - to enable DWH access from S3..")
        role_arn = self.create_iam_role_with_s3_access()

        logger.info("Creating Redshift Cluster - to host DWH..")
        self.create_cluster(role_arn)
        _wait_cluster_switching(
            self.redshift,
            self.cluster_identifier,
            initial_status="creating"
        )

        logger.info("Extracting cluster properties..")
        cluster_descriptor = self.redshift.describe_clusters(
            ClusterIdentifier=self.cluster_identifier
        )['Clusters'][0]
        dwh_endpoint = cluster_descriptor['Endpoint']['Address']
        dwh_role_arn = cluster_descriptor['IamRoles'][0]['IamRoleArn']
        dwh_vpc_id = cluster_descriptor['VpcId']

        logger.info("Enabling communication s3 <-> DWH..")
        self.enable_communication_s3_with_dwh(dwh_vpc_id)

        "Setup completed, Host:\npostgresql://{}:{}@{}:{}/{}".format(
            self.db_user,
            self.db_password,
            dwh_endpoint,
            self.db_port,
            self.db_name
        )

        logger.info("Exporting current machine configuration in {}".format(path_config_output))
        self.export_dwh_current_config(path_config_output, dwh_vpc_id, dwh_role_arn, dwh_endpoint)


class CloudInfrastructureDestructor:
    def __init__(self, config_file):
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        self.key = config.get('AWS', 'KEY')
        self.secret = config.get('AWS', 'SECRET')

        self.host = config.get('CLUSTER', 'HOST')
        self.dwh_port = config.get('CLUSTER', 'DB_PORT')

        self.dwh_role_arn = config.get('IAM_ROLE', 'ARN')

        self.dwh_cluster_identifier = config.get('DESTROYER_INFO', 'DWH_CLUSTER_IDENTIFIER')
        self.dwh_iam_role_name = config.get('DESTROYER_INFO', 'DWH_IAM_ROLE_NAME')
        self.dwh_vpc_id = config.get('DESTROYER_INFO', 'DWH_VPC_ID')
        self.dwh_security_group_id = config.get('DESTROYER_INFO', 'DWH_SECURITY_GROUP_ID')

        self.iam, self.redshift, self.ec2 = _get_resources_as_clients(self.key, self.secret)

    def destroy(self):
        """Destroy the cluster and the security settings of the current machine.
        """
        logger.info("Deleting the cluster..")
        self.redshift.delete_cluster(
            ClusterIdentifier=self.dwh_cluster_identifier,
            SkipFinalClusterSnapshot=True
        )
        _wait_cluster_switching(
            self.redshift,
            self.dwh_cluster_identifier,
            initial_status="deleting"
        )
        logger.info("Deleting IamRole..")
        self.iam.detach_role_policy(
            RoleName=self.dwh_iam_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        self.iam.delete_role(
            RoleName=self.dwh_iam_role_name
        )
        logger.info("Revoking DWH authorization..")
        vpc = self.ec2.Vpc(id=self.dwh_vpc_id)
        security_group = _get_security_group(vpc, self.dwh_security_group_id)
        security_group.revoke_ingress(
            GroupName=security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(self.dwh_port),
            ToPort=int(self.dwh_port)
        )
        logger.info("Infrastructure has been fully deleted")


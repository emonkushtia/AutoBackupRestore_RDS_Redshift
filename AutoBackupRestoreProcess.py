from __future__ import print_function
import boto3
import datetime
import time
import sys
import re

aws_access_key_id = boto3.session.Session().get_credentials().access_key
aws_secret_access_key = boto3.session.Session().get_credentials().secret_key
aws_security_token = boto3.session.Session().get_credentials().token


def sort_by_time_stamp(snapshot):
    if 'SnapshotCreateTime' in snapshot:
        return datetime.datetime.isoformat(snapshot['SnapshotCreateTime'])
    else:
        return datetime.datetime.isoformat(datetime.datetime.now())


def is_ec2_off(instanceId):
    client = boto3.client('ec2')
    try:
        ec2instance = client.describe_instances(InstanceIds=[instanceId])
        if len(ec2instance['Reservations']) > 0:
            instance = ec2instance['Reservations'][0]['Instances'][0]
            instanceState = instance['State']['Name']
            if instanceState == 'stopped' or instanceState == 'stopping':
                return True
    except:
        print('got exception to get the EC2 instance: ', sys.exc_info())
    return False


def start_ec2(instanceId):
    client = boto3.client('ec2')
    client.start_instances(InstanceIds=[instanceId], DryRun=False)


class Config(object):
    def __init__(self, eventObj):
        self.OrganizationIdentifier = eventObj['organizationIdentifier']

        self.RedshiftClusterIdentifier = eventObj['redshiftClusterIdentifier']
        self.RedshiftVpcSecurityGroupId = eventObj['redshiftVpcSecurityGroupId']
        self.RedshiftClusterParameterGroupName = eventObj['redshiftClusterParameterGroupName']
        self.RedshiftClusterSubnetGroupName = eventObj['redshiftClusterSubnetGroupName']

        self.RdsInstanceIdentifier = eventObj['rdsInstanceIdentifier']
        self.RdsVPCSecurityGroup = eventObj['rdsVPCSecurityGroup']
        self.RdsSubnetGroupName = eventObj['rdsSubnetGroupName']

        self.WebEC2Id = eventObj['webEC2Id']
        self.NumberOfSnapshots = eventObj['numberOfSnapshots']
        self.startEc2Forcefully = eventObj['startEc2Forcefully']


class RdsService(object):
    def __init__(self, config):
        self.config = config
        self.client = boto3.client('rds')

    def backup(self):
        if self._is_instance_available():
            self._delete_instance()
        self._clean_up_old_snapshots()

    def restore(self):
        if self._is_instance_exist():
            return

        latest_snapshot = self._get_latest_snapshot_id()
        if latest_snapshot is not None:
            self._create_instance(latest_snapshot)
        self._clean_up_old_snapshots()

    def _is_instance_exist(self):
        try:
            self.client.describe_db_instances(DBInstanceIdentifier=self.config.RdsInstanceIdentifier)
            return True
        except:
            pass
        return False

    def _is_instance_available(self):
        try:
            result = self.client.describe_db_instances(DBInstanceIdentifier=self.config.RdsInstanceIdentifier)
            instanceStatus = result['DBInstances'][0]['DBInstanceStatus']
            if instanceStatus == 'available':
                return True
        except:
            pass
        return False

    def _clean_up_old_snapshots(self):
        try:
            all_snaps = self.client.describe_db_snapshots(DBInstanceIdentifier=self.config.RdsInstanceIdentifier,
                                                          SnapshotType='manual')['DBSnapshots']
            if len(all_snaps) > 0:
                pattern = re.compile(self.config.RdsInstanceIdentifier + r'-lambda-\d\d\d\d\d\d\d\d-\d\d\d\d')
                matchedSnaps = []
                for snap in all_snaps:
                    if pattern.search(snap['DBSnapshotIdentifier']) is not None:
                        matchedSnaps.append(snap)
                if len(matchedSnaps) > self.config.NumberOfSnapshots:
                    matchedSnaps = sorted(matchedSnaps, key=sort_by_time_stamp, reverse=True)
                    snaps_to_be_deleted = matchedSnaps[self.config.NumberOfSnapshots:]
                    for snap_delete in snaps_to_be_deleted:
                        self.client.delete_db_snapshot(
                            DBSnapshotIdentifier=snap_delete['DBSnapshotIdentifier']
                        )
        except:
            print(sys.exc_info())

    def _create_instance(self, latest_snapshot):
        response = self.client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=self.config.RdsInstanceIdentifier,
            DBSnapshotIdentifier=latest_snapshot,
            DBSubnetGroupName=self.config.RdsSubnetGroupName,
            PubliclyAccessible=False
        )
        return response

    def _get_latest_snapshot_id(self):
        try:
            all_snaps = self.client.describe_db_snapshots(DBInstanceIdentifier=self.config.RdsInstanceIdentifier,
                                                          SnapshotType='manual')['DBSnapshots']
            if len(all_snaps) > 0:
                available_snaps = [snap for snap in all_snaps if snap['Status'] == 'available']
                if len(available_snaps) > 0:
                    available_snaps_sorted = sorted(available_snaps, key=sort_by_time_stamp, reverse=True)
                    return available_snaps_sorted[0]['DBSnapshotIdentifier']
        except:
            print(sys.exc_info())

        return None

    def _delete_instance(self):
        try:
            self.client.delete_db_instance(
                DBInstanceIdentifier=self.config.RdsInstanceIdentifier,
                SkipFinalSnapshot=False,
                FinalDBSnapshotIdentifier=self.config.RdsInstanceIdentifier + '-lambda-' + time.strftime("%Y%m%d-%H%M")
            )
        except:
            print(sys.exc_info())


class RedshiftService(object):
    def __init__(self, config):
        self.config = config
        self.client = boto3.client('redshift')

    def backup(self):
        if self._is_instance_available():
            self._delete_instance()
        self._clean_up_old_snapshots()

    def restore(self):
        if self._is_instance_exist():
            return

        latest_snapshot = self._get_latest_snapshot_id()
        if latest_snapshot is not None:
            self._create_instance(latest_snapshot)
        self._clean_up_old_snapshots()

    def _is_instance_exist(self):
        try:
            self.client.describe_clusters(ClusterIdentifier=self.config.RedshiftClusterIdentifier)
            return True
        except:
            pass
        return False

    def _is_instance_available(self):
        try:
            result = self.client.describe_clusters(ClusterIdentifier=self.config.RedshiftClusterIdentifier)
            instanceStatus = result['Clusters'][0]['ClusterStatus']
            if instanceStatus == 'available':
                return True
        except:
            pass
        return False

    def _clean_up_old_snapshots(self):
        try:
            all_snaps = self.client.describe_cluster_snapshots(ClusterIdentifier=self.config.RedshiftClusterIdentifier,
                                                               SnapshotType='manual')['Snapshots']
            if len(all_snaps) > 0:
                pattern = re.compile(self.config.OrganizationIdentifier + r'-redshift-lambda-\d\d\d\d\d\d\d\d-\d\d\d\d')
                matchedSnaps = []
                for snap in all_snaps:
                    if pattern.search(snap['SnapshotIdentifier']) is not None:
                        matchedSnaps.append(snap)
                if len(matchedSnaps) > self.config.NumberOfSnapshots:
                    matchedSnaps = sorted(matchedSnaps, key=sort_by_time_stamp, reverse=True)
                    snaps_to_be_deleted = matchedSnaps[self.config.NumberOfSnapshots:]
                    for snap_delete in snaps_to_be_deleted:
                        self.client.delete_cluster_snapshot(
                            SnapshotIdentifier=snap_delete['SnapshotIdentifier']
                        )
        except:
            print(sys.exc_info())

    def _create_instance(self, latest_snapshot):
        response = self.client.restore_from_cluster_snapshot(
            ClusterIdentifier=self.config.RedshiftClusterIdentifier,
            SnapshotIdentifier=latest_snapshot,
            ClusterSubnetGroupName=self.config.RedshiftClusterSubnetGroupName,
            VpcSecurityGroupIds=[self.config.RedshiftVpcSecurityGroupId],
            ClusterParameterGroupName=self.config.RedshiftClusterParameterGroupName,
            PubliclyAccessible=False
        )
        return response

    def _get_latest_snapshot_id(self):
        try:
            all_snaps = self.client.describe_cluster_snapshots(ClusterIdentifier=self.config.RedshiftClusterIdentifier,
                                                               SnapshotType='manual')['Snapshots']
            if len(all_snaps) > 0:
                available_snaps = [snap for snap in all_snaps if snap['Status'] == 'available']
                if len(available_snaps) > 0:
                    available_snaps_sorted = sorted(available_snaps, key=sort_by_time_stamp, reverse=True)
                    return available_snaps_sorted[0]['SnapshotIdentifier']
        except:
            print(sys.exc_info())

        return None

    def _delete_instance(self):
        try:
            self.client.delete_cluster(
                ClusterIdentifier=self.config.RedshiftClusterIdentifier,
                SkipFinalClusterSnapshot=False,
                FinalClusterSnapshotIdentifier=self.config.OrganizationIdentifier + '-redshift-lambda-' + time.strftime(
                    "%Y%m%d-%H%M")
            )
        except:
            print(sys.exc_info())


class SecurityGroup(object):
    def __init__(self, config):
        self.config = config
        self.client = boto3.client('rds')

    def update(self):
        if self._is_rds_has_default_security_group():
            self._update_rds_instance()

    def _update_rds_instance(self):
        self.client.modify_db_instance(
            DBInstanceIdentifier=self.config.RdsInstanceIdentifier,
            VpcSecurityGroupIds=[self.config.RdsVPCSecurityGroup],
            ApplyImmediately=True
        )

    def _is_rds_has_default_security_group(self):
        try:
            response = self.client.describe_db_instances(DBInstanceIdentifier=self.config.RdsInstanceIdentifier)
            instance = response['DBInstances'][0]
            security_groups = instance['VpcSecurityGroups']
            status = instance['DBInstanceStatus']
            if status != 'available':
                return False
            if len(security_groups) == 0:
                return True
            if security_groups[0]['VpcSecurityGroupId'] != self.config.RdsVPCSecurityGroup:
                return True
        except:
            pass
        return False


def lambda_handler(event, context):
    print(event)
    config = Config(event)
    if config.startEc2Forcefully:
        start_ec2(config.WebEC2Id)
        print('Ec2 startted successfully.')
        return

    if is_ec2_off(config.WebEC2Id):
        print('EC2 is off, So executing backup process')
        RdsService(config).backup()
        RedshiftService(config).backup()
    else:
        print('EC2 is On, So executing restore process')
        RdsService(config).restore()
        RedshiftService(config).restore()
        SecurityGroup(config).update()

    return 'Python backup / restore processed successfully.'

# lambda_handler({
#     "organizationIdentifier": "bbl33322274",
#     "redshiftClusterIdentifier": "developerorganization-bbl33322274-redshiftcluster-vennmhjbrp03",
#     "redshiftVpcSecurityGroupId": "sg-93eb63ec",
#     "redshiftClusterParameterGroupName": "developerorganization-bbl33322274-redshiftclusterparametergroup-16sf84ismq0w2",
#     "redshiftClusterSubnetGroupName": "developerorganization-bbl33322274-redshiftclustersubnetgroup-1ufs1ucnbxnw",
#     "rdsInstanceIdentifier": "bbl33322274-sql",
#     "rdsVPCSecurityGroup": "sg-23ea625c",
#     "rdsSubnetGroupName": "developerorganization-bbl33322274-rdsdbsubnetgroup-13bwww5ii6rg1",
#     "numberOfSnapshots": 2,
#     "webEC2Id": "i-092ce2aad65a9edae",
#     "startEc2Forcefully": False,
# }, None)

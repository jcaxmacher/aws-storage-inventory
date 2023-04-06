from operator import attrgetter
import datetime
from dateutil.tz import tzutc
import boto3


def get_ec2_instance_size(session, account_id, region_name, instance_id):
    ec2 = session.client('ec2', region_name=region_name)
    volume_ids = []
    response = ec2.describe_instances(InstanceIds=[instance_id])
    for reservation in response.get('Reservations', []):
        for instance in reservation.get('Instances', []):
            for block_device_mapping in instance.get('BlockDeviceMappings', []):
                volume_ids.append(block_device_mapping['Ebs']['VolumeId'])
    response = ec2.describe_volumes(VolumeIds=volume_ids)
    volumes = []
    for volume in response.get('Volumes', []):
        volumes.append([
            account_id,
            region_name,
            f"{instance_id}::{volume['VolumeId']}",
            volume['Size'] * 1024 * 1024 * 1024 # Convert to bytes,
        ])
    return volumes


def get_rds_cluster_size(session, account_id, region_name, db_cluster_identifier):
    rds = session.client('rds', region_name=region_name)
    response = rds.describe_db_clusters(DBClusterIdentifier=db_cluster_identifier)
    if not response['DBClusters']:
        raise Exception(f'DB Cluster {db_cluster_identifier} was not found')
    cluster = response['DBClusters'][0]
    engine = cluster['Engine']
    if not engine.startswith('aurora-'):
        return [
            account_id,
            region_name,
            db_cluster_identifier,
            cluster['AllocatedStorage'] * 1024 * 1024 * 1024 # Convert to bytes
        ]
    metric_data = []
    period = datetime.timedelta(minutes=30)
    end_time = datetime.datetime.now(tzutc())
    start_time = end_time - period
    cloudwatch = session.client('cloudwatch', region_name=region_name)
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/RDS',
        MetricName='VolumeBytesUsed',
        Dimensions=[{
            'Name': 'DBClusterIdentifier',
            'Value': db_cluster_identifier
        }],
        StartTime=start_time,
        EndTime=end_time,
        Statistics=['Maximum'],
        Period=int(period.total_seconds()),
        Unit='Bytes'
    )
    if not response['Datapoints']:
        raise Exception(f'The VolumeBytesUsed metric was not found for DB cluster {db_cluster_identifier}')
    datapoints = sorted(response['Datapoints'], key=lambda d: d['Timestamp'], reverse=True)
    return [
        account_id,
        region_name,
        db_cluster_identifier,
        int(datapoints[0]['Maximum'])
    ]


def get_rds_instance_size(session, account_id, region_name, db_instance_identifier):
    rds = session.client('rds', region_name=region_name)
    response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
    if not response['DBInstances']:
        raise Exception(f'DB Instance {db_instance_identifier} was not found')
    instance = response['DBInstances'][0]
    return [
        account_id,
        region_name,
        db_instance_identifier,
        instance['AllocatedStorage'] * 1024 * 1024 * 1024 # Convert to bytes
    ]


def get_efs_file_system_size(session, account_id, region_name, file_system_id):
    efs = session.client('efs', region_name=region_name)
    response = efs.describe_file_systems(
        FileSystemId=file_system_id
    )
    if not response['FileSystems']:
        raise Exception(f'File system {file_system_id} in region {region_name} could not be found')
    file_system_size = response['FileSystems'][0]['SizeInBytes']['Value']
    return [
        account_id,
        region_name,
        file_system_id,
        file_system_size
    ]


def get_dynamodb_table_size(session, account_id, region_name, table_name):
    dynamodb = session.client('dynamodb', region_name=region_name)
    response = dynamodb.describe_table(TableName=table_name)
    table = response['Table']
    return [
        account_id,
        region_name,
        table_name,
        table['TableSizeBytes']
    ]


def get_redshift_cluster_size(session, account_id, region_name, cluster_identifier):
    redshift = session.client('redshift', region_name=region_name)
    now = datetime.datetime.now(tzutc())
    start_time = now - datetime.timedelta(days=7)
    response = redshift.describe_cluster_snapshots(
        ClusterIdentifier=cluster_identifier,
        StartTime=start_time,
        SortingEntities=[{
            'Attribute': 'CREATE_TIME',
            'SortOrder': 'DESC'
        }]
    )
    snapshots = sorted(response['Snapshots'], key=lambda s: s['SnapshotCreateTime'], reverse=True)
    if not snapshots:
        raise Exception(f'Cluster {cluster_identifier} does not have any snapshots in the last seven days')
    last_snapshot = snapshots[0]
    return [
        account_id,
        region_name, 
        cluster_identifier,
        last_snapshot['TotalBackupSizeInMegaBytes'] * 1024 * 1024 # Convert to bytes
    ]


def get_bucket_size(session, account_id, region_name, bucket_name):
    s3 = session.client('s3', region_name=region_name)
    try:
        response = s3.get_bucket_location(Bucket=bucket_name)
        location_constraint = response.get('LocationConstraint') or 'us-east-1'
        if location_constraint != region_name:
            raise Exception(f'Bucket {bucket_name} is not in Region {region_name}')
    except Exception as exc:
        raise Exception(f'Bucket {bucket_name} was not found in Region {region_name} or credentials are invalid. Inner exception {exc}')
    cloudwatch = session.client('cloudwatch', region_name=region_name)
    backup_storage_types = [
        'StandardStorage',
        'StandardIAStorage', 
        'OneZoneIAStorage',
        'GlacierInstantRetrievalStorage',
        'IntelligentTieringFAStorage',
        'IntelligentTieringIAStorage',
        'IntelligentTieringAAStorage',
        'IntelligentTieringAIAStorage',
        'IntelligentTieringDAAStorage'
    ]
    other_storage_types = [
        'StandardIASizeOverhead',
        'StandardIAObjectOverhead',
        'OneZoneIASizeOverhead',
        'ReducedRedundancyStorage',
        'GlacierInstantRetrievalSizeOverhead',
        'GlacierInstantRetrievalStorage',
        'GlacierStorage',
        'GlacierStagingStorage',
        'GlacierObjectOverhead',
        'GlacierS3ObjectOverhead',
        'DeepArchiveStorage',
        'DeepArchiveObjectOverhead',
        'DeepArchiveS3ObjectOverhead',
        'DeepArchiveStagingStorage'
    ]
    paginator = cloudwatch.get_paginator('list_metrics')
    response_iterator = paginator.paginate(
        Namespace='AWS/S3',
        MetricName='BucketSizeBytes',
        Dimensions=[
            {
                'Name': 'BucketName',
                'Value': bucket_name
            }
        ]
    )
    metric_dimensions = []
    for page in response_iterator:
        for metric in page['Metrics']:
            storage_type = [d['Value'] for d in metric['Dimensions'] if d['Name'] == 'StorageType'][0]
            metric_dimensions.append((storage_type, metric['Dimensions']))
    metric_data = []
    period = datetime.timedelta(days=2)
    end_time = datetime.datetime.now(tzutc())
    start_time = end_time - period
    for storage_type, dimensions in metric_dimensions:
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/S3',
            MetricName='BucketSizeBytes',
            Dimensions=dimensions,
            StartTime=start_time,
            EndTime=end_time,
            Statistics=['Average'],
            Period=int(period.total_seconds()),
            Unit='Bytes'
        )
        if storage_type in backup_storage_types:
            metric_data.append([
                account_id, region_name, f'{bucket_name}::{storage_type}', response['Datapoints'][0]['Average']
            ])
    return metric_data

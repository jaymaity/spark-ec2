from boto import ec2
from moto import mock_ec2
import python.aws.security_group as sg
import uuid


class Opt:
    region = 'us-east-1'
    vpc_id = None
    use_existing_master = False
    additional_security_group = False
    authorized_address = "0.0.0.0/0"


@mock_ec2
def test_security_group_no_group_exists():

    conn = ec2.connect_to_region('us-east-1')
    modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
               'mapreduce', 'spark-standalone', 'tachyon']
    opts = Opt()
    opts.region = 'us-east-1'
    opts.vpc_id = None
    opts.use_existing_master = False
    opts.additional_security_group = False
    opts.authorized_address = "0.0.0.0/0"

    cluster_name = "1f23341d-5003-40a9-99b2-a23e1c61b384"  # str(uuid.uuid4())

    sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name)
    master_group, slave_group, additional_group_ids = \
        sec_group.create_apply_security_group_rules()

    # TODO: Write proper test cases
    group1 = conn.get_all_security_groups([master_group.name, slave_group.name])
    print(master_group, slave_group, additional_group_ids)


@mock_ec2
def test_security_group_group_exists():
    # TODO: Write proper test cases
    return None


@mock_ec2
def test_security_group_vnc_exists():
    # TODO: Write proper test cases
    return None


@mock_ec2
def test_security_group_additional_group():
    # TODO: Write proper test cases
    return None

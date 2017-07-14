from unittest import TestCase
from boto import ec2
from moto import mock_ec2
import python.aws.security_group as sg
import uuid
import json


# Creates opt class
class Opt:
    region = 'us-east-1'
    vpc_id = None
    use_existing_master = False
    additional_security_group = False
    authorized_address = "0.0.0.0/0"


class TestSecurityGroup(TestCase):

    @mock_ec2
    def test_get_master_group(self):
        self.fail()

    def test_get_slave_group(self):
        self.fail()

    def test_make_and_get_group(self):
        self.fail()

    def test_create_security_group(self):
        self.fail()

    @staticmethod
    def is_security_rule_exists(protocol, port_from, port_to, security_group):
        if security_group:
            for rule in security_group.rules:
                if protocol == rule.ip_protocol \
                        and ((port_from == int(rule.from_port) and port_to == int(rule.to_port))
                             or protocol == 'icmp'):
                    return True
            return False

    @staticmethod
    def is_inter_security_rule_exists(protocol, port_from, port_to, security_group,
                                      target_group_name):
        if security_group:
            for rule in security_group.rules:
                if protocol == rule.ip_protocol \
                        and ((port_from == int(rule.from_port) and port_to == int(rule.to_port))
                             or protocol == 'icmp'):
                    for auth in rule.grants:
                        if auth.name == target_group_name:
                            return True
            return False

    @staticmethod
    def is_all_security_rule_exists(json_rules, modules, security_group):
        # Test for master security group rules
        for security_row in json_rules:
            protocol = security_row["ip_protocol"]
            port_from = security_row["from_port"]
            port_to = security_row["to_port"]
            for_app = security_row["for_app"]

            if for_app in modules:
                is_rule_exists = TestSecurityGroup.is_security_rule_exists(protocol, port_from,
                                                                           port_to, security_group)
                if not is_rule_exists:
                    return False
        return True

    @staticmethod
    def is_inter_conn_rule_exists(json_rules, security_group, master_group_name, slave_group_name):
        # Test for master security group rules
        for security_row in json_rules:
            protocol = security_row["ip_protocol"]
            port_from = security_row["from_port"]
            port_to = security_row["to_port"]
            target = security_row["target"]
            if target == "master":
                target_group_name = master_group_name
            elif target == "slave":
                target_group_name = slave_group_name

            is_rule_exists = TestSecurityGroup.\
                is_inter_security_rule_exists(protocol, port_from, port_to,
                                              security_group, target_group_name)
            if not is_rule_exists:
                return False
        return True

    @staticmethod
    def is_all_rule_exists(region, master_group_name, slave_group_name, modules, is_delete_groups=True):

        conn = ec2.connect_to_region(region)

        # Get Security group by querying
        master_group = conn.get_all_security_groups([master_group_name])[0]
        slave_group = conn.get_all_security_groups([slave_group_name])[0]

        # Real all rule files
        json_master = json.load(open("../../config/security_group/master.json"))
        json_slave = json.load(open("../../config/security_group/slave.json"))
        json_inter_conn = json.load(open("../../config/security_group/interconn-masterslave.json"))

        is_master_rule_exists = TestSecurityGroup.is_all_security_rule_exists(json_master,
                                                                              modules,
                                                                              master_group)
        is_slave_rule_exists = TestSecurityGroup.is_all_security_rule_exists(json_slave,
                                                                             modules,
                                                                             slave_group)
        is_master_interconn_exists = TestSecurityGroup.is_inter_conn_rule_exists(
            json_inter_conn["master"], master_group, master_group.name, slave_group.name)

        is_slave_interconn_exists = TestSecurityGroup.is_inter_conn_rule_exists(
            json_inter_conn["slave"], slave_group, master_group.name, slave_group.name)

        if is_delete_groups:
            conn.delete_security_group(master_group.name, master_group.id)
            conn.delete_security_group(slave_group.name, slave_group.id)

        if is_master_rule_exists and is_master_interconn_exists \
                and is_slave_rule_exists and is_slave_interconn_exists:
            return True

    @mock_ec2
    def test_new_security_group(self):
        region = 'us-east-1'
        conn = ec2.connect_to_region(region)
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']
        opts = Opt()
        opts.region = region
        opts.vpc_id = None
        opts.use_existing_master = False
        opts.additional_security_group = False
        opts.authorized_address = "0.0.0.0/0"

        cluster_name = str(uuid.uuid4())

        # Creates the security group
        sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name)
        master_group, slave_group, additional_group_ids = \
            sec_group.create_security_group()

        # Check all the security rules: if it exists in recently
        # created group for the modules selected from the json files
        all_rules_in_secgroup = TestSecurityGroup.is_all_rule_exists(region, master_group.name, slave_group.name, modules)

        self.assertEqual(all_rules_in_secgroup, True)

    def test_existing_security_group_override(self):
        region = 'us-east-1'
        conn = ec2.connect_to_region(region)
        opts = Opt()
        opts.region = region
        opts.vpc_id = None
        opts.use_existing_master = False
        opts.additional_security_group = False
        opts.authorized_address = "0.0.0.0/0"

        cluster_name = str(uuid.uuid4())

        # Creates the security group with less modules
        modules = ['ssh', 'spark', 'ephemeral-hdfs']
        sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name)
        master_group, slave_group, additional_group_ids = \
            sec_group.create_security_group()

        # Test if the group is created for the first time
        all_rules_in_secgroup = TestSecurityGroup. \
            is_all_rule_exists(region, master_group.name, slave_group.name, modules, False)
        self.assertEqual(all_rules_in_secgroup, True)

        # 2nd part of the test with existing security group
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']
        # Creates the group for the second time with more modules
        # So that more rules can be added
        sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name, is_override=True)
        master_group, slave_group, additional_group_ids = \
            sec_group.create_security_group()

        all_rules_in_secgroup = TestSecurityGroup. \
            is_all_rule_exists(region, master_group.name, slave_group.name, modules)
        self.assertEqual(all_rules_in_secgroup, True)

    def test_existing_security_group_no_override(self):
        region = 'us-east-1'
        conn = ec2.connect_to_region(region)
        opts = Opt()
        opts.region = region
        opts.vpc_id = None
        opts.use_existing_master = False
        opts.additional_security_group = False
        opts.authorized_address = "0.0.0.0/0"

        cluster_name = str(uuid.uuid4())

        # Creates the security group with less modules
        modules = ['ssh', 'spark', 'ephemeral-hdfs']
        sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name)
        master_group, slave_group, additional_group_ids = \
            sec_group.create_security_group()

        # Test if the group is created for the first time
        all_rules_in_secgroup = TestSecurityGroup.\
            is_all_rule_exists(region, master_group.name, slave_group.name, modules, False)
        self.assertEqual(all_rules_in_secgroup, True)

        # 2nd part of the test with existing security group
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']

        with self.assertRaises(Exception) as context:
            # Creates the group for the second time with more modules
            # So that more rules can be added
            sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name, is_override=False)
            sec_group.create_security_group()

        # Deletes all security groups
        conn.delete_security_group(master_group.name, master_group.id)
        conn.delete_security_group(slave_group.name, slave_group.id)

        self.assertTrue(context.exception, "Security Group Exists")

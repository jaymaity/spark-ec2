from unittest import TestCase
from boto import ec2
from moto import mock_ec2
import python.aws.security_group as sg
import uuid
import json
import os

# Creates opt class
class Opt:
    REGION = 'us-east-1'
    vpc_id = None
    use_existing_master = False
    additional_security_group = False
    authorized_address = "0.0.0.0/0"


AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Configure common parameters for security group options
REGION = 'us-east-1'
CONNEC2 = ec2.connect_to_region(REGION, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
MODULES = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
            'mapreduce', 'spark-standalone', 'tachyon']
OPTS = Opt()
OPTS.region = REGION
OPTS.vpc_id = None
OPTS.use_existing_master = False
OPTS.additional_security_group = False
OPTS.authorized_address = "0.0.0.0/0"


class TestSecurityGroup(TestCase):

    @staticmethod
    def is_security_rule_exists(protocol, port_from, port_to, security_group):
        """
        Checks for a particular security group exists or not
        :param protocol: Name of the protocol
        :param port_from: Starting port (in Case of ICMP it could be anything)
        :param port_to: End port
        :param security_group: Security group object to test
        :return:
        """
        if security_group:
            for rule in security_group.rules:
                # If it is ICMP protocol, we do not need to check the ports otherwise we have to
                if protocol == rule.ip_protocol \
                        and ((port_from == int(rule.from_port) and port_to == int(rule.to_port))
                             or protocol == 'icmp'):
                    return True
            return False

    @staticmethod
    def is_inter_security_rule_exists(protocol, port_from, port_to, security_group,
                                      target_group_id):
        """
        Check if security rules among security groups exists or not
        :param protocol: Name of the protocol
        :param port_from: Starting port (in Case of ICMP it could be anything)
        :param port_to: End port
        :param security_group: Security group object to test
        :param target_group_id: Id of the target security group to check
        connection between them
        :return: Boolean
        """
        if security_group:
            for rule in security_group.rules:
                if protocol == rule.ip_protocol \
                        and ((port_from == int(rule.from_port) and port_to == int(rule.to_port))
                             or protocol == 'icmp'):
                    for auth in rule.grants:
                        if auth.group_id == target_group_id:
                            return True
            return False

    @staticmethod
    def is_all_security_rule_exists(json_rules, modules, security_group):
        """
        Checks for all security rules for a module exists or not
        :param json_rules: JSON rules from the files
        :param modules: Name of the modules to check the security rules for
        :param security_group: Security group object to check
        :return: Boolean
        """
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
    def is_inter_conn_rule_exists(json_rules, security_group, master_group_id, slave_group_id):
        """
        Check all security rule for inter conn module exists or not
        :param json_rules: JSON rule from inter con file
        :param security_group: Security group object to check
        :param master_group_id: Id of the master group to check
        :param slave_group_id: Id of the slave group to check
        :return: boolean value based on rule exists or not
        """
        # Test for master security group rules
        for security_row in json_rules:
            protocol = security_row["ip_protocol"]
            port_from = security_row["from_port"]
            port_to = security_row["to_port"]
            target = security_row["target"]
            if target == "master":
                target_group_id = master_group_id
            elif target == "slave":
                target_group_id = slave_group_id

            is_rule_exists = TestSecurityGroup.\
                is_inter_security_rule_exists(protocol, port_from, port_to,
                                              security_group, target_group_id)
            if not is_rule_exists:
                return False
        return True

    @staticmethod
    def is_all_rule_exists(region, master_group_name, slave_group_name,
                           modules, cluster_name, is_delete_groups=True):
        """
        Overall check for all security rules for a list of moduless
        :param region: Name of the region to connect ex:east-us-1
        :param master_group_name: Name of the master group
        :param slave_group_name: Name of the slave group
        :param modules: Name of the modules to check for
        :param is_delete_groups: Confirmation delete the groups after checking
        :return: Return True if the rules exist
        """
        conn = ec2.connect_to_region(region, aws_access_key_id=AWS_ACCESS_KEY_ID,
                                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

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
            json_inter_conn["master"], master_group, master_group.id, slave_group.id)

        is_slave_interconn_exists = TestSecurityGroup.is_inter_conn_rule_exists(
            json_inter_conn["slave"], slave_group, master_group.id, slave_group.id)

        if is_delete_groups:
            sg.destroy(conn, cluster_name)

        if is_master_rule_exists and is_master_interconn_exists \
                and is_slave_rule_exists and is_slave_interconn_exists:
            return True

    def test_new_security_group(self):
        """
        Test creation of brand new security group
        :return:
        """
        cluster_name = str(uuid.uuid4())

        # Creates the security group
        master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name)

        # Check all the security rules: if it exists in recently
        # created group for the modules selected from the json files
        all_rules_in_secgroup = TestSecurityGroup.is_all_rule_exists(REGION, master_group.name,
                                                                     slave_group.name, MODULES,
                                                                     cluster_name)

        self.assertEqual(all_rules_in_secgroup, True)

    def test_get_master_slave_group_not_exists(self):
        """
        Test getting master and slave group when it does not exists previously
        """
        cluster_name = str(uuid.uuid4())

        # Creates the security group
        master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name)
        is_rule_passed = TestSecurityGroup.is_all_rule_exists(REGION, master_group.name,
                                                              slave_group.name, MODULES,
                                                              cluster_name,
                                                              is_delete_groups=True)
        self.assertEquals(is_rule_passed, True)

    def test_existing_security_group_override(self):
        """
        Test Existing group with rule override
        :return:
        """
        cluster_name = str(uuid.uuid4())

        # Creates the security group with less modules
        modules = ['ssh', 'spark', 'ephemeral-hdfs']
        master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name)

        # Test if the group is created for the first time
        all_rules_in_secgroup = TestSecurityGroup. \
            is_all_rule_exists(REGION, master_group.name, slave_group.name, modules, cluster_name,
                               False)
        self.assertEqual(all_rules_in_secgroup, True)

        # 2nd part of the test with existing security group
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']
        # Creates the group for the second time with more modules
        # So that more rules can be added
        master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name,
                                                                    is_override=True)

        all_rules_in_secgroup = TestSecurityGroup. \
            is_all_rule_exists(REGION, master_group.name, slave_group.name, modules, cluster_name)
        self.assertEqual(all_rules_in_secgroup, True)

    def test_existing_security_group_no_override(self):
        """
        Test Existing security group without any override
        Ideally it should throw exception
        :return:
        """
        cluster_name = str(uuid.uuid4())

        # Creates the security group with less modules
        modules = ['ssh', 'spark', 'ephemeral-hdfs']
        master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name)

        # Test if the group is created for the first time
        all_rules_in_secgroup = TestSecurityGroup.\
            is_all_rule_exists(REGION, master_group.name, slave_group.name, modules, cluster_name, False)
        self.assertEqual(all_rules_in_secgroup, True)

        # 2nd part of the test with existing security group
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']

        with self.assertRaises(Exception) as context:
            # Creates the group for the second time with more modules
            # So that more rules can be added
            sg.create(CONNEC2, modules, OPTS, cluster_name, is_override=False)

        # Deletes all security groups
        sg.destroy(CONNEC2, cluster_name)

        self.assertTrue(context.exception, "Security Group Exists")

    def test_destroy_existing_sec_group(self):
        """
        Create and destroy cluster security group
        :return:
        """
        cluster_name = str(uuid.uuid4())

        # Creates security group to delete
        sg.create(CONNEC2, MODULES, OPTS, cluster_name)

        with self.assertRaises(Exception) as context:
            # Wait time is less, so that it will be highly unlikely that it can be destroyed
            sg.destroy(CONNEC2, cluster_name, total_wait=0)

        # clean up , putting enough time to propagate
        # Most likely it's going to finish much earlier
        is_time_delete = sg.destroy(CONNEC2, cluster_name, total_wait=600)

        self.assertTrue(is_time_delete)
        self.assertTrue(context.exception, "We couldn't able to delete the groups in time.")
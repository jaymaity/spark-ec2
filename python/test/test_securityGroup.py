from unittest import TestCase
from boto import ec2
from moto import mock_ec2
import python.aws.security_group as sg
import uuid
import json


# Creates opt class
class Opt:
    REGION = 'us-east-1'
    vpc_id = None
    use_existing_master = False
    additional_security_group = False
    authorized_address = "0.0.0.0/0"

# Configure common parameters for security group options
REGION = 'us-east-1'
CONNEC2 = ec2.connect_to_region(REGION)
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
        :param protocol:
        :param port_from:
        :param port_to:
        :param security_group:
        :return:
        """
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
        """
        Check if security rules among security groups exists or not
        :param protocol:
        :param port_from:
        :param port_to:
        :param security_group:
        :param target_group_name:
        :return:
        """
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
        """
        Checks for all security rules for a module exists or not
        :param json_rules:
        :param modules:
        :param security_group:
        :return:
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
    def is_inter_conn_rule_exists(json_rules, security_group, master_group_name, slave_group_name):
        """
        Check all security rule for inter conn module exists or not
        :param json_rules:
        :param security_group:
        :param master_group_name:
        :param slave_group_name:
        :return:
        """
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
        """
        Overall check for all security rules for a list of moduless
        :param region:
        :param master_group_name:
        :param slave_group_name:
        :param modules:
        :param is_delete_groups:
        :return:
        """
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
            sg.destroy()

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
        all_rules_in_secgroup = TestSecurityGroup.is_all_rule_exists(REGION, master_group.name, slave_group.name, MODULES)

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
            is_all_rule_exists(REGION, master_group.name, slave_group.name, modules, False)
        self.assertEqual(all_rules_in_secgroup, True)

        # 2nd part of the test with existing security group
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']
        # Creates the group for the second time with more modules
        # So that more rules can be added
        master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name,
                                                                    is_override=True)

        all_rules_in_secgroup = TestSecurityGroup. \
            is_all_rule_exists(REGION, master_group.name, slave_group.name, modules)
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
            is_all_rule_exists(REGION, master_group.name, slave_group.name, modules, False)
        self.assertEqual(all_rules_in_secgroup, True)

        # 2nd part of the test with existing security group
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']

        with self.assertRaises(Exception) as context:
            # Creates the group for the second time with more modules
            # So that more rules can be added
            master_group, slave_group, additional_group_ids = sg.create(CONNEC2, MODULES, OPTS, cluster_name,
                                                                        is_override=False)

        # Deletes all security groups
        CONNEC2.delete_security_group(master_group.name, master_group.id)
        CONNEC2.delete_security_group(slave_group.name, slave_group.id)

        self.assertTrue(context.exception, "Security Group Exists")

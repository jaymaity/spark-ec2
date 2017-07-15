from __future__ import division, print_function, with_statement
import sys
from sys import stderr
import json


def get_master_name(cluster_name):
    return cluster_name + "-master"


def get_slave_name(cluster_name):
    return cluster_name + "-slave"


def create(conn, modules, opts, cluster_name, is_override=False,
           existing_master=None, existing_slaves=None,
           inter_security_rules="../../config/security_group/interconn-masterslave.json"
           ):
    """
    Creates security group for the cluster
    :param conn:
    :param modules:
    :param opts:
    :param cluster_name:
    :param is_override:
    :param existing_master:
    :param existing_slaves:
    :param inter_security_rules:
    :return:
    """
    sg_grp = SecurityGroup(conn, modules, opts, cluster_name, is_override)
    return sg_grp.create_security_group(existing_master, existing_slaves, inter_security_rules)


def __remove_all_rules(group):
    """
    Remove all rules from a group
    :param group:
    :return:
    """
    success = True
    for rule in group.rules:
        for grant in rule.grants:
            success &= group.revoke(ip_protocol=rule.ip_protocol,
                                    from_port=rule.from_port,
                                    to_port=rule.to_port,
                                    src_group=grant)
    return success


def destroy(conn, cluster_name):
    """
    Destroy security groups for the cluster
    :param conn:
    :param cluster_name:
    :return:
    """
    # Get Security group by querying
    master_group = conn.get_all_security_groups(
        [get_master_name(cluster_name)])[0]
    slave_group = conn.get_all_security_groups(
        [get_slave_name(cluster_name)])[0]

    __remove_all_rules(master_group)
    __remove_all_rules(slave_group)
    import time
    time.sleep(30)
    conn.delete_security_group(master_group.name, master_group.id)
    conn.delete_security_group(slave_group.name, slave_group.id)




class SecurityGroup(object):
    """
    Security group implementation
    """

    def __init__(self, conn, modules, opts, cluster_name, is_override=False):
        """
        Initialize all variables necessary
        :param conn:
        :param modules:
        :param opts:
        """
        self.conn = conn
        self.modules = modules
        self.opts = opts
        self.cluster_name = cluster_name

        # Creates master and slave security group name
        self.__master_group = self.__make_and_get_group(get_master_name(cluster_name),
                                                        is_override)
        self.__slave_group = self.__make_and_get_group(get_slave_name(cluster_name),
                                                       is_override)

    def get_master_group(self):
        """
        Get master group object
        :return:
        """
        return self.__master_group

    def get_slave_group(self):
        """
        Get Slave group object
        :return:
        """
        return self.__slave_group

    def __make_and_get_group(self, name,
                             override_existing=True,
                             description="Spark EC2 group"):
        """
        Get the EC2 security group of the given name, creates it, if it doesn't exist
        Example: ec2.connect_to_region
        :param name: Name of the group
        :param override_existing: Make sure we override existing security group
        :param description: Description of the security group
        :return:
        """
        groups = self.conn.get_all_security_groups()
        # Find all the groups with same group name
        group = [g for g in groups if g.name == name]
        if len(group) > 0:
            if override_existing:
                print("Security group already exists. Adding the rules needed with this group")
                return group[0]
            else:
                raise Exception("Security Group Exists")
        else:
            print("Creating security group " + name)
            return self.conn.create_security_group(name, description, self.opts.vpc_id)

    def __apply_inter_security_group_rules(self, connection_list, group_to_apply):
        """
        Apply interconnection security rules among master slave interaction
        :param connection_list:
        :param group_to_apply:
        :return:
        """
        master_inter_conn = connection_list[group_to_apply]

        # Finding right group to choose to apply the rules
        if group_to_apply == "master":
            src_group = self.__master_group
        elif group_to_apply == "slave":
            src_group = self.__slave_group

        # Applying rules for each rule
        for security_row in master_inter_conn:
            protocol = security_row["ip_protocol"]
            port_from = security_row["from_port"]
            port_to = security_row["to_port"]
            group_to_authorize = security_row["target"]

            # Choosing source group
            if group_to_authorize == "master":
                target_group = self.__master_group
            elif group_to_authorize == "slave":
                target_group = self.__slave_group
            else:
                print("Sorry we do not know where should we apply security group with:\n"
                      "Protocol: {0}\tStart Port:{1}\tEnd Port:{2}\tSource Type:{3}".
                      format(protocol, port_from, port_to, group_to_authorize))

            src_group.authorize(ip_protocol=protocol,
                                from_port=port_from,
                                to_port=port_to,
                                src_group=target_group)

    def __apply_individual_security_group_rules(self, security_group, connection_list):
        """
        Apply security rules within group
        :param security_group: Name of the security group
        :param connection_list: List of connection in JSON format
        :return:
        """
        for security_row in connection_list:
            protocol = security_row["ip_protocol"]
            port_from = security_row["from_port"]
            port_to = security_row["to_port"]
            for_app = security_row["for_app"]

            if for_app in self.modules:
                security_group.authorize(protocol, port_from,
                                         port_to, self.opts.authorized_address)

    def __apply_master_group_rules(self, inter_conn,
                                   master_rules_file="../../config/security_group/master.json"):
        """
        Configure and apply all slave group rules
        :param inter_conn:
        :param master_rules_file: JSON file containing all the rules for master
        :return:
        """
        self.__apply_inter_security_group_rules(inter_conn, "master")
        master_conn = json.load(open(master_rules_file))

        # Apply security group rules in master security group
        self.__apply_individual_security_group_rules(self.__master_group, master_conn)

    def __apply_slave_group_rules(self, inter_conn,
                                  slave_rules_file="../..//config/security_group/slave.json"):
        """
        Configure and apply all master group rules
        :param inter_conn:
        :param slave_rules_file: JSON file for security rules for slave
        :return:
        """
        self.__apply_inter_security_group_rules(inter_conn, "slave")
        slave_conn = json.load(open(slave_rules_file))

        # Apply security group rules in master security group
        self.__apply_individual_security_group_rules(self.__slave_group, slave_conn)

    def create_security_group(self, existing_masters=None,
                              existing_slaves=None,
                              inter_security_rules="../../config/security_group/interconn-masterslave.json"):
        """
        Creates and apply security groups
        :param existing_masters:
        :param existing_slaves:
        :param inter_security_rules: Rules for communication between master and slaves
        :return:
        """
        # If there is already some instances running on that security group
        if existing_slaves or (existing_masters and not self.opts.use_existing_master):
            print("ERROR: There are already instances running in group %s or %s" %
                  (self.__master_group.name, self.__slave_group.name), file=stderr)
            sys.exit(1)

        # Loads inter connection security rules
        fp_inter_conn = open(inter_security_rules)
        inter_conn = json.load(fp_inter_conn)

        if len(self.__master_group.rules) == 0:  # Group was just now created
            self.__apply_master_group_rules(inter_conn)

        if len(self.__slave_group.rules) == 0:  # Group was just now created
            self.__apply_slave_group_rules(inter_conn)

        # we use group ids to work around https://github.com/boto/boto/issues/350
        additional_group_ids = []
        if self.opts.additional_security_group:
            additional_group_ids = [sg.id
                                    for sg in self.conn.get_all_security_groups()
                                    if self.opts.additional_security_group in (sg.name, sg.id)]

        return self.__master_group, self.__slave_group, additional_group_ids

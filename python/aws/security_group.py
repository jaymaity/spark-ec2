import sys
from sys import stderr
import json

SECURITY_GROUP_DESCRIPTION = "Spark EC2 group"


def get_master_group_name(cluster_name):
    return cluster_name + "-master"


def get_slave_group_name(cluster_name):
    return cluster_name + "-slave"


def get_or_make_group(conn, name, vpc_id):
    """
    Get the EC2 security group of the given name, creating it if it doesn't exist
    :param conn:
    :param name:
    :param vpc_id:
    :return:
    """
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print("Creating security group " + name)
        return conn.create_security_group(name, SECURITY_GROUP_DESCRIPTION, vpc_id)


class SecurityGroup(object):
    """
    Security group implementation
    """

    __master_group = None
    __slave_group = None

    def __init__(self, conn, modules, opts, cluster_name):
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
        self.__master_group = get_or_make_group(self.conn, get_master_group_name(cluster_name), self.opts.vpc_id)
        self.__slave_group = get_or_make_group(self.conn, get_slave_group_name(cluster_name), self.opts.vpc_id)

    def __apply_inter_security_group_rules(self, connection_list, group_to_apply):
        """
        Apply interconnection security rules among master slave interaction
        :param connection_list:
        :param group_to_apply:
        :return:
        """
        master_inter_conn = connection_list[group_to_apply]

        for security_row in master_inter_conn:
            protocol = security_row["ip_protocol"]
            port_from = security_row["port_from"]
            port_to = security_row["port_to"]
            group_to_authorize = security_row["target"]

            if group_to_authorize == "master":
                src_group = self.__master_group
            elif group_to_authorize == "slave":
                src_group = self.__slave_group
            else:
                print("Sorry we do not know where should we apply security group with:\n"
                      "Protocol: {0}\tStart Port:{1}\tEnd Port:{2}\tSource Type:{3}".
                      format(protocol, port_from, port_to, group_to_authorize))

            self.__master_group.authorize(ip_protocol=protocol,
                                          from_port=-port_from,
                                          to_port=-port_to,
                                          src_group=src_group)

    def __apply_individual_security_group_rules(self, security_group, connection_list):
        """
        Apply security rules within group
        :param security_group:
        :param connection_list:
        :param connection_list:
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

    def __apply_master_group_rules(self, inter_conn):
        """
        Configure and apply all slave group rules
        :param inter_conn:
        :return:
        """
        if self.opts.vpc_id is None:
            self.__master_group.authorize(src_group=self.__master_group)
            self.__master_group.authorize(src_group=self.__slave_group)
        else:
            self.__apply_inter_security_group_rules(inter_conn, "master")

        fp_master_conn = open("../../config/security_group/master.json")
        master_conn = json.load(fp_master_conn)

        # Apply security group rules in master security group
        self.__apply_individual_security_group_rules(self.__master_group, master_conn)

    def __apply_slave_group_rules(self, inter_conn):
        """
        Configure and apply all master group rules
        :param inter_conn:
        :return:
        """
        if self.opts.vpc_id is None:
            self.__slave_group.authorize(src_group=self.__master_group)
            self.__slave_group.authorize(src_group=self.__slave_group)
        else:
            self.__apply_inter_security_group_rules(inter_conn, "slave")

        fp_master_conn = open("../..//config/security_group/slave.json")
        slave_conn = json.load(fp_master_conn)

        # Apply security group rules in master security group
        self.__apply_individual_security_group_rules(self.__slave_group, slave_conn)

    def create_apply_security_group_rules(self, existing_masters=None, existing_slaves=None):
        """
        Creates and apply security groups
        :param existing_masters:
        :param existing_slaves:
        :return:
        """
        # If there is already some instances running on that security group
        if existing_slaves or (existing_masters and not self.opts.use_existing_master):
            print("ERROR: There are already instances running in group %s or %s" %
                  (self.__master_group.name, self.__slave_group.name), file=stderr)
            sys.exit(1)

        # Loads inter connection security rules
        fp_inter_conn = open("../../config/security_group/interconn-masterslave.json")
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

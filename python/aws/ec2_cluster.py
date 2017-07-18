"""Manages EC2 Instances for cluster"""
from __future__ import division, print_function, with_statement
import itertools
import random
import string
import sys
from sys import stderr
import time
import python.aws.security_group as sg_grp
import python.aws.config as aws_config
import python.aws.common as aws_common
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType


def get_instances_by_groupnames(conn, sec_group_names):
    """
    Get all non-terminated instances that belong to any of the provided security groups.
    EC2 reservation filters and instance states are documented here:
        http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
    """
    reservations = conn.get_all_reservations(
        filters={"instance.group-name": sec_group_names})
    instances = itertools.chain.from_iterable(r.instances for r in reservations)
    return [i for i in instances if i.state not in ["shutting-down", "terminated"]]


def get_existing_cluster(conn, cluster_name, die_on_error=True):
    """
    Get the EC2 instances in an existing cluster if available.
    Returns a tuple of lists of EC2 instance objects for the masters and slaves.
    """

    region = conn.region.name
    print("Searching for existing cluster {c} in region {r}...".format(
        c=cluster_name, r=region))

    master_instances = get_instances_by_groupnames(
        conn,
        [sg_grp.get_master_name(cluster_name)])
    slave_instances = get_instances_by_groupnames(
        conn,
        [sg_grp.get_slave_name(cluster_name)])

    if any((master_instances, slave_instances)):
        print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
            m=len(master_instances),
            plural_m=('' if len(master_instances) == 1 else 's'),
            s=len(slave_instances),
            plural_s=('' if len(slave_instances) == 1 else 's')))

    if not master_instances and die_on_error:
        print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
            c=cluster_name, r=region), file=sys.stderr)
        sys.exit(1)

    return master_instances, slave_instances


def launch(conn, opts, cluster_name, master_group,
           slave_group, additional_group_ids=None,
           time_wait_to_propagate=15):
    """
    Launch cluster instances in ec2
    :param conn:
    :param opts:
    :param cluster_name:
    :param master_group:
    :param slave_group:
    :param additional_group_ids:
    :param time_wait_to_propagate:
    :return:
    """
    launch_ins = LaunchInstances(conn, opts, master_group, slave_group,
                                 additional_group_ids, cluster_name)
    return launch_ins.launch_instances(time_wait_to_propagate)


def __execute_inst_operation(action, inst):
    """
    Does EC2 operations based on input
    :param action:
    :param inst:
    :return:
    """
    if inst.state not in ["shutting-down", "terminated"]:
        if action == "start":
            inst.start()
        elif action == "stop":
            inst.stop()
        elif action == "reboot":
            inst.reboot()
        elif action == "destroy":
            inst.terminate()
        else:
            raise Exception("Unknown action on ec2 instances")
    else:
        raise Exception("Cannot take action as it is shutting-down or terminated")


def __execute_ops_on_cluster(conn, action, cluster_name,
                             action_on_masters=True, action_on_slaves=True):
    """
    Execute desired operation on cluster
    :param conn:
    :param action:
    :param cluster_name:
    :param action_on_masters:
    :param action_on_slaves:
    :return:
    """
    master_nodes, slave_nodes = get_existing_cluster(conn, cluster_name)
    print(action + "ing master...")
    if action_on_masters:
        for inst in master_nodes:
            __execute_inst_operation(action, inst)

    print(action + "ing slaves...")
    if action_on_slaves:
        for inst in slave_nodes:
            __execute_inst_operation(action, inst)


def __get_ec2_states(group_nodes):
    """
    Get single state for all the group
    if it is mixed, it will return the string "mixed"
    :param group_nodes:
    :return:
    """
    ec2_status = None
    for inst in group_nodes:
        if inst.state == ec2_status or ec2_status is None:
            ec2_status = inst.state
        else:
            return "mixed"
    return ec2_status


def get_state(conn, cluster_name, group_type="both"):
    """
    Get State of all instances
    This will return 8 possible string
    rebooting, pending, running, shutting-down, terminated,
    stopping, stopped and mixed
    :param conn:
    :param cluster_name:
    :param group_type:
    :return:
    """
    master_nodes, slave_nodes = get_existing_cluster(conn, cluster_name, False)

    master_state = None
    slave_state = None

    if group_type == "both" or group_type == "master":
        master_state = __get_ec2_states(master_nodes)

    if group_type == "both" or group_type == "slave":
        slave_state = __get_ec2_states(slave_nodes)

    if group_type == "both" and slave_state == master_state \
            and slave_state != "mixed" and master_state != "mixed":
        return slave_state
    else:
        return "mixed"


def destroy(conn, cluster_name,
            action_on_masters=True, action_on_slaves=True):
    """
    Destroy all instances
    :param conn:
    :param cluster_name:
    :param action_on_masters:
    :param action_on_slaves:
    :return:
    """
    __execute_ops_on_cluster(conn, "destroy", cluster_name,
                             action_on_masters, action_on_slaves)


def stop(conn, cluster_name,
         action_on_masters=True, action_on_slaves=True):
    """
    Stop all instances
    :param conn:
    :param cluster_name:
    :param action_on_masters:
    :param action_on_slaves:
    :return:
    """
    __execute_ops_on_cluster(conn, "stop", cluster_name,
                             action_on_masters, action_on_slaves)


def start(conn, cluster_name,
          action_on_masters=True, action_on_slaves=True):
    """
    start all instances
    :param conn:
    :param cluster_name:
    :param action_on_masters:
    :param action_on_slaves:
    :return:
    """
    __execute_ops_on_cluster(conn, "start", cluster_name,
                             action_on_masters, action_on_slaves)


def reboot(conn, cluster_name,
           action_on_masters=True, action_on_slaves=True):
    """
    Reboot all instances
    :param conn:
    :param cluster_name:
    :param action_on_masters:
    :param action_on_slaves:
    :return:
    """
    __execute_ops_on_cluster(conn, "reboot", cluster_name,
                             action_on_masters, action_on_slaves)


class LaunchInstances(object):
    """
    Responsible for launching EC2 instances
    """

    def __init__(self, conn, opts, master_group, slave_group, additional_group_ids, cluster_name):
        """
        Initialize EC2 Launch with variables
        :param conn:
        :param opts:
        :param master_group:
        :param slave_group:
        :param additional_group_ids:
        :param cluster_name:
        """
        self.conn = conn
        self.opts = opts
        self.__master_group = master_group
        self.__slave_group = slave_group
        self.__additional_group_id = additional_group_ids
        self.__block_map = self.__create_block_device_map()
        self.__user_data = self.__get_user_data_content()
        self.cluster_name = cluster_name

    def __get_image_from_ami(self, ami):
        """
        Get EC2 instance image for particular ami
        :param ami:
        :return:
        """
        try:
            image = self.conn.get_image(ami)
        except:
            print("Could not find AMI " + ami, file=stderr)
            sys.exit(1)
        return image

    def __get_user_data_content(self):
        user_data_content = None
        if self.opts.user_data:
            with open(self.opts.user_data) as user_data_file:
                user_data_content = user_data_file.read()
        return user_data_content

    def __create_block_device_map(self):
        """
        Create block device mapping so that we can add EBS volumes if asked to.
        The first drive is attached as /dev/sds, 2nd as /dev/sdt, ... /dev/sdz
        :return:
        """
        block_map = BlockDeviceMapping()
        if self.opts.ebs_vol_size > 0:
            for i in range(self.opts.ebs_vol_num):
                device = EBSBlockDeviceType()
                device.size = self.opts.ebs_vol_size
                device.volume_type = self.opts.ebs_vol_type
                device.delete_on_termination = True
                block_map["/dev/sd" + chr(ord('s') + i)] = device

        # AWS ignores the AMI-specified block device mapping for M3 (see SPARK-3342).
        if self.opts.instance_type.startswith('m3.'):
            for i in range(aws_config.get_num_disks(self.opts.instance_type)):
                dev = BlockDeviceType()
                dev.ephemeral_name = 'ephemeral%d' % i
                # The first ephemeral drive is /dev/sdb.
                name = '/dev/sd' + string.ascii_letters[i + 1]
                block_map[name] = dev

        return block_map

    def __get_slave_spot_instances(self):
        # Launch spot instances with the requested price
        print("Requesting %d slaves as spot instances with price $%.3f" %
              (self.opts.slaves, self.opts.spot_price))
        zones = aws_common.get_zones(self.conn, self.opts)
        num_zones = len(zones)
        i = 0
        my_req_ids = []
        for zone in zones:
            num_slaves_this_zone = aws_common.get_partition(self.opts.slaves, num_zones, i)
            slave_reqs = self.conn.request_spot_instances(
                price=self.opts.spot_price,
                image_id=self.opts.ami,
                launch_group="launch-group-%s" % self.cluster_name,
                placement=zone,
                count=num_slaves_this_zone,
                key_name=self.opts.key_pair,
                security_group_ids=[self.__slave_group.id] + self.__additional_group_id,
                instance_type=self.opts.instance_type,
                block_device_map=self.__block_map,
                subnet_id=self.opts.subnet_id,
                placement_group=self.opts.placement_group,
                user_data=self.__user_data,
                instance_profile_name=self.opts.instance_profile_name)
            my_req_ids += [req.id for req in slave_reqs]
            i += 1
        print("Waiting for spot instances to be granted...")
        try:
            while True:
                time.sleep(10)
                reqs = self.conn.get_all_spot_instance_requests()
                id_to_req = {}
                for r in reqs:
                    id_to_req[r.id] = r
                active_instance_ids = []
                for i in my_req_ids:
                    if i in id_to_req and id_to_req[i].state == "active":
                        active_instance_ids.append(id_to_req[i].instance_id)
                if len(active_instance_ids) == self.opts.slaves:
                    print("All %d slaves granted" % self.opts.slaves)
                    reservations = self.conn.get_all_reservations(active_instance_ids)
                    slave_nodes = []
                    for r in reservations:
                        slave_nodes += r.instances
                    break
                else:
                    print("%d of %d slaves granted, waiting longer" % (
                        len(active_instance_ids), self.opts.slaves))
        except:
            print("Canceling spot instance requests")
            self.conn.cancel_spot_instance_requests(my_req_ids)
            # Log a warning if any of these requests actually launched instances:
            (master_nodes, slave_nodes) = get_existing_cluster(
                self.conn, self.cluster_name, die_on_error=False)
            running = len(master_nodes) + len(slave_nodes)
            if running:
                print(("WARNING: %d instances are still running" % running), file=stderr)
            sys.exit(0)
        return slave_nodes, zone

    def __get_slave_non_spot_instances(self):
        """
        Launch non-spot instances
        :return:
        """
        zones = aws_common.get_zones(self.conn, self.opts)
        num_zones = len(zones)
        i = 0
        slave_nodes = []
        image = self.__get_image_from_ami(self.opts.ami)
        for zone in zones:
            num_slaves_this_zone = aws_common.get_partition(self.opts.slaves, num_zones, i)
            if num_slaves_this_zone > 0:
                slave_res = image.run(
                    key_name=self.opts.key_pair,
                    security_group_ids=[self.__slave_group.id] + self.__additional_group_id,
                    instance_type=self.opts.instance_type,
                    placement=zone,
                    min_count=num_slaves_this_zone,
                    max_count=num_slaves_this_zone,
                    block_device_map=self.__block_map,
                    subnet_id=self.opts.subnet_id,
                    placement_group=self.opts.placement_group,
                    user_data=self.__user_data,
                    instance_initiated_shutdown_behavior=
                    self.opts.instance_initiated_shutdown_behavior,
                    instance_profile_name=self.opts.instance_profile_name)
                slave_nodes += slave_res.instances
                print("Launched {s} slave{plural_s} in {z}, regid = {r}".format(
                    s=num_slaves_this_zone,
                    plural_s=('' if num_slaves_this_zone == 1 else 's'),
                    z=zone,
                    r=slave_res.id))
            i += 1

        return slave_nodes, zone

    def __launch_slaves(self):
        """
        Launch All slave instances
        :param cluster_name:
        :return:
        """
        if self.opts.spot_price is not None:
            slave_nodes, zone = self.__get_slave_spot_instances()
        else:
            slave_nodes, zone = self.__get_slave_non_spot_instances()

        return slave_nodes, zone

    def __launch_or_resume_masters(self):
        """
        Launch or resume master instances
        """
        existing_masters, existing_slaves = get_existing_cluster(
            self.conn, self.cluster_name, die_on_error=False)
        image = self.__get_image_from_ami(self.opts.ami)
        if existing_masters:
            print("Starting master...")
            for inst in existing_masters:
                if inst.state not in ["shutting-down", "terminated"]:
                    inst.start()
            master_nodes = existing_masters
        else:
            master_type = self.opts.master_instance_type
            if master_type == "":
                master_type = self.opts.instance_type
            if self.opts.zone == 'all':
                self.opts.zone = random.choice(self.conn.get_all_zones()).name
            master_res = image.run(
                key_name=self.opts.key_pair,
                security_group_ids=[self.__master_group.id] + self.__additional_group_id,
                instance_type=master_type,
                placement=self.opts.zone,
                min_count=1,
                max_count=1,
                block_device_map=self.__block_map,
                subnet_id=self.opts.subnet_id,
                placement_group=self.opts.placement_group,
                user_data=self.__user_data,
                instance_initiated_shutdown_behavior=self.opts.instance_initiated_shutdown_behavior,
                instance_profile_name=self.opts.instance_profile_name)

            master_nodes = master_res.instances
            print("Launched master in %s, regid = %s" % (self.opts.zone, master_res.id))

        return master_nodes

    def __add_tag_to_instances(self, all_instances):
        """
        Add tags to ec2 instances
        :return:
        """
        additional_tags = {}
        if self.opts.additional_tags.strip():
            additional_tags = dict(
                map(str.strip, tag.split(':', 1)) for tag in self.opts.additional_tags.split(',')
            )
        for ec2_instance in all_instances:
            ec2_instance.add_tags(
                dict(additional_tags, Name='{cn}-master-{iid}'.
                     format(cn=self.cluster_name, iid=ec2_instance.id))
            )

    def launch_instances(self, time_wait_propagate=15):
        """
        Launch all instances (master and Slaves)
        :return:
        """
        print("Launching instances...")
        # Launch slaves
        slave_nodes, zone = self.__launch_slaves()
        # Launch or resume masters
        master_nodes = self.__launch_or_resume_masters()
        # This wait time corresponds to SPARK-4983
        print("Waiting for AWS to propagate instance metadata...")
        time.sleep(time_wait_propagate)

        self.__add_tag_to_instances(master_nodes + slave_nodes)

        return master_nodes, slave_nodes

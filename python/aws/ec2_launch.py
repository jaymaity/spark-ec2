from __future__ import division, print_function, with_statement
import itertools
import random
import string
import sys
import time
from sys import stderr
import python.aws.security_group as sg_grp
import python.aws.config as aws_config
import python.aws.common as aws_common

from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType


class EC2Launch(object):
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
            (master_nodes, slave_nodes) = self.__get_existing_cluster(die_on_error=False)
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
                    instance_initiated_shutdown_behavior=self.opts.instance_initiated_shutdown_behavior,
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

    def __get_existing_cluster(self, die_on_error=True):
        """
        Get the EC2 instances in an existing cluster if available.
        Returns a tuple of lists of EC2 instance objects for the masters and slaves.
        """
        print("Searching for existing cluster {c} in region {r}...".format(
            c=self.cluster_name, r=self.opts.region))

        def get_instances(group_names):
            """
            Get all non-terminated instances that belong to any of the provided security groups.

            EC2 reservation filters and instance states are documented here:
            http://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#options
            """
            reservations = self.conn.get_all_reservations(
                filters={"instance.group-name": group_names})
            instances = itertools.chain.from_iterable(r.instances for r in reservations)
            return [i for i in instances if i.state not in ["shutting-down", "terminated"]]

        master_instances = get_instances([sg_grp.get_master_group_name(self.cluster_name)])
        slave_instances = get_instances([sg_grp.get_slave_group_name(self.cluster_name)])

        if any((master_instances, slave_instances)):
            print("Found {m} master{plural_m}, {s} slave{plural_s}.".format(
                m=len(master_instances),
                plural_m=('' if len(master_instances) == 1 else 's'),
                s=len(slave_instances),
                plural_s=('' if len(slave_instances) == 1 else 's')))

        if not master_instances and die_on_error:
            print("ERROR: Could not find a master for cluster {c} in region {r}.".format(
                c=self.cluster_name, r=self.opts.region), file=sys.stderr)
            sys.exit(1)

        return master_instances, slave_instances

    def __launch_or_resume_masters(self):
        """
        Launch or resume master instances
        :return:
        """
        # TODO: Activate with real app
        existing_masters, existing_slaves = None, None # self.__get_existing_cluster(die_on_error=False)
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
                dict(additional_tags, Name='{cn}-master-{iid}'.format(cn=self.cluster_name, iid=ec2_instance.id))
            )
        # for slave in self.slave_nodes:
        #     slave.add_tags(
        #         dict(additional_tags, Name='{cn}-slave-{iid}'.format(cn=self.cluster_name, iid=slave.id))
        #     )

    def launch_instances(self):
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
        time.sleep(15)

        self.__add_tag_to_instances(master_nodes+slave_nodes)

        return master_nodes, slave_nodes

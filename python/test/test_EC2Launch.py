from unittest import TestCase
from boto import ec2
# from moto import mock_ec2
import python.aws.security_group as sg
import uuid
import python.aws.ec2_cluster as ec2launch
from optparse import OptionParser

parser = OptionParser(
    prog="spark-ec2",
    version="%prog {v}".format(v="1.6"),
    usage="%prog [options] <action> <cluster_name>\n\n"
          + "<action> can be: launch, destroy, login, stop, start, get-master, reboot-slaves")

parser.add_option(
    "-s", "--slaves", type="int", default=1,
    help="Number of slaves to launch (default: %default)")
parser.add_option(
    "-w", "--wait", type="int",
    help="DEPRECATED (no longer necessary) - Seconds to wait for nodes to start")
parser.add_option(
    "-k", "--key-pair",
    help="Key pair to use on instances")
parser.add_option(
    "-i", "--identity-file",
    help="SSH private key file to use for logging into instances")
parser.add_option(
    "-p", "--profile", default=None,
    help="If you have multiple profiles (AWS or boto config), you can configure " +
         "additional, named profiles by using this option (default: %default)")
parser.add_option(
    "-t", "--instance-type", default="t2.micro",
    help="Type of instance to launch (default: %default). " +
         "WARNING: must be 64-bit; small instances won't work")
parser.add_option(
    "-m", "--master-instance-type", default="t2.micro",
    help="Master instance type (leave empty for same as instance-type)")
parser.add_option(
    "-r", "--region", default="us-east-1",
    help="EC2 region used to launch instances in, or to find them in (default: %default)")
parser.add_option(
    "-z", "--zone", default="",
    help="Availability zone to launch instances in, or 'all' to spread " +
         "slaves across multiple (an additional $0.01/Gb for bandwidth" +
         "between zones applies) (default: a single zone chosen at random)")
parser.add_option(
    "-a", "--ami",
    help="Amazon Machine Image ID to use")
parser.add_option(
    "-v", "--spark-version", default="1.6",
    help="Version of Spark to use: 'X.Y.Z' or a specific git hash (default: %default)")
parser.add_option(
    "--spark-git-repo",
    default="",
    help="Github repo from which to checkout supplied commit hash (default: %default)")
parser.add_option(
    "--spark-ec2-git-repo",
    default="",
    help="Github repo from which to checkout spark-ec2 (default: %default)")
parser.add_option(
    "--spark-ec2-git-branch",
    default="",
    help="Github repo branch of spark-ec2 to use (default: %default)")
parser.add_option(
    "--deploy-root-dir",
    default=None,
    help="A directory to copy into / on the first master. " +
         "Must be absolute. Note that a trailing slash is handled as per rsync: " +
         "If you omit it, the last directory of the --deploy-root-dir path will be created " +
         "in / before copying its contents. If you append the trailing slash, " +
         "the directory is not created and its contents are copied directly into /. " +
         "(default: %default).")
parser.add_option(
    "--hadoop-major-version", default="1",
    help="Major version of Hadoop. Valid options are 1 (Hadoop 1.0.4), 2 (CDH 4.2.0), yarn " +
         "(Hadoop 2.4.0) (default: %default)")
parser.add_option(
    "-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
    help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
         "the given local address (for use with login)")
parser.add_option(
    "--resume", action="store_true", default=False,
    help="Resume installation on a previously launched cluster " +
         "(for debugging)")
parser.add_option(
    "--ebs-vol-size", metavar="SIZE", type="int", default=0,
    help="Size (in GB) of each EBS volume.")
parser.add_option(
    "--ebs-vol-type", default="standard",
    help="EBS volume type (e.g. 'gp2', 'standard').")
parser.add_option(
    "--ebs-vol-num", type="int", default=1,
    help="Number of EBS volumes to attach to each node as /vol[x]. " +
         "The volumes will be deleted when the instances terminate. " +
         "Only possible on EBS-backed AMIs. " +
         "EBS volumes are only attached if --ebs-vol-size > 0. " +
         "Only support up to 8 EBS volumes.")
parser.add_option(
    "--placement-group", type="string", default=None,
    help="Which placement group to try and launch " +
         "instances into. Assumes placement group is already " +
         "created.")
parser.add_option(
    "--swap", metavar="SWAP", type="int", default=1024,
    help="Swap space to set up per node, in MB (default: %default)")
parser.add_option(
    "--spot-price", metavar="PRICE", type="float",
    help="If specified, launch slaves as spot instances with the given " +
         "maximum price (in dollars)")
parser.add_option(
    "--ganglia", action="store_true", default=True,
    help="Setup Ganglia monitoring on cluster (default: %default). NOTE: " +
         "the Ganglia page will be publicly accessible")
parser.add_option(
    "--no-ganglia", action="store_false", dest="ganglia",
    help="Disable Ganglia monitoring for the cluster")
parser.add_option(
    "-u", "--user", default="root",
    help="The SSH user you want to connect as (default: %default)")
parser.add_option(
    "--delete-groups", action="store_true", default=False,
    help="When destroying a cluster, delete the security groups that were created")
parser.add_option(
    "--use-existing-master", action="store_true", default=False,
    help="Launch fresh slaves, but use an existing stopped master if possible")
parser.add_option(
    "--worker-instances", type="int", default=1,
    help="Number of instances per worker: variable SPARK_WORKER_INSTANCES. Not used if YARN " +
         "is used as Hadoop major version (default: %default)")
parser.add_option(
    "--master-opts", type="string", default="",
    help="Extra options to give to master through SPARK_MASTER_OPTS variable " +
         "(e.g -Dspark.worker.timeout=180)")
parser.add_option(
    "--user-data", type="string", default="",
    help="Path to a user-data file (most AMIs interpret this as an initialization script)")
parser.add_option(
    "--authorized-address", type="string", default="0.0.0.0/0",
    help="Address to authorize on created security groups (default: %default)")
parser.add_option(
    "--additional-security-group", type="string", default="",
    help="Additional security group to place the machines in")
parser.add_option(
    "--additional-tags", type="string", default="",
    help="Additional tags to set on the machines; tags are comma-separated, while name and " +
         "value are colon separated; ex: \"Task:MySparkProject,Env:production\"")
parser.add_option(
    "--copy-aws-credentials", action="store_true", default=False,
    help="Add AWS credentials to hadoop configuration to allow Spark to access S3")
parser.add_option(
    "--subnet-id", default=None,
    help="VPC subnet to launch instances in")
parser.add_option(
    "--vpc-id", default=None,
    help="VPC to launch instances in")
parser.add_option(
    "--private-ips", action="store_true", default=False,
    help="Use private IPs for instances rather than public if VPC/subnet " +
         "requires that.")
parser.add_option(
    "--instance-initiated-shutdown-behavior", default="stop",
    choices=["stop", "terminate"],
    help="Whether instances should terminate when shut down or just stop")
parser.add_option(
    "--instance-profile-name", default=None,
    help="IAM profile name to launch instances under")

(opts, args) = parser.parse_args()


class TestEC2Launch(TestCase):
    def test_simple_ec2_launch(self):
        opts.region = 'us-east-1'
        opts.vpc_id = None
        opts.use_existing_master = False
        opts.additional_security_group = False
        opts.authorized_address = "0.0.0.0/0"
        opts.ami = "ami-a4c7edb2"
        opts.slaves = 1

        conn = ec2.connect_to_region(opts.region)
        modules = ['ssh', 'spark', 'ephemeral-hdfs', 'persistent-hdfs',
                   'mapreduce', 'spark-standalone', 'tachyon']

        cluster_name = str(uuid.uuid4())

        sec_group = sg.SecurityGroup(conn, modules, opts, cluster_name)
        master_group, slave_group, additional_group_ids = \
            sec_group.create_security_group()

        # # Creation of test image
        # reservation = conn.run_instances(opts.ami)
        # instance = reservation.instances[0]
        # # instance.modify_attribute("kernel", "test-kernel")
        # image_id = conn.create_image(instance.id, "test-ami", "this is a test ami")
        #
        # opts.ami = image_id

        # Launch instances
        ec2launch.launch(conn, opts, cluster_name, master_group, slave_group,
                         additional_group_ids, time_wait_to_propagate=15)

        # Check if same no of slaves launched or not
        # TODO: Write proper test cases
        # It seems security group is not associated with mock
        # Needs more investigation
        reservations = conn.get_all_reservations()
        for reservation in reservations:
            print(opts.region + ':', reservation)

        for vol in conn.get_all_volumes():
            print(opts.region + ':', vol.id)
        import time
        time.sleep(300)
        ec2launch.stop(conn, cluster_name)
        time.sleep(300)
        ec2launch.start(conn, cluster_name)
        time.sleep(300)
        ec2launch.reboot(conn, cluster_name)
        time.sleep(300)
        ec2launch.destroy(conn, cluster_name)



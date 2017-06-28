from pyspark import SparkContext, SparkConf
import os
import sys
import boto
import os.path
import tarfile
import shutil

JOB_NAME = "Test Spark JOB"
TEMP_FOLDER = "/root/task/"
APP_PATH = TEMP_FOLDER + "distrib-load-test-app/"
TEMP_TAR = TEMP_FOLDER + "appfile.tar.gz"
LOAD_COUNT = 2
# KEY_PATH = "/app/distrib-load-test-app.tar.gz"
KEY_PATH = "/testjay/distrib-load-test-app.tar.gz"
BUCKET_NAME = "distrib-load-test-bucket"

SCRIPT_PATH = "/root/task/"
LOADTEST_SCRIPT_PATH = SCRIPT_PATH + "runloadtest.sh"
COLLECTLOG_SCRIPT_PATH = SCRIPT_PATH + "collectlog.sh"

SC = None


def initialize_spark(job_name="SparkJob"):
    """
    Configure spark object
    :param job_name:
    :return: Spark object
    """
    # Spark Configuration
    conf = SparkConf().setAppName(job_name)
    return SparkContext(conf=conf)


def untar_files(source_file, dest_folder):

    tar = tarfile.open(source_file)
    try:
        tar.extractall(path=dest_folder)
    except Exception as ex:
        print(ex.message)

    tar.close()


def download_file_from_s3(bucket_name, filename, dest_file):
    """
    Download file to destination location
    :param src_file: Path or URL of the file to be downloaded
    :param dest_file: Destination of the file to be downloaded
    :return: Returns boolean true if successful
    """
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None

    if not os.path.exists(dest_file):
        # connect to the bucket
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
                               AWS_SECRET_ACCESS_KEY)
        bucket = conn.get_bucket(bucket_name)
        # go through the list of files
        l = bucket.get_key(filename)
        l.get_contents_to_filename(dest_file)

        untar_files(dest_file, TEMP_FOLDER)
        os.chmod(APP_PATH + "distrib-load-test-app", 0o777)
        return True

    return False


def delete_downloaded_file(file_path):
    """
    Delete downloaded file
    :param file_path:
    :return:
    """
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(APP_PATH):
        shutil.rmtree(APP_PATH)


def get_zone():
    """
    Get hostname and zone of ec2 machine
    :return:
    """
    try:
        import boto.utils
        metadata = boto.utils.get_instance_metadata()
        if len(metadata) > 0:
            availability_zone = metadata['placement']['availability-zone'][:-1]
            return availability_zone
    except:
        print("This might not be a ec2 machine")

    return "unknown"


def configure_app():
    """
    Run the application for simulate test
    :param file_path:
    :param arguments:
    :return:
    """
    if not os.path.exists(TEMP_TAR):
        download_file_from_s3(BUCKET_NAME, KEY_PATH, TEMP_TAR)
    else:
        print("\n\n\n\n\n File download skipped... \n\n\n\n")

    # Creation of shell script dynamically
    # if not os.path.exists(app_path + SCRIPT_FILE):
    #     shell_file_contains = """#!/bin/sh
    #         cd {app_path}
    #         chmod 400 {app_path}*.*
    #         {app_path}distrib-load-test-app {host} {zone}
    #         """.format(host=host, zone=zone, app_path=app_path)
    #
    #     print(shell_file_contains)
    #     f = file(app_path + SCRIPT_FILE, "w")
    #     f.write(shell_file_contains)
    #     f.close()
    # else:
    #     print("\n\n\n\n\n Script creation skipped... \n\n\n\n")


def run_parallel_job(no_of_load):
    """
    Run parallel job using spark
    :return:
    """

    sc = initialize_spark(JOB_NAME)
    load_range = range(0, no_of_load)
    rdd_load = sc.parallelize(load_range).\
        map(lambda s: configure_app())

    # Execution of rdd_load
    print(rdd_load.count())

    zone = get_zone()
    exec_runload_arg = " ".join([LOADTEST_SCRIPT_PATH, APP_PATH, zone])
    print("-----------------\nExecuting:\n "+exec_runload_arg+"\n\n\n")

    # execute the program using pipe
    rdd_load_pipe = rdd_load.pipe(exec_runload_arg)
    rdd_load_pipe.count()
    print(rdd_load_pipe.collect())
    print("\n\nTask execution complete.\n\n")

    # Collection of log after execution
    exec_collectlog_arg = " ".join([COLLECTLOG_SCRIPT_PATH, APP_PATH, BUCKET_NAME])
    print("-----------------\nExecuting:\n "+exec_collectlog_arg+"\n\n\n")
    rdd_log_pipe = rdd_load.pipe(exec_collectlog_arg)
    rdd_log_pipe.count()
    print(rdd_log_pipe.collect())

    print("\n\nLogs are uploaded to S3.\n\n")
    sc.stop()


def clear_downloaded_file(noof_worker_nodes):
    """
    Clear all downloaded file from nodes
    SOURCE_PROGRAM_PATH = sys.argv[1]
    :param noof_worker_nodes:
    :return:
    """
    sc = initialize_spark(JOB_NAME)
    load_range = range(1, noof_worker_nodes + 1)
    rdd1 = sc.parallelize(load_range). \
        map(lambda s: delete_downloaded_file(TEMP_TAR))
    rdd1.count()
    print("Old files are removed from the nodes.")

    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        BUCKET_NAME = sys.argv[1]
    if len(sys.argv) >= 3:
        # s3 file path
        KEY_PATH = sys.argv[2]
    if len(sys.argv) == 4:
        LOAD_COUNT = int(sys.argv[3])

    # clear_downloaded_file(LOAD_COUNT)
    run_parallel_job(LOAD_COUNT)




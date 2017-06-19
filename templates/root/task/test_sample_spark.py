from pyspark import SparkContext, SparkConf
import os
import subprocess
import tempfile
import urllib2
import sys


JOB_NAME = "Test Spark JOB"
SOURCE_PROGRAM_PATH = "https://s3.ca-central-1.amazonaws.com/cmpt733jay/a.out"
TEMP_PATH = "/tmp/a.out"
LOAD_COUNT = 8

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


def download_file(src_file, dest_file):
    """
    Download file to destination location
    :param src_file: Path or URL of the file to be downloaded
    :param dest_file: Destination of the file to be downloaded
    :return: Returns boolean true if successful
    """
    response = urllib2.urlopen(src_file)
    with open(dest_file, 'wb') as output:
        output.write(response.read())
    os.system("chmod u+x %s" % dest_file)

    return True


def delete_downloaded_file(file_path):
    """
    Delete downloaded file
    :param file_path:
    :return:
    """
    if os.path.exists(file_path):
        os.remove(file_path)


def run_app(file_path, arguments):
    """
    Run the application for simulate test
    :param file_path:
    :param arguments:
    :return:
    """
    if not os.path.exists(file_path):
        download_file(SOURCE_PROGRAM_PATH, file_path)

    popen_list = []
    popen_list.append(file_path)
    popen_list.extend(arguments)

    # Precaution to handle large output
    with tempfile.TemporaryFile() as temp_file:
        proc = subprocess.Popen(popen_list, stdout=temp_file)
        proc.wait()
        temp_file.seek(0)
        return temp_file.read()


def run_parallel_job(no_of_load):
    """
    Run parallel job using spark
    :return:
    """
    sc = initialize_spark(JOB_NAME)
    load_range = range(1, no_of_load+1)
    rdd1 = sc.parallelize(load_range).\
        map(lambda s: run_app(TEMP_PATH, [str(s)]))
    print("\n\nOutput in a list:\n")
    print(rdd1.collect())
    sc.stop()


def clear_downloaded_file(noof_worker_nodes):
    """
    Clear all downloaded file from nodes        SOURCE_PROGRAM_PATH = sys.argv[1]
    :param noof_worker_nodes:
    :return:
    """
    sc = initialize_spark(JOB_NAME)
    load_range = range(1, noof_worker_nodes + 1)
    rdd1 = sc.parallelize(load_range). \
        map(lambda s: delete_downloaded_file(TEMP_PATH))
    rdd1.count()
    print("Old files are removed from the nodes.")

    sc.stop()

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        SOURCE_PROGRAM_PATH = sys.argv[1]
    if len(sys.argv) == 3:
        LOAD_COUNT = int(sys.argv[2])

    clear_downloaded_file(LOAD_COUNT)
    run_parallel_job(LOAD_COUNT)




def get_zones(conn, opts):
    """
    Gets a list of zones to launch instances in
    :param conn:
    :param opts:
    :return:
    """
    if opts.zone == 'all':
        zones = [z.name for z in conn.get_all_zones()]
    else:
        zones = [opts.zone]
    return zones


def get_partition(total, num_partitions, current_partitions):
    """
    Gets the number of items in a partition
    :param total:
    :param num_partitions:
    :param current_partitions:
    :return:
    """
    num_slaves_this_zone = total // num_partitions
    if (total % num_partitions) - current_partitions > 0:
        num_slaves_this_zone += 1
    return num_slaves_this_zone

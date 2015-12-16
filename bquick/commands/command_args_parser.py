# -*- coding: utf-8 -*-


def parse():
    parser = argparse.ArgumentParser(description='bquick - Google BigQuery management toolkit.')
    parser.add_argument('-c', '--config',
        help='Read configuration from a configuration file')
    args = parser.parse_args()
    print args

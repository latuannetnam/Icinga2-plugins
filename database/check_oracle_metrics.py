#!/usr/bin/python3
# Check Oracle metrics and export to InfluxDB
# Credit:
# https://bdrouvot.wordpress.com/2016/03/05/graphing-oracle-performance-metrics-with-telegraf-influxdb-and-grafana/

import sys
import argparse
from influxdb import InfluxDBClient
import cx_Oracle


class OracleMetrics():
    def __init__(self, args):
        self.influx_host = args.influx_host
        # print("Influx host:", self.influx_host)


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # self.print_help(sys.stderr)
        print("UKNOWN - %s." % (message))
        self.exit(3)


def parse_args():
    """Parse the args."""
    parser = ArgumentParser(
        description="Plugin for Icinga to check oracle's metrics and export to InfluxDB")
    parser.add_argument('-hostname', type=str, required=False,
                        default='localhost',
                        help='hostname of Icinga client')
    parser.add_argument('-influx_host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB server')
    parser.add_argument('-influx_port', type=int, required=False, default=8086,
                        help='port of InfluxDB server')
    parser.add_argument('-influx_user', type=str,
                        required=False, help='InfluxDB user name')
    parser.add_argument('-influx_password', type=str, required=False)
    parser.add_argument('-influx_db', type=str, required=True,
                        help='InfluxDB database name')
    parser.add_argument(
        '-oracle_user', help="Oracle username with sys views grant", required=True)
    parser.add_argument('-oracle_password', required=True)
    parser.add_argument(
        '-oracle_sid', help="tnsnames SID to connect", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    object = OracleMetrics(args)
    print("OK - Oracle Metrics for %s" % (args.oracle_sid))
    sys.exit(0)

#!/usr/bin/python3
# Check Oracle metrics and export to InfluxDB
# Credit:
# https://bdrouvot.wordpress.com/2016/03/05/graphing-oracle-performance-metrics-with-telegraf-influxdb-and-grafana/
# Run: python3 check_oracle_metrics.py -influx_host=10.30.12.170 -influx_user=user -influx_password=pass -influx_db=oracle_metrics -oracle_user=user -oracle_password=pass -oracle_sid=ip/orcl
import sys
import argparse
from influxdb import InfluxDBClient
import cx_Oracle


class OracleMetrics():
    def __init__(self, args):
        self.hostname = args.hostname
        self.host_group = args.host_group
        # self.host_group = ['abc', 'def', 'gjk']
        self.influx_host = args.influx_host
        self.influx_port = args.influx_port
        self.influx_user = args.influx_user
        self.influx_password = args.influx_password
        self.influx_db = args.influx_db
        self.oracle_user = args.oracle_user
        self.oracle_password = args.oracle_password
        self.oracle_sid = args.oracle_sid
        self.db_connection = cx_Oracle.connect(
            self.oracle_user, self.oracle_password, self.oracle_sid)
        self.influx_client = InfluxDBClient(
            self.influx_host, self.influx_port, self.influx_user, self.influx_password, self.influx_db)

    def database_details(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select created, open_mode, log_mode, database_role, controlfile_type, switchover_status, protection_mode, open_resetlogs, guard_status,force_logging from v$database
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['created'] = detail[0]
            db_detail['open_mode'] = detail[1]
            db_detail['log_mode'] = detail[2]
            db_detail['database_role'] = detail[3]
            db_detail['controlfile_type'] = detail[4]
            db_detail['switchover_status'] = detail[5]
            db_detail['protection_mode'] = detail[6]
            db_detail['open_resetlogs'] = detail[7]
            db_detail['guard_status'] = detail[8]
            db_detail['force_logging'] = detail[9]
            for key, value in db_detail.items():
                json_body = [
                    {
                        "measurement": "oracle_database_details",
                        "tags": {
                            "hostname": format(self.hostname),
                            "host_group": format(self.host_group),
                            "metric": key
                        },
                        "fields": {
                            "value": format(value),
                        }
                    }
                ]
                # print("Write points: {0}".format(json_body))
                self.influx_client.write_points(json_body)


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
    parser.add_argument('-host_group', type=str, required=False,
                        default='oracle',
                        help='host_group of Icinga client')                    
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
    object.database_details()
    print("OK - Oracle Metrics for %s" % (args.oracle_sid))
    sys.exit(0)

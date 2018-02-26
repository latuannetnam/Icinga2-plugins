#!/usr/bin/python3
# Check MSSQL metrics and export to InfluxDB
# Author: Le Anh Tuan (tuan.le@netnam.vn/latuannetnam@gmail.com)
import sys
import argparse
from influxdb import InfluxDBClient
import pymssql


class MSSQLMetrics():
    def __init__(self, args):
        self.hostname = args.hostname
        self.host_group = args.host_group
        self.influx_host = args.influx_host
        self.influx_port = args.influx_port
        self.influx_user = args.influx_user
        self.influx_password = args.influx_password
        self.influx_db = args.influx_db
        self.mssql_server = args.mssql_server
        self.mssql_port = args.mssql_port
        self.mssql_user = args.mssql_user
        self.mssql_password = args.mssql_password
        self.mssql_database = args.mssql_database

        self.db_connection = pymssql.connect(server=self.mssql_server, user=self.mssql_user,
                                             password=self.mssql_password, database=self.mssql_database, charset='UTF-8', port=self.mssql_port)
        self.influx_client = InfluxDBClient(
            self.influx_host, self.influx_port, self.influx_user,
            self.influx_password, self.influx_db)

    # data point will be:
    #   series1 = {
    #               tags = {
    #                   metric: key1,
    #               }
    #               fields = {
    #                   value: value1
    #               }
    #   series2 = {
    #               tags = {
    #                   metric: key2,
    #               }
    #               fields = {
    #                   value: value2
    #               }
    def write_data_by_tags(self, measurement, db_detail):
        for key, value in db_detail.items():
            json_body = [
                {
                    "measurement": measurement,
                    "tags": {
                        "hostname": format(self.hostname),
                        "host_group": format(self.host_group),
                        "metric": key
                    },
                    "fields": {
                        "value": value,
                    }
                }
            ]
            # print("Write points: {0}".format(json_body))
            self.influx_client.write_points(json_body)

    # data point will be:
    #   series = {
    #               tags = {
    #                   tag_key: tag_value,
    #               }
    #               fields = {
    #                   key1: value1,
    #                   key2: value2,
    #               }
    def write_data_by_fields(self, measurement, tag_key, db_detail):
        json_body = []
        json_detail = {
            "measurement": measurement,
            "tags": {
                "hostname": format(self.hostname),
                "host_group": format(self.host_group),
                tag_key: format(db_detail[tag_key])
            },
        }
        json_fields = {}
        for key, value in db_detail.items():
            if key != tag_key:
                json_fields[key] = value
        json_detail['fields'] = json_fields
        json_body.append(json_detail)
        # print("Write points: {0}".format(json_body))
        self.influx_client.write_points(json_body)

    def database_details(self):
        cursor = self.db_connection.cursor()
        # database size and state
        cursor.execute("""
        SELECT DB_NAME(database_id) AS DatabaseName, SUM(state) as state,
            sum((size * 8) / 1024) SizeMB
            FROM sys.master_files
            WHERE type = 0
            group by database_id
        """)
        details = []
        for detail in cursor:
            db_detail = {}
            db_detail['Database name'] = detail[0]
            db_detail['state'] = detail[1]
            db_detail['size'] = detail[2]
            details.append(db_detail)

        # database log size and state
        cursor.execute("""
        DBCC SQLPERF(logspace)
        """)
        detail2s = []
        for detail in cursor:
            db_detail = {}
            db_detail['Database name'] = detail[0]
            db_detail['Log size'] = detail[1]
            db_detail['Log space used'] = detail[2]
            db_detail['Log status'] = detail[3]
            detail2s.append(db_detail)
        # Aggregate result
        for detail in details:
            for detail2 in detail2s:
                if detail['Database name'] == detail2['Database name']:
                    detail['Log size'] = detail2['Log size']
                    detail['Log space used'] = detail2['Log space used']
                    detail['Log status'] = detail2['Log status']
            # Export to InfluxDB
            self.write_data_by_fields(
                'mssql_database_details', 'Database name', detail)

    def backup_details(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        SELECT
        B.backup_start_date,
        A.last_db_backup_date,
        DATEDIFF(day, A.last_db_backup_date, B.backup_start_date) AS total_time,
        B.backup_size,
        B.physical_device_name,
        DATEDIFF(day, A.last_db_backup_date, GETDATE()) AS backup_age,
        A.[Server],
        B.expiration_date,
        B.logical_device_name,
        B.backupset_name,
        B.description
        FROM
                (
                        SELECT
                                CONVERT(
                                        CHAR(100),
                                        SERVERPROPERTY('Servername')
                                ) AS Server,
                                msdb.dbo.backupset.database_name,
                                MAX( msdb.dbo.backupset.backup_finish_date ) AS last_db_backup_date
                        FROM
                                msdb.dbo.backupmediafamily
                        INNER JOIN msdb.dbo.backupset ON
                                msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id
                        WHERE
                                msdb..backupset.type = 'D'
                        GROUP BY
                                msdb.dbo.backupset.database_name
                ) AS A
        LEFT JOIN(
                        SELECT
                                CONVERT(
                                        CHAR(100),
                                        SERVERPROPERTY('Servername')
                                ) AS Server,
                                msdb.dbo.backupset.database_name,
                                msdb.dbo.backupset.backup_start_date,
                                msdb.dbo.backupset.backup_finish_date,
                                msdb.dbo.backupset.expiration_date,
                                msdb.dbo.backupset.backup_size,
                                msdb.dbo.backupmediafamily.logical_device_name,
                                msdb.dbo.backupmediafamily.physical_device_name,
                                msdb.dbo.backupset.name AS backupset_name,
                                msdb.dbo.backupset.description
                        FROM
                                msdb.dbo.backupmediafamily
                        INNER JOIN msdb.dbo.backupset ON
                                msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id
                        WHERE
                                msdb..backupset.type = 'D'
                ) AS B ON
                A.[server] = B.[server]
                AND A.[database_name] = B.[database_name]
                AND A.[last_db_backup_date] = B.[backup_finish_date]
        ORDER BY
                A.database_name
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Start time'] = format(detail[0])
            db_detail['End time'] = format(detail[1])
            db_detail['Total time'] = detail[2]
            db_detail['Size'] = int(detail[3])
            db_detail['Physical name'] = detail[4]
            db_detail['Backup age'] = detail[5]
            self.write_data_by_fields(
                'mssql_backup_details', 'Physical name', db_detail)


class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        # self.print_help(sys.stderr)
        print("UKNOWN - %s." % (message))
        self.exit(3)


def parse_args():
    """Parse the args."""
    parser = ArgumentParser(
        description="Plugin for Icinga to check mssql's metrics and export to InfluxDB")

    parser.add_argument('-hostname', type=str, required=False,
                        default='localhost',
                        help='hostname of Icinga client')
    parser.add_argument('-host_group', type=str, required=False,
                        default='mssql',
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
        '-mssql_server', help="MSSQL server's hostname/IP", required=True)
    parser.add_argument('-mssql_port', type=int, required=False, default=1433,
                        help='port of MSSQL server. Default 1433')
    parser.add_argument(
        '-mssql_user', help="MSSQL username with VIEW SERVER STATE grant", required=True)
    parser.add_argument('-mssql_password', required=True)
    parser.add_argument(
        '-mssql_database', required=False, default="master",
        help='Initial MSSQL database to connect')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    object = MSSQLMetrics(args)
    object.database_details()
    object.backup_details()
    print("OK - MSSQL Metrics for %s" % (args.mssql_server))
    sys.exit(0)

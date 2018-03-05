#!/usr/bin/python3
# Check Oracle metrics and export to InfluxDB
# Credit:
# https://bdrouvot.wordpress.com/2016/03/05/graphing-oracle-performance-metrics-with-telegraf-influxdb-and-grafana/
# Run: python3 check_oracle_metrics.py -influx_host=10.30.12.170
# -influx_user=user -influx_password=pass -influx_db=oracle_metrics
# -oracle_user=user -oracle_password=pass -oracle_sid=ip/orcl
import sys
import argparse
from influxdb import InfluxDBClient
import cx_Oracle


class OracleMetrics():
    def __init__(self, args):
        self.hostname = args.hostname
        self.host_group = args.host_group
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
                tag_key: db_detail[tag_key]
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

    def database_availability(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select database_status from sys.v_$instance
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Current Status'] = detail[0]
            self.write_data_by_tags('oracle_availability', db_detail)

    def database_uptime(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select (SYSDATE - startup_time)*24*3600 up_time from sys.v_$instance
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Up Time'] = int(detail[0])
            self.write_data_by_tags('oracle_uptime', db_detail)

    def database_details(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select created, open_mode, log_mode, database_role, controlfile_type,
        switchover_status, protection_mode, open_resetlogs, guard_status,
        force_logging from v$database
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Database Created Time'] = format(detail[0])
            db_detail['Open Mode'] = detail[1]
            db_detail['Log Mode'] = detail[2]
            db_detail['DB Role'] = detail[3]
            db_detail['Control File Type'] = detail[4]
            db_detail['Switch Over Status'] = detail[5]
            db_detail['Protection Mode'] = detail[6]
            db_detail['Open Reset Logs'] = detail[7]
            db_detail['Guard Status'] = detail[8]
            db_detail['Force Logging'] = detail[9]
            self.write_data_by_tags('oracle_database_details', db_detail)

    def tablespace_details(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        SELECT d.tablespace_name,
               COUNT(d.file_name) num_datafiles,
               NVL(SUM(decode(sign(d.maxbytes-d.bytes),1,d.maxbytes,d.bytes)),0) allocated_bytes,
               SUM(d.blocks) allocated_blocks
        FROM sys.dba_data_files d
        GROUP BY d.tablespace_name
                 union
                 SELECT d.tablespace_name,
                        COUNT(d.file_name) num_datafiles,
                        NVL(SUM(d.bytes),0) allocated_bytes,
                        SUM(d.blocks) allocated_blocks
                 FROM sys.dba_temp_files d
                 GROUP BY d.tablespace_name order by tablespace_name
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Tablespace'] = detail[0]
            db_detail['Datafiles'] = detail[1]
            db_detail['AllocatedBytes'] = detail[2]
            db_detail['allocatedBlocks'] = detail[3]
            self.write_data_by_fields(
                'oracle_tablespace_details', 'Tablespace', db_detail)

    def tablespace_status_1(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        SELECT d.tablespace_name,
               SUM(f.phyrds) phyrds,
               SUM(f.phywrts) phywrts,
               SUM(f.readtim) readtim,
               SUM(f.writetim) writetim
        FROM sys.dba_data_files d,
                    V$filestat f
        WHERE d.file_id = f.file#
        GROUP BY d.tablespace_name
        ORDER by d.tablespace_name
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Tablespace'] = detail[0]
            db_detail['Reads'] = detail[1]
            db_detail['Writes'] = detail[2]
            db_detail['Readtime'] = detail[3]
            db_detail['Writetime'] = detail[4]
            self.write_data_by_fields(
                'oracle_tablespace_status_1', 'Tablespace', db_detail)

    def tablespace_status_2(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        SELECT t.tablespace_name,
               t.contents,
               t.status,
               NVL(df.allocated_bytes,0)-NVL((NVL(f.free_bytes,0)+df.max_free_bytes),0) usedBytes,
               NVL((NVL(f.free_bytes,0)+df.max_free_bytes),0) freeBytes,
               NVL(f.free_blocks,0) freeBlocks
        FROM sys.dba_tablespaces t,
             (select ff.tablespace_name,sum(ff.free_bytes) free_bytes,
                     sum(ff.free_blocks) free_blocks
              from
                (SELECT fs.tablespace_name,
                        SUM(fs.bytes) free_bytes,
                        SUM(fs.blocks) free_blocks
                 FROM sys.dba_free_space fs,
                      sys.dba_data_files dfs
                 WHERE fs.file_id=dfs.file_id
                 GROUP BY fs.tablespace_name,
                          dfs.autoextensible
                ) ff
              group by tablespace_name
             ) f,
             (select dff.tablespace_name,
                     sum(dff.allocated_bytes) allocated_bytes,
                     sum(dff.max_free_bytes) max_free_bytes
              from
                (select tablespace_name,
                        autoextensible,
                        sum(decode(sign(maxbytes-bytes),
                        1,
                        maxbytes,
                        bytes
                )
              ) allocated_bytes,
              sum(decode(sign(maxbytes-bytes),
              1,
              abs(maxbytes-bytes),0)) max_free_bytes
        from dba_data_files
        group by tablespace_name,autoextensible) dff
        group by tablespace_name) df
        WHERE t.tablespace_name = f.tablespace_name(+)
              and t.tablespace_name=df.tablespace_name(+) order by tablespace_name
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Tablespace'] = detail[0]
            db_detail['Contents'] = detail[1]
            db_detail['Status'] = detail[2]
            db_detail['Usedbytes'] = detail[3]
            db_detail['Freebytes'] = detail[4]
            db_detail['Freeblocks'] = detail[5]
            self.write_data_by_fields(
                'oracle_tablespace_status_2', 'Tablespace', db_detail)

    def redo_logs(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select r.group#, r.thread#,
               r.sequence#, r.bytes,
               r.members, r.archived,
               r.status, r.first_time,
               m.member, m.status mstatus
               from v$log r, v$logfile m where r.group# = m.group#
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Group Name'] = detail[0]
            db_detail['Thread'] = detail[1]
            db_detail['Sequence'] = detail[2]
            db_detail['Bytes'] = detail[3]
            db_detail['Members'] = detail[4]
            db_detail['Archive'] = detail[5]
            db_detail['Status'] = detail[6]
            db_detail['First Time'] = format(detail[7])
            db_detail['Member'] = detail[8]
            db_detail['Mstatus'] = detail[9]
            self.write_data_by_fields(
                'oracle_redo_logs', 'Group Name', db_detail)

    def oracle_users(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select username, expiry_date, 
               round(expiry_date - current_date) days_to_expiry, 
               account_status, profile from dba_users
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Username'] = detail[0]
            db_detail['Expiry Date'] = format(detail[1])
            db_detail['Days To Expiry'] = detail[2]
            db_detail['Account Status'] = detail[3]
            db_detail['Profile'] = detail[4]
            self.write_data_by_fields('oracle_users', 'Username',  db_detail)

    def oracle_dblinks(self):
        cursor = self.db_connection.cursor()
        cursor.execute("""
        select db_link, owner, username, host, created from dba_db_links
        """)
        for detail in cursor:
            db_detail = {}
            db_detail['Db Link'] = detail[0]
            db_detail['Owner'] = detail[1]
            db_detail['Username'] = detail[2]
            db_detail['Host'] = detail[3]
            db_detail['Created'] = format(detail[4])
            self.write_data_by_fields('oracle_dblinks', 'Db Link', db_detail)


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
    object.database_uptime()
    object.database_availability()
    object.database_details()
    object.tablespace_details()
    object.tablespace_status_1()
    object.tablespace_status_2()
    object.redo_logs()
    object.oracle_users()
    object.oracle_dblinks()
    print("OK - Oracle Metrics for %s" % (args.oracle_sid))
    sys.exit(0)

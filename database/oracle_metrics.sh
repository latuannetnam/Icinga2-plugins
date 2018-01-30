#!/usr/bin/env bash
cd /usr/lib/nagios/plugins/thirdparty/libexec
python oracle_metrics.py -f influx ALL -u icinga -p netdept -s 10.30.12.174/orcl

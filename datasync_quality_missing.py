
import sys
sys.path.append("/apps/common/")
from utils import load_config, sendMailHTML, dbConnect, dbQuery
import base64
from datetime import datetime as logdt
import traceback


def checkMissing(config_list):
    print_hdr = "[datasync_quality_missing: checkMissing] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    conn_metadata = None
    try:

        conn_metadata, cur_metadata = dbConnect(config_list['meta_db_dbName'], config_list['meta_db_dbUser'],
                                                config_list['meta_db_dbUrl'], base64.b64decode(config_list['meta_db_dbPwd']))

        check_sql = "select c.id, c.data_path, c.load_type,c.source_schemaname||'.'||c.source_tablename as source_table, c.target_schemaname||'.'||c.target_tablename as target_table, " \
                            "c.system_name, c.hvr_source_schemaname, to_char(l.last_success_run_time,'YYYY-MM-DD HH24:MI:SS') as last_success_run_time, " \
                            "to_char(q.last_count_run_time,'YYYY-MM-DD HH24:MI:SS') as last_count_run_time, to_char(c.last_run_time,'YYYY-MM-DD HH24:MI:SS') as last_control_run_time " \
                    "from sync.control_table c " \
                    "left outer join (select data_path,load_type, target_tablename,max(run_time) as last_count_run_time from sbdt.datasync_quality group by data_path,load_type,target_tablename) q " \
                    "on q.data_path = c.data_path " \
                            "AND q.load_type = c.load_type " \
                            "AND q.target_tablename = c.target_schemaname||'.'||c.target_tablename " \
                    "left outer join (select data_path, table_name, max(log_time) as last_success_run_time from sbdt.edl_log where plant_name = 'DATASYNC' and data_path in ('SRC2Hive','Talend2Hive','KFK2Hive','SQOOP2Hive') and status = 'Job Finished' group by data_path, table_name) l " \
                    "on l.data_path = c.data_path " \
                            "AND l.table_name = c.target_schemaname||'.'||c.target_tablename " \
                    "where 1 = 1 " \
                            "AND c.data_path in ('SRC2Hive','Talend2Hive','KFK2Hive','SQOOP2Hive') " \
                            "AND c.source_schemaname not in ('ftp') " \
                            "AND (c.system_name is null or c.system_name not in ('externaldata')) " \
                            "AND c.status_flag = 'Y' " \
                            "AND (c.custom_sql is NULL OR trim(c.custom_sql) = '') " \
                            "AND ((q.last_count_run_time is null) or (l.last_success_run_time is not null and q.last_count_run_time < l.last_success_run_time - interval '1 day')) " \
                    "order by last_success_run_time desc nulls last"

        print check_sql

        check_results = dbQuery(cur_metadata, check_sql)
        if len(check_results) > 0:
            mail_subject = "ATTENTION: Datasync Quality: Missing Count Validation"
            sendMailHTML(config_list['email_dataQualityReceivers'], mail_subject, formatMissingMail(check_results))

    except Exception as e:
        mail_subject = "ERROR: Datasync Quality Missing"
        output_msg = "ERROR: Encountered error while running job" + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(config_list['email_dataQualityReceivers'], mail_subject, output_msg)
        sys.exit(0)
    finally:
        if conn_metadata is not None and not conn_metadata.closed:
            conn_metadata.close()


def formatMissingMail(results):
    print_hdr = "[datasync_quality_missing: formatMissingMail] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    html_msg = """\

<head>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <title>Hive Lock Alert</title>
  <style type="text/css" media="screen">
    table{
        background-color: #AAD373;
        empty-cells:hide;
    }
    th{
        border: black 1px solid;
    }    
    td{
        background-color: white;
        border: black 1px solid;
    }
  </style>
</head>
<body>
    <table style="border: black 1px solid;">
        <tr>
            <th>Id</th>
            <th>Data Path</th>
            <th>Load Type</th>
            <th>Source Table</th>
            <th>Target Table</th>
            <th>System Name</th>
            <th>HVR Source Schema</th>
            <th>Last Success Run Time</th>
            <th>Last Count Run Time</th>
            <th>Last Control Run Time</th>
        </tr> """

    for result in results:
        html_msg = html_msg + """\
        <tr>
            <td>""" + str(result['id']) + """</td>
            <td>""" + result['data_path'] + """</td>
            <td>""" + result['load_type'] + """</td>
            <td>""" + result['source_table'] + """</td>
            <td>""" + result['target_table'] + """</td>
            <td>""" + str(result['system_name']) + """</td>
            <td>""" + str(result['hvr_source_schemaname']) + """</td>
            <td>""" + str(result['last_success_run_time']) + """</td>
            <td>""" + str(result['last_count_run_time']) + """</td>
            <td>""" + str(result['last_control_run_time']) + """</td>
        </tr>"""

    html_msg = html_msg + """\
    </table>
</body>"""

    return html_msg



if __name__ == "__main__":

    print ('\n******************************************************************************************************************************************************\n')
    print_hdr = "[datasync_quality_missing: main] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    config_list = load_config()
    # config_list['email_dataQualityReceivers'] = 'kuntal.deb@ge.com'

    try:
        checkMissing(config_list)

    except Exception as e:
        mail_subject = "ERROR: Datasync Quality Missing"
        output_msg = "ERROR: Encountered error while running job" + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(config_list['email_dataQualityReceivers'], mail_subject, output_msg)

    sys.exit(0)


import sys
sys.path.append("/apps/common/")
from utils import load_config, sendMailHTML, dbConnect, dbQuery, dbConnectOra, dbConnectSQLServer, dbConnectHive
from auditLog import audit_logging
import base64
from datetime import datetime as logdt
import traceback

class DatasyncQuality(object):

    def __init__(self):
        self.class_name = self.__class__.__name__
        self.config_list = load_config()
        self.data_path = None
        self.schema_system_name = None
        self.load_id = None
        self.table_meta_dlist = None
        self.emailReceiver = None
        self.__db_hostname = None
        self.__db_database = None
        self.__db_port = None
        self.__db_username = None
        self.__db_edlpgppass = None
        self.__count_diff_flag = False
        self.run_time = None
        self.plant_name = 'DATASYNC'
        self.system_name = 'Hive'
        self.job_name = 'datasync quality'
        self.technology = 'Python'
        self.tablename = 'sbdt.datasync_quality'
        self.rows_inserted = 0
        self.rows_deleted = 0


    def __getEdlConn(self, p_conn_systemname=None):

        method_name = self.class_name + ": " + "__getEdlConn"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        conn_metadata = None
        error = 0
        err_msg = ''
        output = {}

        try:
            schema_system_map = {'adw': 'adw',
                                'ats': 'ats',
                                'cas': 'cas',
                                'erp_oracle': 'erp_oracle',
                                'erp_sqlwh': 'erp_sqlwh',
                                'eservice': 'eservice',
                                'eservice_kafka': 'eservice',
                                'get_dap': None,
                                'gets_rin': 'gets_rin',
                                'historic_weather': None,
                                'ncmr': 'ncmr',
                                'nucleus': 'nucleus',
                                'odw': 'odw',
                                'odw_kaf': 'odw',
                                'odw_kafka': 'odw',
                                'pbts': 'tspbtprd',
                                'proficy': 'proficy',
                                'proficy_kafka': 'proficy',
                                'proficy_grr': 'proficy_grr',
                                'proficy_grr_kafka': 'proficy_grr',
                                'ras_audit': 'mfgdata',
                                'reliance': 'reliance',
                                'rmd': 'rmd',
                                'scp': 'scp',
                                'sdr': 'sdr',
                                'shop_supt': 'shop_supt',
                                'srs': 'srs',
                                'svc_txn': None,
                                'teamcenter': 'teamcenter',
                                'todw': None,
                                'tscollp': 'tscollp'}

            conn_systemname = None
            if p_conn_systemname is None:
                if self.data_path in ['SRC2Hive','KFK2Hive']:
                    conn_systemname = schema_system_map.get(self.schema_system_name, None)
                elif self.data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
                    conn_systemname = self.schema_system_name
            else:
                if self.data_path in ['SRC2Hive', 'KFK2Hive']:
                    conn_systemname = schema_system_map.get(p_conn_systemname, None)
                elif self.data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
                    conn_systemname = p_conn_systemname

            if conn_systemname == None:
                error = 1
                err_msg = method_name + "[{0}]: No system name mapping for schema system: ".format(error) + self.schema_system_name + ' or requested system name: ' + str(p_conn_systemname)
                output_msg = "No system name mapping for schema system: " + self.schema_system_name + ' or requested system name: ' + str(p_conn_systemname)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
                output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
                return output

            edl_private_key = base64.b64decode(self.config_list['misc_edlPrivateKey'])

            conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'], self.config_list['meta_db_dbUser'],
                                                    self.config_list['meta_db_dbUrl'], base64.b64decode(self.config_list['meta_db_dbPwd']))

            db_conn_sql = None
            if self.data_path in ['SRC2Hive','KFK2Hive']:
                db_conn_sql = "SELECT system_name, host_name, database, port, user_name, sbdt.hvr_decrypt_password(system_name, '" + edl_private_key + "') as pgp_pass " \
                               "FROM sbdt.hvr_connection c " \
                               "WHERE LOWER(system_name) = '" + str(conn_systemname) + "'"

            elif self.data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
                db_conn_sql = "SELECT system_name, host_name, database, port, user_name, sbdt.edl_decrypt_password(system_name, '" + edl_private_key + "') as pgp_pass " \
                               "FROM sbdt.edl_connection c " \
                               "WHERE LOWER(system_name) = '" + str(conn_systemname) + "'"

            # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "db_conn_sql: " + db_conn_sql)

            conn_results = dbQuery(cur_metadata, db_conn_sql)

            # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "results: ", conn_results)

            if len(conn_results) == 0:
                error = 2
                err_msg = method_name + "[{0}]: No connnection details available for schema: ".format(error) + conn_systemname + " in hvr or edl control table"
                output_msg = "No connnection details available for schema: " + conn_systemname + " in hvr or edl control table"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
                output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
                return output

            self.__db_hostname = conn_results[0]['host_name']
            self.__database = conn_results[0]['database']
            self.__db_port = conn_results[0]['port']
            self.__db_username = conn_results[0]['user_name']
            self.__db_edlpgppass = conn_results[0]['pgp_pass']

            if len(output) == 0:
                output = {"error": error, "err_msg": err_msg}
            return output

        except Exception as e:
            error = 3
            err_msg = method_name + "[{0}]: Encountered error getting edl connection".format(error)
            output_msg = "ERROR: Encountered error getting edl connection" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        finally:
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()


    def __preProcessing(self):
        method_name = self.class_name + ": " + "__preProcessing"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        try:
            if self.data_path in ['SRC2Hive', 'KFK2Hive']:
                hvr_schema_missing_dlist = []
                for table_dict in self.table_meta_dlist:
                    if table_dict['hvr_source_schemaname'] == None:
                        hvr_schema_missing_dlist.append(table_dict)
                    else:
                        if self.data_path == 'SRC2Hive':
                            table_dict['source_tablename'] = table_dict['source_tablename'].replace('_ext_hvr','')
                        if self.data_path == 'KFK2Hive':
                            table_dict['source_tablename'] = table_dict['source_tablename'].replace('_kext','')

                # Remove missing hvr schema records from main table list
                for table in hvr_schema_missing_dlist:
                    self.table_meta_dlist.remove(table)

                if len(hvr_schema_missing_dlist) > 0:
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "hvr_schema_missing_dlist: ", hvr_schema_missing_dlist)
                    mail_subject = "ATTENTION: Datasync Quality: Missing HVR Source Schema setup in " + self.schema_system_name + ": " + self.data_path
                    sendMailHTML(self.emailReceiver, mail_subject, self.__formatSetupMail(hvr_schema_missing_dlist))

                # conn_metadata = None
                # try:
                #     conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'], self.config_list['meta_db_dbUser'],
                #                                             self.config_list['meta_db_dbUrl'], base64.b64decode(self.config_list['meta_db_dbPwd']))
                #
                #     metadata_sql = "select data_path,load_type,source_schemaname,source_tablename,target_schemaname,target_tablename,status_flag,hvr_source_schemaname " \
                #                 "from sync.control_table " \
                #                 "where  1 = 1 " \
                #                     "and data_path in ('" + self.data_path + "') " \
                #                     "and target_schemaname = '" + self.schema_name + "' " \
                #                     "and load_type <> 'APPEND_ONLY' " \
                #                     "and hvr_source_schemaname is null " \
                #                     "and source_schemaname not in ('ftp') " \
                #                 "order by target_schemaname, target_tablename "
                #
                #     print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metadata_sql: " + metadata_sql)
                #
                #     meta_results = dbQuery(cur_metadata, metadata_sql)

                    # if len(meta_results) > 0:
                    #     mail_subject = "ATTENTION: Datasync Quality: Missing HVR Source Schema setup in " + self.schema_name + ": " + self.data_path
                    #     sendMailHTML(emailReceiver, mail_subject, self.__formatSetupMail(meta_results))

                # except Exception as e:
                #     raise
                # finally:
                #     if conn_metadata is not None and not conn_metadata.closed:
                #         conn_metadata.close()

        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Encountered error while pre-processing".format(error)
            output_msg = "ERROR: Encountered error gwhile pre-processing" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            self.__postProcessing(output)


    def __postProcessing(self, output):
        method_name = self.class_name + ": " + "__postProcessing"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        mail_subject = "ERROR: Datasync Quality: " + self.schema_system_name + ": " + self.data_path + ": " + self.load_id

        status = 'Job Finished'
        try:
            if output['error'] > 0:
                status = 'Job Error'
                sendMailHTML(self.emailReceiver, mail_subject, output['output_msg'])
            elif output['error'] > 0:
                status = 'Job Error'
                sendMailHTML(self.emailReceiver, mail_subject, output['output_msg'])
            else:
                # if self.__count_diff_flag:
                run_duration = (logdt.now() - logdt.strptime(self.run_time, '%Y-%m-%d %H:%M:%S')).seconds
                mail_subject = "VALIDATION: Datasync Quality: " + self.schema_system_name + ": " + self.data_path + ": " + self.load_id + ": " + str(run_duration) + " secs"
                sendMailHTML(self.emailReceiver, mail_subject, self.__formatCountMail())

            # audit log
            audit_logging('', self.load_id, 0, self.plant_name, self.system_name, self.job_name, self.tablename, status,
                        self.data_path, self.technology,self.rows_inserted,0, self.rows_deleted,
                        output.get('error',0), output.get('err_msg','') ,0,0,output.get('output_msg',''))

            sys.exit(0)
        except Exception as e:
            error = 1
            err_msg = "ERROR: Encountered error while post processing"
            output_msg = "ERROR: Encountered error while post processing" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            sendMailHTML(self.emailReceiver, mail_subject, output_msg)
            audit_logging('', self.load_id, 0, self.plant_name, self.system_name, self.job_name, self.tablename,'Job Error',
                          self.data_path, self.technology, self.rows_inserted, 0, self.rows_deleted,
                          error, err_msg, 0, 0, output_msg)
            sys.exit(0)


    def __formatCountMail(self):
        method_name = self.class_name + ": " + "__formatCountMail"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
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
        td#td_bg_red{
            background-color: red;
            border: black 1px solid;
        }
      </style>
    </head>
    <body>
        <table style="border: black 1px solid;">
            <tr>
                <th>Load Id</th>
                <th>Data Path</th>
                <th>Load Type</th>
                <th>Source Object</th>
                <th>Source Row Count</th>
                <th>Target Table</th>
                <th>Target Row Count</th>
                <th>Count Diff</th>
                <th>Custom SQL Flag</th>
                <th>Run Time</th>
                <th>System Name</th>
                <th>Insert Count</th>
                <th>Load Start Time</th>
                <th>Load Count Gap</th>
                <th>Load Status</th>
                <th>Source DB</th>
            </tr> """

        for table_dict in self.table_meta_dlist:
            # if int(table_dict['count_diff']) <> 0:
            html_msg = html_msg + """\
            <tr>
                <td>""" + str(self.load_id) + """</td>
                <td>""" + self.data_path + """</td>
                <td>""" + table_dict['load_type'] + """</td>
                <td>""" + table_dict['source_tablename'] + """</td>
                <td>""" + str(table_dict['source_row_count']) + """</td>
                <td>""" + table_dict['target_tablename'] + """</td>
                <td>""" + str(table_dict['target_row_count']) + """</td>"""
            if int(table_dict['count_diff']) <> 0:
                html_msg = html_msg + """\
                <td id="td_bg_red">""" + str(table_dict['count_diff']) + """</td>"""
            else:
                html_msg = html_msg + """\
                <td>""" + str(table_dict['count_diff']) + """</td>"""
            html_msg = html_msg + """\
                <td>""" + table_dict['custom_sql_flag'] + """</td>
                <td>""" + self.run_time + """</td>
                <td>""" + str(table_dict['system_name']) + """</td>
                <td>""" + str(table_dict['insert_count']) + """</td>
                <td>""" + str(table_dict['load_start_time']) + """</td>
                <td>""" + str(table_dict['load_count_gap_mins']) + """ mins</td>"""
            if str(table_dict['load_status']).lower().find('finish') == -1:
                html_msg = html_msg + """\
                <td id="td_bg_red">""" + table_dict['load_status'] + """</td>"""
            else:
                html_msg = html_msg + """\
                <td>""" + table_dict['load_status'] + """</td>"""
            html_msg = html_msg + """\
                <td>""" + table_dict['source_db'] + """</td>
            </tr>"""

        html_msg = html_msg + """\
        </table>
    </body>"""

        return html_msg


    def __formatSetupMail(self, results):
        method_name = self.class_name + ": " + "__formatSetupMail"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
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
                <th>HVR Source Schema</th>
            </tr> """

        for result in results:
            html_msg = html_msg + """\
            <tr>
                <td>""" + str(result['id']) + """</td>
                <td>""" + self.data_path + """</td>
                <td>""" + result['load_type'] + """</td>
                <td>""" + result['source_tablename'] + """</td>
                <td>""" + result['target_tablename'] + """</td>
                <td>""" + str(result['hvr_source_schemaname']) + """</td>
            </tr>"""

        html_msg = html_msg + """\
        </table>
    </body>"""

        return html_msg


    def mainThread(self):
        method_name = self.class_name + ": " + "mainThread"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        try:
            print sys._getframe(1).f_code.co_filename
            # audit log
            error = 1
            err_msg = ''
            output_msg = ''
            audit_logging('', self.load_id, 0, self.plant_name, self.system_name, self.job_name, self.tablename,'Job Started', \
                          self.data_path, self.technology,self.rows_inserted,0, self.rows_deleted, error, err_msg ,0,0,output_msg)

            self.__preProcessing()

            if len(self.table_meta_dlist) == 0:
                error = 1
                err_msg = 'ERROR: No Table to process'
                output_msg = 'ERROR: No Table to process'
                output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
                self.__postProcessing(output)

            conn_output = self.__getEdlConn()
            if conn_output['error'] > 0:
                self.__postProcessing(conn_output)

            self.run_time = logdt.now().strftime('%Y-%m-%d %H:%M:%S')
            src_output = self.__getSourceCount()

            if src_output['error'] > 0:
                self.__postProcessing(src_output)

            if src_output.get('secondary_schema_name',None) <> None:
                conn_output = self.__getEdlConn(src_output['secondary_schema_name'])
                if conn_output['error'] > 0:
                    self.__postProcessing(conn_output)

                secondary_src_output = self.__getSourceCount(isSecondaryCallFlag=True)
                if secondary_src_output['error'] > 0:
                    self.__postProcessing(secondary_src_output)
                else:
                    src_output['cnt_output'].extend(secondary_src_output['cnt_output'])

            # print src_output

            tgt_output = self.__getTargetCount()
            # print tgt_output

            if tgt_output['error'] > 0:
                self.__postProcessing(tgt_output)

            load_stats_output = self.__getLoadStats()
            if load_stats_output['error'] > 0:
                self.__postProcessing(load_stats_output)

            self.__count_diff_flag = False
            for table_meta_dict in self.table_meta_dlist:

                table_meta_dict['source_row_count'] = -1
                table_meta_dict['target_row_count'] = -1
                table_meta_dict['insert_count'] = -1
                table_meta_dict['load_start_time'] = ''
                table_meta_dict['load_count_gap_mins'] = -1
                table_meta_dict['load_status'] = 'Ignored'

                for src_cnt_dict in src_output["cnt_output"]:
                    # if table_meta_dict['source_tablename'] == src_cnt_dict['table_name']:
                    if table_meta_dict['id'] == src_cnt_dict['control_id']:
                        table_meta_dict['source_row_count'] = src_cnt_dict['row_count']
                        table_meta_dict['source_db'] = src_cnt_dict['source_db']
                for tgt_cnt_dict in tgt_output["cnt_output"]:
                    if table_meta_dict['target_tablename'] == tgt_cnt_dict['table_name']:
                        table_meta_dict['target_row_count'] = tgt_cnt_dict['row_count']

                if len(load_stats_output['load_stats_results']) <> 0:
                    for load_stats_dict in load_stats_output['load_stats_results']:
                        if table_meta_dict['target_tablename'] == load_stats_dict['table_name']:
                            table_meta_dict['insert_count'] = load_stats_dict['insert_count']
                            table_meta_dict['load_start_time'] = load_stats_dict['load_start_time']
                            table_meta_dict['load_count_gap_mins'] = ((logdt.strptime(self.run_time, '%Y-%m-%d %H:%M:%S') - (logdt.strptime(load_stats_dict['load_start_time'], '%Y-%m-%d %H:%M:%S'))).seconds)/60
                            table_meta_dict['load_status'] = load_stats_dict['load_status']

                # table_meta_dict['source_row_count'] = table_meta_dict.get('source_row_count', -1)
                # table_meta_dict['target_row_count'] = table_meta_dict.get('target_row_count', -1)

                if table_meta_dict['source_row_count'] == -1 or table_meta_dict['target_row_count'] == -1:
                    table_meta_dict['count_diff'] = -1
                else:
                    table_meta_dict['count_diff'] = int(table_meta_dict['source_row_count']) - int(table_meta_dict['target_row_count'])

                if not self.__count_diff_flag and int(table_meta_dict['count_diff']) <> 0:
                    self.__count_diff_flag = True

            # print self.table_meta_dlist

            insert_output = self.__insertQualityResult()
            if insert_output['error'] > 0:
                self.__postProcessing(insert_output)

            output = {"error": 0, "err_msg": ''}
            self.__postProcessing(output)

        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Encountered error in mainThread".format(error)
            output_msg = "ERROR: Encountered error in mainThread" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            self.__postProcessing(output)


    def __getSourceCount(self, isSecondaryCallFlag=False):
        method_name = self.class_name + ": " + "__getSourceCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        if self.schema_system_name in ['adw', 'adw_wkly', 'ats', 'erp_oracle', 'eservice', 'eservice_kafka', 'get_abm', 'get_dm', 'gets_msa', 'gets_rin', 'ncmr', 'nucleus', 'odw', 'odw_2', 'odw_hrly', 'odw_kaf', 'odw_kafka',
                                       'odw_dyn_prcg', 'odw_gl_det', 'pan_ins_erp', 'reliance', 'reliance4hours', 'reliance_ppap', 'reliance_ppap_sd', 'rmd',
                                       'sas_adw', 'scp', 'sdr', 'teamcenter', 'todw', 'tscollp']:
            return self.__getOracleCount(isSecondaryCallFlag)
        elif self.schema_system_name in ['cas', 'cas_2', 'erp_sqlwh', 'erp_sqlwh_2', 'mfgdata', 'pan_ins', 'proficy', 'proficy_kafka', 'proficy_grr', 'proficy_grr_kafka', 'shop_supt', 'shop_supt_2', 'srs', 'tspbtprd']:
            return self.__getSQLServerCount()

        error = 1
        err_msg = method_name + "[{0}]: No Source count logic for ".format(error) + self.schema_system_name
        output_msg = "ERROR: No Source count logic for " + self.schema_system_name + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
        return output


    def __getOracleCount(self, isSecondaryCallFlag=False):
        method_name = self.class_name + ": " + "__getOracleCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''

        import cx_Oracle as cx_ora
        conn_ora = None
        try:
            conn_ora, cur_ora = dbConnectOra(self.__db_hostname, self.__db_port, self.__database, self.__db_username, self.__db_edlpgppass)

            count_sql = None
            table_list = []
            secondary_schema_name = None
            # secondary_table_list = []
            # for table_dict in self.table_meta_dlist:
            #     if self.data_path in ['SRC2Hive','KFK2HIve'] and self.schema_system_name == 'odw' and (table_dict['source_tablename']).find('gets_dw_rdb') <> -1:
            #         # secondary_schema_name = 'tscollp'
            #         secondary_schema_name = None
            #         secondary_table_list.append(table_dict['source_tablename'])
            #     else:
            #         table_list.append(table_dict['source_tablename'])
            #
            # if isSecondaryCallFlag:
            #     table_distinct_list = list(set(secondary_table_list))
            # else:
            #     table_distinct_list = list(set(table_list))

            # for idx, table in enumerate(table_distinct_list):
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Checking for direct source tables........")
            idx = 0
            for table_dict in self.table_meta_dlist:
                if table_dict['custom_sql_flag'] <> 'Y':
                    count_tmp_sql = "SELECT " + str(table_dict['id']) + " AS CONTROL_ID, '" + table_dict['source_tablename'] + "' AS TABLE_NAME, " \
                                    "COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB " \
                                "FROM " + table_dict['source_tablename']
                # if idx == 0:
                #     count_sql = "SELECT '" + table + "' AS TABLE_NAME, COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB FROM " + table
                # # elif idx < 3:
                # else:
                #     count_sql = count_sql + '\n' + 'UNION' + '\n' + "SELECT '" + table + "' AS TABLE_NAME, COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB FROM " + table
                    if idx == 0:
                        count_sql  = count_tmp_sql
                    else:
                        count_sql = count_sql + '\n' + 'UNION' + '\n' + count_tmp_sql
                    idx += 1

            cnt_output = []
            if idx > 0:
                print count_sql

                cur_ora.execute(count_sql)
                results = cur_ora.fetchall()

                # print results
                for result in results:
                    control_id, table_name, row_count, source_db = result
                    cnt_output.append({"control_id":control_id, "table_name":table_name, "row_count": row_count, 'source_db': source_db})

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Checking for custom sql source tables........")
            for idx,table_dict in enumerate(self.table_meta_dlist):
                if table_dict['custom_sql_flag'] == 'Y':
                    count_sql = "SELECT " + str(table_dict['id']) + " AS CONTROL_ID, '" + table_dict['source_tablename'] + "' AS TABLE_NAME, " \
                                    "COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB " \
                                "FROM ( " + table_dict['custom_sql'] + " )"

                    print count_sql
                    cur_ora = conn_ora.cursor()
                    cur_ora.execute(count_sql)
                    results = cur_ora.fetchall()
                    for result in results:
                        control_id, table_name, row_count, source_db = result
                        cnt_output.append({"control_id":control_id, "table_name":table_name, "row_count": row_count, 'source_db': source_db})

            # print cnt_output
            output = {"error": error, "err_msg": err_msg, "cnt_output": cnt_output, "secondary_schema_name": secondary_schema_name}
            return output
        except cx_ora.Error as oe:
            import getpass
            import socket
            osuser = getpass.getuser()
            hostname = socket.gethostname()
            ora_error, = oe.args
            error = 1
            err_msg = method_name + "[{0}]: Encountered SQL error getting Oracle count".format(error)
            output_msg = "Encountered error getting Oracle count in server " + hostname + " for osuser " + osuser + ": Error: " + str(ora_error.code) + ": " + ora_error.message
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        except Exception as e:
            error = 2
            err_msg = method_name + "[{0}]: Encountered error getting Oracle count".format(error)
            output_msg = "ERROR: Encountered error getting Oracle count" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        finally:
            try:
                if conn_ora is not None:
                    conn_ora.close()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "\n" + traceback.format_exc())


    def __getSQLServerCount(self):
        method_name = self.class_name + ": " + "__getSQLServerCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''

        import pyodbc

        conn_sqlServer = None
        try:
            # conn_parm_string = 'driver={ODBC driver 17 for SQL Server};server=' + self.__db_hostname + ';database=' + self.__database + ';uid=' + self.__db_username + ';pwd=' + self.__db_edlpgppass
            conn_sqlServer, cur_sqlServer = dbConnectSQLServer(self.__db_hostname, self.__db_port, self.__database,
                                                               self.__db_username, self.__db_edlpgppass)

            count_sql = None
            # table_list = []
            # for table_dict in self.table_meta_dlist:
            #     table_list.append(table_dict['source_tablename'])
            #
            # table_distinct_list = list(set(table_list))
            #
            # for idx, table in enumerate(table_distinct_list):
            #     if idx == 0:
            #         count_sql = "SELECT '" + table + "' AS TABLE_NAME, COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB FROM " + table
            #     # elif idx < 3:
            #     else:
            #         count_sql = count_sql + '\n' + 'UNION' + '\n' + "SELECT '" + table + "' AS TABLE_NAME, COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB FROM " + table
            #
            # print count_sql
            #
            # cur_sqlServer.execute(count_sql)
            # results = cur_sqlServer.fetchall()
            #
            # # print results
            # cnt_output = []
            # for result in results:
            #     table_name, row_count, source_db = result
            #     cnt_output.append({"table_name":table_name, "row_count": row_count, 'source_db':source_db})

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Checking for direct source tables........")
            idx = 0
            for table_dict in self.table_meta_dlist:
                if table_dict['custom_sql_flag'] <> 'Y':
                    count_tmp_sql = "SELECT " + str(table_dict['id']) + " AS CONTROL_ID, '" + table_dict['source_tablename'] + "' AS TABLE_NAME, " \
                                    "COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB " \
                                "FROM " + table_dict['source_tablename']
                    if idx == 0:
                        count_sql  = count_tmp_sql
                    else:
                        count_sql = count_sql + '\n' + 'UNION' + '\n' + count_tmp_sql
                    idx += 1

            cnt_output = []
            if idx > 0:
                print count_sql

                cur_sqlServer.execute(count_sql)
                results = cur_sqlServer.fetchall()

                # print results
                for result in results:
                    control_id, table_name, row_count, source_db = result
                    cnt_output.append({"control_id":control_id, "table_name":table_name, "row_count": row_count, 'source_db': source_db})

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Checking for custom sql source tables........")
            for idx,table_dict in enumerate(self.table_meta_dlist):
                if table_dict['custom_sql_flag'] == 'Y':
                    count_sql = "SELECT " + str(table_dict['id']) + " AS CONTROL_ID, '" + table_dict['source_tablename'] + "' AS TABLE_NAME, " \
                                    "COUNT(*) AS ROW_COUNT, '" + self.__database + "@" + self.__db_hostname + "' as SOURCE_DB " \
                                "FROM ( " + table_dict['custom_sql'] + " ) AS T"

                    print count_sql
                    cur_sqlServer = conn_sqlServer.cursor()
                    cur_sqlServer.execute(count_sql)
                    results = cur_sqlServer.fetchall()
                    for result in results:
                        control_id, table_name, row_count, source_db = result
                        cnt_output.append({"control_id":control_id, "table_name":table_name, "row_count": row_count, 'source_db': source_db})

            # print cnt_output
            output = {"error": error, "err_msg": err_msg, "cnt_output": cnt_output}
            return output
        except pyodbc.Error as oe:
            error = 1
            err_msg = method_name + "[{0}]: Encountered SQL error getting SQlServer count".format(error)
            output_msg = "Encountered error getting SQlServer count: Error: " + str(oe)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        except Exception as e:
            error = 2
            err_msg = method_name + "[{0}]: Encountered error getting SQL Server count".format(error)
            output_msg = "ERROR: Encountered error getting SQL Server count" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        finally:
            try:
                if conn_sqlServer is not None:
                    conn_sqlServer.close()
            except Exception as e:
                print (print_hdr + "\n" + traceback.format_exc())


    def __getTargetCount(self):
        method_name = self.class_name + ": " + "__getTargetCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        if self.data_path.find('2Hive') <> -1:
            return self.__getHiveCount()

        error = 1
        err_msg = method_name + "[{0}]: No Target count logic for ".format(error) + self.schema_system_name
        output_msg = "ERROR: No Target count logic for " + self.schema_system_name + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
        return output


    def __getHiveCount(self):
        method_name = self.class_name + ": " + "__getHiveCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''
        output = {}

        import mysql.connector

        conn_hmeta = None
        conn_hive = None
        cnt_output = []
        try:
            if self.data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
                dbUrl                       = self.config_list['mysql_dbUrl']
                dbUser                      = self.config_list['mysql_dbUser']
                dbPwd                       = base64.b64decode(self.config_list['mysql_dbPwd'])
                dbMetastore_dbName          = self.config_list['mysql_dbMetastore_dbName']
                dbApp_dbName                = self.config_list['mysql_dbApp_dbName']

                conn_hmeta = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)

                hive_schema_filter = ''
                if self.data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
                    target_schema_list = []
                    for table in self.table_meta_dlist:
                        target_schema_list.append(table['target_schemaname'])
                    hive_schema_filter = ",".join("'" + l + "'" for l in list(set(target_schema_list)))
                elif self.data_path in ('SRC2Hive','KFK2Hive'):
                    hive_schema_filter = "'" + self.schema_system_name + "'"

                count_sql = "select  concat(d.NAME,'.',t.TBL_NAME) AS TABLE_NAME, tp.ROW_CNT AS ROW_COUNT " \
                        "from hive.TBLS t " \
                        "inner join hive.DBS d on d.DB_ID = t.DB_ID " \
                        "left outer join ( " \
                        "        select  TBL_ID, " \
                        "                coalesce(group_concat(if(PARAM_KEY='numRows', PARAM_VALUE, NULL)),0) AS ROW_CNT " \
                        "        from hive.TABLE_PARAMS " \
                        "        group by TBL_ID) tp " \
                        "on tp.TBL_ID = t.TBL_ID " \
                        "left outer join (select TBL_ID, count(*) as PARTITION_COL_CNT, group_concat(PKEY_NAME) as PARTITION_COLS from hive.PARTITION_KEYS group by TBL_ID) pc " \
                        "on pc.TBL_ID = t.TBL_ID " \
                        "where 1 = 1 " \
                            "and d.name in (" + hive_schema_filter + ") " \
                            "and pc.PARTITION_COL_CNT is null " \
                            "and t.TBL_TYPE like '%TABLE' " \
                            "and (t.TBL_NAME not like '%_ext' and t.TBL_NAME not like '%_kext' and t.TBL_NAME not like '%_talend' and t.TBL_NAME not like '%_hvr') " \
                        "union " \
                        "select  concat(d.NAME,'.',t.TBL_NAME) AS TABLE_NAME, CAST(sum(pp.ROW_CNT) AS SIGNED) AS ROW_COUNT " \
                        "from hive.PARTITIONS p " \
                        "inner join TBLS t on t.TBL_ID = p.TBL_ID " \
                        "inner join DBS d on d.DB_ID = t.DB_ID " \
                        "left outer join ( " \
                        "        select  PART_ID, " \
                        "                coalesce(group_concat(if(PARAM_KEY='numRows', PARAM_VALUE, NULL)),0) AS ROW_CNT " \
                        "        from hive.PARTITION_PARAMS " \
                        "        group by PART_ID) pp " \
                        "on pp.PART_ID = p.PART_ID " \
                        "left outer join (select TBL_ID, count(*) as PARTITION_COL_CNT, group_concat(PKEY_NAME) as PARTITION_COLS from hive.PARTITION_KEYS group by TBL_ID) pc " \
                        "on pc.TBL_ID = t.TBL_ID " \
                        "where 1 = 1 " \
                            "and d.name in (" + hive_schema_filter + ") " \
                            "and t.TBL_TYPE like '%TABLE' " \
                            "and (t.TBL_NAME not like '%_ext' and t.TBL_NAME not like '%_kext' and t.TBL_NAME not like '%_talend' and t.TBL_NAME not like '%_hvr') " \
                            "and pc.PARTITION_COL_CNT is not null " \
                        "group by d.NAME, t.TBL_NAME " \
                        "order by TABLE_NAME "

                print count_sql

                cur_hmeta = conn_hmeta.cursor()
                cur_hmeta.execute(count_sql)
                results = cur_hmeta.fetchall()

                # print results
                for result in results:
                    table_name, row_count = result
                    cnt_output.append({"table_name":table_name, "row_count": row_count})

            elif self.data_path in ['SRC2Hive','KFK2Hive']:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    try:
                        conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                    except Exception as e:
                        raise

                count_sql = None
                table_list = []
                for table_dict in self.table_meta_dlist:
                    table_list.append(table_dict['target_tablename'])

                table_distinct_list = list(set(table_list))

                for idx, table in enumerate(table_distinct_list):
                    if idx == 0:
                        count_sql = "SELECT '" + table + "' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM `" + table + "` WHERE hvr_is_deleted = 0"
                    # elif idx < 3:
                    else:
                        count_sql = count_sql + '\n' + 'UNION' + '\n' + "SELECT '" + table + "' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM `" + table + "` WHERE hvr_is_deleted = 0"

                print count_sql

                cur_hive = conn_hive.cursor()
                cur_hive.execute(count_sql)
                results = cur_hive.fetchall()

                # print results
                for result in results:
                    table_name, row_count = result
                    cnt_output.append({"table_name":table_name, "row_count": row_count})

            output = {"error": error, "err_msg": err_msg, "cnt_output": cnt_output}
            return output

        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Encountered error getting Hive count".format(error)
            output_msg = "ERROR: Encountered error getting Hive count" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        finally:
            try:
                if conn_hmeta is not None:
                    conn_hmeta.close()
                if conn_hive is not None:
                    conn_hive.close()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "\n" + traceback.format_exc())


    def __insertQualityResult(self):
        method_name = self.class_name + ": " + "__insertQualityResult"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''

        conn_metadata = None
        try:
            conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'], self.config_list['meta_db_dbUser'],
                                                    self.config_list['meta_db_dbUrl'], base64.b64decode(self.config_list['meta_db_dbPwd']))

            # delete_sql = 'DELETE FROM sbdt.datasync_quality WHERE load_id = ' + self.load_id
            # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "delete_sql: " + delete_sql)
            #
            # cur_metadata.execute(delete_sql)
            # self.rows_deleted = cur_metadata.rowcount
            # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "rows deleted: " + str(self.rows_deleted))

            insert_sql = 'INSERT INTO sbdt.datasync_quality (load_id, data_path, load_type, source_tablename, source_row_count, target_tablename, target_row_count, diff_count, ' \
                                                        'run_time, source_db, system_name, insert_count, load_start_time, load_count_gap_mins, load_status) VALUES \n'
            for idx, table_dict in enumerate(self.table_meta_dlist):
                # if idx == 0:
                #     insert_sql = insert_sql + "(" + str(self.load_id) + ",'" + self.data_path + "','" + table_dict['load_type'] + "','" + table_dict['source_tablename'] + "'," + \
                #                  str(table_dict['source_row_count']) + ",'" + table_dict['target_tablename'] + "'," + str(table_dict['target_row_count']) + "," + \
                #                  str(table_dict['count_diff']) + ",'" + self.run_time + "','" + str(table_dict['source_db']) + "','" + str(table_dict['system_name']) + "'," + \
                #                  str(table_dict['insert_count']) + ",'" + table_dict['load_start_time'] + "'," + str(table_dict['load_count_gap_mins']) + ")"
                # else:
                #     insert_sql = insert_sql + ",\n(" + str(self.load_id) + ",'" + self.data_path + "','" + table_dict['load_type'] + "','" + table_dict['source_tablename'] + "'," + \
                #                  str(table_dict['source_row_count']) + ",'" + table_dict['target_tablename'] + "'," + str(table_dict['target_row_count']) + "," + \
                #                  str(table_dict['count_diff']) + ",'" + self.run_time + "','" + str(table_dict['source_db']) + "','" + str(table_dict['system_name']) + "'," + \
                #                  str(table_dict['insert_count']) + ",'" + table_dict['load_start_time'] + "'," + str(table_dict['load_count_gap_mins']) + ")"
                if idx == 0:
                    insert_sql = insert_sql + "("
                else:
                    insert_sql = insert_sql + ",\n("

                insert_sql = insert_sql + str(self.load_id) + ",'" + self.data_path + "','" + table_dict['load_type'] + "','" + table_dict['source_tablename'] + "'," + \
                            str(table_dict['source_row_count']) + ",'" + table_dict['target_tablename'] + "'," + str(table_dict['target_row_count']) + "," + \
                            str(table_dict['count_diff']) + ",'" + self.run_time + "','" + str(table_dict['source_db']) + "','" + str(table_dict['system_name']) + "'," + str(table_dict['insert_count'])

                if len(table_dict['load_start_time']) == 0:
                    insert_sql = insert_sql + ",NULL"
                else:
                    insert_sql = insert_sql + ",'" + table_dict['load_start_time'] + "'"

                insert_sql = insert_sql + "," + str(table_dict['load_count_gap_mins']) + ",'" + str(table_dict['load_status']) + "')"

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "insert_sql: " + insert_sql)

            cur_metadata.execute(insert_sql)
            self.rows_inserted = cur_metadata.rowcount
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "rows inserted: " + str(self.rows_inserted))

            conn_metadata.commit()

            output = {"error": error, "err_msg": err_msg}
            return output

        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Encountered error writing datasync quality results in table".format(error)
            output_msg = "ERROR: Encountered error writing datasync quality results in table" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            try:
                if conn_metadata is not None and not conn_metadata.closed:
                    conn_metadata.rollback()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "\n" + traceback.format_exc())
            return output
        finally:
            try:
                if conn_metadata is not None and not conn_metadata.closed:
                    conn_metadata.close()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "\n" + traceback.format_exc())


    def __getLoadStats(self):
        method_name = self.class_name + ": " + "__getLoadStats"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''

        conn_metadata = None
        try:
            conn_metadata, cur_metadata = dbConnect(config_list['meta_db_dbName'], config_list['meta_db_dbUser'],
                                                    config_list['meta_db_dbUrl'], base64.b64decode(config_list['meta_db_dbPwd']))

            load_stats_sql = "select table_name, load_status, insert_count, to_char(min_log_time,'YYYY-MM-DD HH24:MI:SS') as load_start_time " \
                            "from ( " \
                                    "select table_name, status as load_status, no_of_inserts as insert_count, log_time, " \
                                        "min(log_time) over (partition by load_id, table_name) as min_log_time, max(log_time) over (partition by load_id, table_name) as max_log_time "\
                                    "from sbdt.edl_log "\
                                    "where 1 = 1 "\
                                        "and plant_name = 'DATASYNC' "\
                                        "and table_name <> 'sbdt.datasync_quality' " \
                                        "and load_id = " + str(self.load_id) + " "\
                                        "and data_path = '" + self.data_path + "') T1 "\
                            "where max_log_time = log_time "

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "load_stats_sql: " + load_stats_sql)

            load_stats_results = dbQuery(cur_metadata, load_stats_sql)

            # print load_stats_results
            output = {"error": error, "err_msg": err_msg, "load_stats_results":load_stats_results}
            return output

        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Encountered error while fetching load stats".format(error)
            output_msg = "ERROR: Encountered error while fetching load stats" + "\n" + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
            return output
        finally:
            try:
                if conn_metadata is not None and not conn_metadata.closed:
                    conn_metadata.close()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "\n" + traceback.format_exc())


    def __getTargetDuplicateCount(self):
        method_name = self.class_name + ": " + "__getTargetDuplicateCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        if self.data_path.find('2Hive') <> -1:
            return self.__getHiveDuplicateCount()

        error = 1
        err_msg = method_name + "[{0}]: No Target count logic for ".format(error) + self.schema_system_name
        output_msg = "ERROR: No Target count logic for " + self.schema_system_name + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        output = {"error": error, "err_msg": err_msg, "output_msg": output_msg}
        return output


    def __getHiveDuplicateCount(self):
        method_name = self.class_name + ": " + "__getHiveDuplicateCount"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " ": " + self.schema_system_name + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")


if __name__ == "__main__":

    print ('\n******************************************************************************************************************************************************\n')
    print_hdr = "[datasync_quality: main] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    config_list = load_config()
    emailReceiver              = config_list['email_dataQualityReceivers']

    error = 0

    if len(sys.argv) < 4:
        mail_subject = config_list['env'] + ": ERROR: Datasync Quality"
        output_msg = "ERROR: Mandatory input arguments not passed"
        print print_hdr + output_msg
        sendMailHTML(emailReceiver, mail_subject, output_msg)
        sys.exit(1)

    schema_system_name  = sys.argv[1]
    data_path           = sys.argv[2]
    load_id             = sys.argv[3]

    load_type = None
    if len(sys.argv) > 4:
        load_type = sys.argv[4]
        if load_type == 'None':
            load_type = None
        elif load_type not in ['INCREMENTAL', 'FULL']:
            mail_subject = "ERROR: Datasync Quality: Unsupported Input"
            output_msg = "ERROR: Quality check for load_type: " + load_type + " not supported"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            sendMailHTML(emailReceiver, mail_subject, output_msg)
            sys.exit(1)
        else:
            load_type = "'" + load_type + "'"

    if load_type == None:
        load_type = "'INCREMENTAL', 'FULL'"

    print_hdr = "[datasync_quality: main: " + data_path + ": " + schema_system_name + ": " + load_id + ": " + load_type + "] - "

    if data_path not in ['SRC2Hive','Talend2Hive','KFK2Hive','SQOOP2Hive','NiFi2Hive']:
        mail_subject = "ERROR: Datasync Quality: Unsupported Input"
        output_msg = "ERROR: Quality check for datapath: " + data_path + " not supported"
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(emailReceiver, mail_subject, output_msg)
        sys.exit(1)


    conn_metadata = None
    try:
        # print sys._getframe(1).f_code.co_filename

        conn_metadata, cur_metadata = dbConnect(config_list['meta_db_dbName'], config_list['meta_db_dbUser'],
                                                config_list['meta_db_dbUrl'], base64.b64decode(config_list['meta_db_dbPwd']))

        check_sql = "SELECT max(run_time) as max_run_time, max(load_id) as max_load_id " \
                    "FROM sbdt.datasync_quality "\
                    "WHERE 1 = 1 "
        if data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
            check_sql = check_sql + \
                        "AND system_name = '" + schema_system_name + "' "
        elif data_path in ['SRC2Hive', 'KFK2Hive']:
            check_sql = check_sql + \
                        "AND target_tablename like '" + schema_system_name + ".%' "
        if load_type in ['INCREMENTAL','FULL']:
            check_sql =  check_sql + \
                        "AND load_type = '" + load_type + "' "
        check_sql = check_sql + \
                        "AND data_path = '" + data_path + "' " \
                        "AND run_time > current_timestamp - interval '" + config_list['misc_qualityCheckDurationMins'] + " mins' "

        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "check_sql: " + check_sql)
        check_results = dbQuery(cur_metadata, check_sql)

        # print check_results
        if check_results[0]['max_run_time'] <> None:
            output_msg = "WARNING: Last Run in " + str(config_list['misc_qualityCheckDurationMins']) + " mins for schema system: " + schema_system_name + \
                         " and data path: " + data_path + " and load type: " + load_type + ": " + (check_results[0]['max_run_time']).strftime('%Y-%m-%d %H:%M:%S') + " for load_id: " + str(check_results[0]['max_load_id'])
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            sys.exit(0)

        metadata_sql = "SELECT id, case when data_path in ('SRC2Hive','KFK2Hive') then coalesce(hvr_source_schemaname, source_schemaname)||'.'||source_tablename else source_schemaname||'.'||source_tablename end as source_tablename, " \
                       "target_schemaname||'.'||target_tablename as target_tablename, load_type, source_schemaname, target_schemaname, system_name, hvr_source_schemaname, " \
                       "CASE WHEN (custom_sql is NULL OR trim(custom_sql) = '') THEN 'N' ELSE 'Y' END AS custom_sql_flag, custom_sql " \
                    "FROM sync.control_table c " \
                    "where  1 = 1 "
        if data_path in ['Talend2Hive', 'SQOOP2Hive', 'NiFi2Hive']:
            metadata_sql = metadata_sql + \
                        "AND system_name = '" + schema_system_name + "' "
        elif data_path in ['SRC2Hive','KFK2Hive']:
            metadata_sql = metadata_sql + \
                        "AND target_schemaname = '" + schema_system_name + "' "

        metadata_sql = metadata_sql + \
                        "AND source_schemaname not in ('ftp') " \
                        "AND status_flag = 'Y' " \
                        "AND data_path = '" + data_path + "' " \
                        "AND load_type IN (" + load_type + ") " \
                        "AND target_tablename not like 'gl_det_*' " \
                        "ORDER BY target_tablename"
                        # "AND EXISTS ( " \
                        #     "SELECT 1 FROM sbdt.edl_log l " \
                        #     "WHERE load_id = " + str(load_id) + " " \
                        #         "AND l.table_name = c.target_schemaname||'.'||c.target_tablename) " \
                        # "ORDER BY target_tablename"

        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metadata_sql: " + metadata_sql)

        meta_results = dbQuery(cur_metadata, metadata_sql)

        if len(meta_results) == 0:
            mail_subject = "WARNING: Datasync Quality: " + schema_system_name + ": " + data_path + ": " + load_type + ": " + str(load_id)
            output_msg = "WARNING: No setup for schema system: " + schema_system_name + " and data path: " + data_path + " and load type: " + load_type + " in datasync control or context parameters table"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            sendMailHTML(emailReceiver, mail_subject, output_msg)
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            sys.exit(0)

        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "results: ", meta_results)

        datasyncQuality = DatasyncQuality()

        datasyncQuality.data_path = data_path
        datasyncQuality.schema_system_name = schema_system_name
        datasyncQuality.load_id = load_id
        datasyncQuality.table_meta_dlist = meta_results
        datasyncQuality.emailReceiver = emailReceiver

        datasyncQuality.mainThread()

    except Exception as e:
        mail_subject = "ERROR: Datasync Quality: " + schema_system_name + ": " + data_path
        output_msg = "ERROR: Encountered error while running job" + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(emailReceiver, mail_subject, output_msg)
        sys.exit(0)
    finally:
        if conn_metadata is not None and not conn_metadata.closed:
            conn_metadata.close()

    sys.exit(0)

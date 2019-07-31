
import mysql.connector
import base64
import sys
import traceback
import shutil
import datetime
from datetime import datetime
sys.path.append("/apps/datasync/scripts/")
sys.path.append("/apps/common/")
sys.path.append("/data/analytics/common/")
from utils import remove_files, dbConnect, dbQuery, sendMail, run_cmd, call_spark_submit, move_files, txn_dbConnect, call_datasync_method, dbConnectHive
from auditLog import audit_logging
import psycopg2
import time
from datasync_init import DataSyncInit
import os
import glob
import subprocess
from datetime import datetime as logdt


class File2DBSync(DataSyncInit):

    def __init__(self, input=None):
        print ("[File2DBSync: __init__] -  Entered")
        super(File2DBSync, self).__init__()

        self.class_name = self.__class__.__name__

        if input is not None:
            self.id = input['id']
            self.source_schemaname = input['source_schemaname']
            self.source_tablename = input['source_tablename']
            self.target_schemaname = input['target_schemaname']
            self.target_tablename = input['target_tablename']
            self.load_type = input['load_type']
            self.incremental_column = input['incremental_column']
            self.last_run_time = input['last_run_time']
            self.second_last_run_time = input['second_last_run_time']
            self.join_columns = input['join_columns']
            self.log_mode = input['log_mode']
            self.data_path = input['data_path']
            self.load_id = input['load_id']
            self.plant_name = input['plant_name']
            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            self.is_special_logic = input.get('is_special_logic', False)
            self.is_partitioned = input.get('is_partitioned', False)
            if self.data_path.find("GP2HDFS") <> -1 and self.target_schemaname <> self.source_schemaname and self.is_special_logic:
                self.source_schemaname = self.target_schemaname
        else:
            print "[File2DBSync: __init__] - No data defined"


    def load_hive(self, control=None):
        method_name = self.class_name + ": " + 'load_hive'

        self.technology = 'Python'
        self.system_name = 'HIVE'
        # self.job_name = 'HDFS-->HIVE'
        # status = "Started Spark Job"
        # Overwrite data path for Hive load
        # self.data_path = "HDFS2MIR"
        if self.data_path.find("HDFS2MIR") <> -1:
            self.job_name = 'HDFS-->HIVE'
        elif self.data_path.find("MIR2") <> -1:
            self.job_name = 'HIVE-->HIVE'
        elif self.data_path.find("SRC2Hive") <> -1:
            self.job_name = 'SRC-->HIVE'
        elif self.data_path.find("Talend2Hive") <> -1:
            self.job_name = 'Talend-->HIVE'
        elif self.data_path.find("KFK2Hive") <> -1:
            self.job_name = 'Kafka-->HIVE'
        elif self.data_path.find("SQOOP2Hive") <> -1:
            self.job_name = 'Sqoop-->HIVE'
        elif self.data_path.find("NiFi2Hive") <> -1:
            self.job_name = 'NiFi-->HIVE'

        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + ": Entered")

        error = 0
        err_msg = ''

        metastore_dbName            = self.config_list['meta_db_dbName']
        dbmeta_Url                  = self.config_list['meta_db_dbUrl']
        dbmeta_User                 = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])
        # emailSender                 = self.config_list['email_sender']
        # emailReceiver               = self.config_list['email_receivers']
        path                        = self.config_list['misc_hdfsStagingPath']
        hvr_hdfs_staging_path       = self.config_list['misc_hvrHdfsStagingPath']
        talend_hdfs_staging_path    = self.config_list['misc_talendHdfsStagingPath']

        if control is None:
            control = {'run_id': None, 'v_timestamp': None, 'rows_inserted': None, 'rows_updated': None, 'rows_deleted': None, 'num_errors': None}

        if (self.source_schemaname is None) or (self.source_tablename is None):
            error = 1
            err_msg = method_name + "[{0}]: No table name provided".format(error)
            status = 'Job Error'
            output_msg = "No table name provided"
            audit_logging(None, self.load_id, None, self.plant_name, self.system_name, self.job_name, None, status, self.data_path,
                          self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            self.error_cleanup(self.source_schemaname, self.source_tablename, control['run_id'], path)
            return error, err_msg
        else:
            tablename       = self.source_schemaname + '.' + self.source_tablename

        rows_inserted       = control['rows_inserted'] if control.has_key('rows_inserted') and control['rows_inserted'] is not None else 0
        rows_updated        = control['rows_updated'] if control.has_key('rows_updated') and control['rows_updated'] is not None else 0
        rows_deleted        = control['rows_deleted'] if control.has_key('rows_deleted') and control['rows_deleted'] is not None else 0
        # num_errors          = control['num_errors'] if control.has_key('num_errors') and control['num_errors'] is not None else 1
        output_msg          = ''
        conn_metadata       = None

        try:
            conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        except Exception as e:
            error = 2
            err_msg = method_name + "[{0}]: Error getting connection from database".format(error)
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            status = 'Job Error'
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, control['run_id'], path, conn_metadata)
            return error, err_msg

        run_id = 0
        if control.has_key('run_id') and control['run_id'] is not None:
            run_id          = control['run_id']
        else:
            try:
                run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
                run_id_lists = dbQuery(cur_metadata, run_id_sql)
                run_id_list = run_id_lists[0]
                run_id = run_id_list['nextval']
                print (print_hdr + "Run ID for the table " + tablename + " is: " + str(run_id))
            except Exception as e:
                error = 3
                err_msg = method_name + "[{0}]: Error connecting to database while fetching metadata".format(error)
                # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,
                              tablename, status, self.data_path, self.technology, rows_inserted, rows_updated,
                              rows_deleted, error, err_msg, 0, 0, output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, path, conn_metadata)
                return error, err_msg

        if control.has_key('v_timestamp') and  control['v_timestamp'] is not None:
            v_timestamp     = control['v_timestamp']
        else:
            t = datetime.fromtimestamp(time.time())
            v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "before call of spark submit checking for the existence of files.............................................................................@")
            if self.data_path == 'SRC2Hive':
                incoming_path = hvr_hdfs_staging_path + self.target_schemaname + "/*-" + self.target_tablename + ".csv"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "incoming_path: " + incoming_path)
                files = glob.glob(incoming_path)
                if len(files) == 0:
                    error = 2
                    err_msg = method_name + "[{0}]: No Data Found".format(error)
                    status = 'Job Finished'
                    output_msg = 'No Data Found'
                    # output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                    #                'rows_deleted': rows_deleted,
                    #                'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name,
                                  tablename, status, self.data_path, self.technology, rows_inserted, rows_updated,
                                  rows_deleted, error, err_msg, 0, 0, output_msg)
                    return error, err_msg

            elif self.data_path == 'Talend2Hive':
                incoming_path = path + self.source_schemaname + "/" + self.source_tablename + "_ext_talend/" + self.target_tablename + ".csv"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "incoming_path: " + incoming_path)
                (ret, out, err) = run_cmd(['hadoop', 'fs', '-test', '-f', incoming_path ])
                if ret:
                    error = 2
                    err_msg = method_name + "[{0}]: No Data Found".format(error)
                    status = 'Job Finished'
                    output_msg = 'No Data Found'
                    # output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                    #                'rows_deleted': rows_deleted,
                    #                'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name,
                                  tablename, status, self.data_path, self.technology, rows_inserted, rows_updated,
                                  rows_deleted, error, err_msg, 0, 0, output_msg)
                    return error, err_msg

            elif self.data_path == 'SQOOP2Hive':
                incoming_path = path + self.source_schemaname + "/" + self.source_tablename + "_ext_talend/" + "part-*"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "incoming_path: " + incoming_path)
                (ret, out, err) = run_cmd(['hadoop', 'fs', '-test', '-f', incoming_path ])
                if ret:
                    error = 2
                    err_msg = method_name + "[{0}]: No Data Found".format(error)
                    status = 'Job Finished'
                    output_msg = 'No Data Found'
                    # output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                    #                'rows_deleted': rows_deleted,
                    #                'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name,
                                  tablename, status, self.data_path, self.technology, rows_inserted, rows_updated,
                                  rows_deleted, error, err_msg, 0, 0, output_msg)
                    return error, err_msg

            elif self.data_path == 'NiFi2Hive':
                incoming_path = path + self.source_schemaname + "/" + self.source_tablename + "_ext_talend/" + self.source_schemaname + "." + self.source_tablename + "*.txt"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "incoming_path: " + incoming_path)
                (ret, out, err) = run_cmd(['hadoop', 'fs', '-ls', incoming_path])
                print ret, out, err
                (ret, out, err) = run_cmd(['hadoop', 'fs', '-test', '-f', incoming_path ])
                if ret:
                    error = 2
                    err_msg = method_name + "[{0}]: No Data Found".format(error)
                    status = 'Job Finished'
                    output_msg = 'No Data Found'
                    # output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                    #                'rows_deleted': rows_deleted,
                    #                'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name,
                                  tablename, status, self.data_path, self.technology, rows_inserted, rows_updated,
                                  rows_deleted, error, err_msg, 0, 0, output_msg)
                    return error, err_msg

            spark_app_name = 'datasync_spark_driver_' + tablename
            call_spark_submit(self.config_list['spark_params_sparkVersion'], self.config_list['spark_params_sparkMaster'], \
                                self.config_list['spark_params_deployMode'], self.config_list['spark_params_executorMemory'], \
                                self.config_list['spark_params_executorCores'], self.config_list['spark_params_driverMemory'], \
                                self.config_list['spark_params_driverMaxResultSize'], spark_app_name, \
                                self.config_list['spark_params_confFiles'], self.config_list['spark_params_loadScript'], \
                                self.source_schemaname, self.source_tablename, self.load_id, run_id, self.data_path, v_timestamp)

            status = ''
            if self.log_mode == 'DEBUG':
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename, status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
        except subprocess.CalledProcessError as e:
            error        = e.returncode if e.returncode is not None else 1
            err_msg      = method_name + "[{0}]: CalledProcessError while running spark submit - check log file for more details".format(error)
            status       = 'Job Error'
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            # output_msg   = traceback.format_exc()
            # output_msg   = err_msg + '\n\r' + str(e.output) + '\n\r' + traceback.format_exc()
            output_msg = err_msg + '\n\r' + traceback.format_exc()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + output_msg)
            audit_logging(cur_metadata, self.load_id,run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, path, conn_metadata)
            return error, err_msg
        except Exception as e:
            error        = 4
            err_msg      = method_name + "[{0}]: ERROR while running spark submit - check log file for more details".format(error)
            status       = 'Job Error'
            # remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schemaname, self.source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id,run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, error, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, path, conn_metadata)
            return error, err_msg
        return error, err_msg


    def sync_hive_wrapper(self, sc, table_name, load_id, run_id, data_path, v_timestamp):
        method_name = self.class_name + ": " + 'sync_hive_wrapper'
        print_hdr = "[" + method_name + ": " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        output = {}
        self.plant_name = 'DATASYNC'
        self.technology = 'PySpark'
        self.system_name = 'HIVE'

        self.data_path = data_path if data_path is not None else ""
        self.load_id = load_id if load_id is not None else -1
        input_schema_name = ''
        input_table_name = ''
        error = 0
        err_msg = ''

        if self.data_path.find("HDFS2MIR") <> -1:
            self.job_name = 'HDFS-->HIVE'
        elif self.data_path.find("MIR2") <> -1:
            self.job_name = 'HIVE-->HIVE'
        elif self.data_path.find("SRC2Hive") <> -1:
            self.job_name = 'SRC-->HIVE'
        elif self.data_path.find("Talend2Hive") <> -1:
            self.job_name = 'Talend-->HIVE'
        elif self.data_path.find("KFK2Hive") <> -1:
            self.job_name = 'Kafka-->HIVE'
        elif self.data_path.find("SQOOP2Hive") <> -1:
            self.job_name = 'Sqoop-->HIVE'
        elif self.data_path.find("NiFi2Hive") <> -1:
            self.job_name = 'NiFi-->HIVE'
        status = "Job Started"
        audit_logging('', self.load_id, run_id, self.plant_name, self.system_name, self.job_name, table_name, status, self.data_path,
                      self.technology, 0, 0, 0, error, err_msg, 0, 0, '')

        if table_name is not None and table_name.find('.') <> -1:
            input_schema_name, input_table_name = table_name.split('.')

        if self.data_path == 'SRC2Hive' or self.data_path == 'Talend2Hive' or self.data_path == 'SQOOP2Hive' or self.data_path == 'NiFi2Hive':
            try:
                metadata_sql = "SELECT * FROM sync.control_table \
                        WHERE target_tablename = '" + input_table_name + "' \
                            AND target_schemaname = '" + input_schema_name + "'" + " \
                            AND data_path = " + "'" + self.data_path + "'"

                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metadata_sql: " + metadata_sql)
                conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'],self.config_list['meta_db_dbUser'],
                                                        self.config_list['meta_db_dbUrl'],base64.b64decode(self.config_list['meta_db_dbPwd']))
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "before connecting to metastore controls")
                controls = dbQuery(cur_metadata, metadata_sql)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metastore controls:", controls)
                conn_metadata.close()
            except psycopg2.Error as e:
                error = 2
                err_msg = method_name + "[{0}]: Error connecting to control table database".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                output.update({'status': status, 'rows_inserted': 0, 'rows_updated': 0, 'rows_deleted': 0,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                return output

            if not controls:
                error = 3
                err_msg = method_name + "[{0}]: No Entry found in control table".format(error)
                status = 'Job Error'
                output_msg = "No Entry found in control table"
                output.update({'status': status, 'rows_inserted': 0, 'rows_updated': 0, 'rows_deleted': 0,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                return output

            self.id                                 = str(controls[0]['id'])
            self.source_schema                      = str(controls[0]['source_schemaname'])
            self.source_tablename                   = str(controls[0]['source_tablename'])
            self.target_schema                      = str(controls[0]['target_schemaname'])
            self.target_tablename                   = str(controls[0]['target_tablename'])
            partitioned                             = controls[0]['is_partitioned']
            self.load_type                          = str(controls[0]['load_type'])
            self.s3_backed                          = controls[0]['s3_backed']
            first_partitioned_column                = str(controls[0]['first_partitioned_column'])
            second_partitioned_column               = str(controls[0]['second_partitioned_column'])
            partitioned_column_transformation       = str(controls[0]['partition_column_transformation'])
            custom_sql                              = str(controls[0]['custom_sql'])
            self.join_columns                       = str(controls[0]['join_columns'])
            self.archived_enabled                   = controls[0]['archived_enabled']
            distribution_columns                    = str(controls[0]['distribution_columns'])
            dist_col_transformation                 = str(controls[0]['dist_col_transformation'])
            self.log_mode                           = str(controls[0]['log_mode'])
            last_run_time                           = str(controls[0]['last_run_time'])
            dbtgt_host                              = self.config_list['tgt_db_hive_dbUrl']
            dbtgt_host2                             = self.config_list['tgt_db_hive_dbUrl2']
            dbtgt_Port                              = self.config_list['tgt_db_hive_dbPort']
            dbtgt_Auth                              = self.config_list['tgt_db_hive_authMech']

        if (sc is None) or (table_name is None) or (load_id is None) or (run_id is None) or (data_path is None) or (v_timestamp is None) or (table_name.find('.') == -1):
            error = 1
            err_msg = method_name + "[{0}]: Input Entry Missing".format(error)
            status = 'Job Error'
            output_msg = "Input Entry Missing"

            output.update({'status': status, 'rows_inserted': 0, 'rows_updated': 0,'rows_deleted': 0,
                           'error': error, 'err_msg': err_msg,'output_msg': output_msg})
            self.error_cleanup(input_schema_name, input_table_name, run_id)

        else:
            if self.data_path.find("HDFS2MIR") == -1:
                self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)

            module_path = str(os.path.dirname(os.path.realpath(__file__))) + "/"
            method = "sync_hive_spark"

            output = call_datasync_method(module_path, self, method, sc, table_name, load_id, run_id, self.data_path, v_timestamp)

            if output['error'] > 0:
                self.error_cleanup(input_schema_name, input_table_name, run_id)
            else:
                rows_inserted = output.get('rows_inserted', 0)
                rows_updated = output.get('rows_updated', 0)
                rows_deleted = output.get('rows_deleted', 0)
                conn_metadata = None

                try:
                    if self.archived_enabled:
                        archive_path = 's3a://' + self.config_list['s3_bucket_name'] + '/' + self.target_schema + '/' + self.target_tablename + '_bkp/'
                        if self.s3_backed:
                            source_path = 's3a://' + self.config_list['s3_bucket_name'] + '/' + self.target_schema + '/' + self.target_tablename + '/'
                        else:
                            source_path = 'hdfs:/' + self.config_list['misc_hiveWarehousePath'] + self.target_schema + '.db/' + self.target_tablename + '/*'

                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "source_path: " + source_path)
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "archive_path: " + archive_path)

                        (ret, out, err) = run_cmd(['hadoop', 'distcp', source_path, archive_path])
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "distcp files between source and archive:" + str(ret))

                        if err:
                            error = 1
                            err_msg = method_name + "[{0}]: Error while loading data in archive location".format(error)
                            status = 'Job Error'
                            output_msg = traceback.format_exc()
                            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                           'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                            self.error_cleanup(input_schema_name, input_table_name, run_id,self.config_list['misc_hdfsStagingPath'], conn_metadata)
                            return output

                    conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'],self.config_list['meta_db_dbUser'],
                                                            self.config_list['meta_db_dbUrl'],base64.b64decode(self.config_list['meta_db_dbPwd']))

                    update_control_table_sql = "UPDATE sync.control_table \
                                            SET last_run_time = '" + v_timestamp + "', \
                                                second_last_run_time = last_run_time, \
                                                status_flag = 'Y', \
                                                load_status_cd = '" + self.CONTROL_STATUS_COMPLETED + "' \
                                            WHERE target_schemaname = '" + input_schema_name + "'"

                    if self.data_path == 'HDFS2MIR':
                        update_control_table_sql = update_control_table_sql \
                                                   + " AND target_tablename in( '" + input_table_name + "','" + input_table_name + "_ext')" \
                                                   + " AND data_path in ('GP2HDFS','HDFS2MIR')"
                    elif (self.data_path == 'Talend2Hive' or self.data_path == 'SQOOP2Hive') and self.target_schema == 'todw':
                        max_id_col_of_tgt_table_sql     = "SELECT id_column FROM sbdt.todw_table_list WHERE target_schema = 'todw' and target_name = '" + self.target_tablename + "'"
                        print max_id_col_of_tgt_table_sql
                        query_res                       = dbQuery(cur_metadata, max_id_col_of_tgt_table_sql)
                        for i in query_res:
                            max_id_col_of_tgt_table     = i['id_column']
                        print "ID Column for the table ", self.target_schema, ".", self.target_tablename ," is :",  max_id_col_of_tgt_table
                        max_id_value_of_tgt_table_sql = "SELECT max(" + max_id_col_of_tgt_table + ") FROM " + self.target_schema + "." + self.target_tablename
                        print max_id_value_of_tgt_table_sql
                        try:
                            conn_target, cur_target = dbConnectHive(dbtgt_host, dbtgt_Port, dbtgt_Auth)
                        except Exception as e:
                            try:
                                conn_target, cur_target = dbConnectHive(dbtgt_host2, dbtgt_Port, dbtgt_Auth)
                            except Exception as e:
                                error = 15
                                num_errors = 1
                                err_msg = method_name + "[{0}]: Error while connecting to target database".format(error)
                                status = 'Job Error'
                                output_msg = traceback.format_exc()
                                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name,
                                              self.job_name, (self.target_schema + "." + self.target_tablename), status, self.data_path, self.technology,
                                              rows_inserted, rows_updated, rows_deleted,
                                              num_errors, err_msg, 0, 0, output_msg)
                                return error, err_msg, output
                        max_val                     = dbQuery(cur_target, max_id_value_of_tgt_table_sql)
                        print "Max ID value is :", max_val
                        update_control_table_sql = "UPDATE sbdt.todw_table_list SET max_id_value = " + str(max_val[0][0]) + " WHERE target_schema = 'todw' AND target_name = '" + self.target_tablename +"'"
                        print update_control_table_sql
                        cur_metadata.execute(update_control_table_sql)
                    else:
                        update_control_table_sql = update_control_table_sql \
                                                   + " AND target_tablename in( '" + input_table_name + "')" \
                                                   + " AND data_path in ('" + self.data_path + "')"

                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "update_control_table_sql: " + update_control_table_sql)
                    cur_metadata.execute(update_control_table_sql)
                    if self.log_mode == 'DEBUG':
                        status = "Updated Control Table"
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,table_name, status,
                                      self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, error,err_msg, 0, 0, None)

                    error = 0
                    err_msg = method_name + "[{0}]: No Errors".format(error)
                    status = 'Job Finished'
                    # output_msg = 'Job Finished successfully'
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg})
                    #, 'output_msg': output_msg})

                except Exception as e:
                    error = 2
                    err_msg = method_name + "[{0}]: Error while updating the control table".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                finally:
                    if conn_metadata is not None:
                        conn_metadata.close()

        if self.data_path == 'KFK2Hive':
            remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schema, self.source_tablename, self.data_path)
        elif data_path == "SRC2Hive" and output["error"] == 0:
        # Move Files to HDFS Backup location
            try:
                from datetime import date
                today = date.today()
                todaystr = today.isoformat()
                path = self.config_list['misc_hvrHdfsStagingPath'] + self.target_schema + "/in_progress/*-" + self.target_tablename + ".csv"
                hvr_hdfs_backup_path_tmp = self.config_list['misc_hvrhdfsBackupPath'] + self.target_schema + "/" + todaystr + "/" + self.target_tablename + "/"
                move_files(path, hvr_hdfs_backup_path_tmp)
            except Exception as e:
                error = 13
                err_msg = "Error while moving files to HDFS directory"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  + err_msg)
                status = "Job Error"
                path_from = self.config_list['misc_hdfsStagingPath'] + self.source_schema + "/" + self.source_tablename + "/in_progress/"
                path_to = self.config_list['misc_hdfsStagingPath'] + self.source_schemaname
                move_files(path_from, path_to)
                output_msg = traceback.format_exc()
                print output_msg
                output.update(
                    {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                     'rows_deleted': rows_deleted,
                     'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id, ((self.config_list['misc_hvrHdfsStagingPath'] + self.source_schema + "/" + "/in_progress") + "/*-" + self.target_tablename + ".csv"), conn_metadata, None, None, self.config_list['misc_hvrHdfsStagingPath'])
                return output
            remove_files(self.config_list['misc_hdfsStagingPath'], self.source_schema, self.source_tablename,self.data_path, self.target_tablename)
        elif (data_path == "Talend2Hive"  or data_path == 'NiFi2Hive') and output["error"] == 0:
            remove_files(self.config_list['misc_hdfsStagingPath'], self.target_schema, self.source_tablename,self.data_path, self.target_tablename)
        else:
            remove_files(self.config_list['misc_hdfsStagingPath'], input_schema_name, input_table_name)

        audit_logging('', load_id, run_id, self.plant_name, self.system_name, self.job_name, table_name, output["status"], self.data_path,
                      self.technology, output["rows_inserted"], output["rows_updated"], output["rows_deleted"], output["error"], output["err_msg"], 0, 0, output["output_msg"])

        if len(output) == 0:
            output = {"error": 0, "err_msg": ''}
        return output


    def sync_hive_spark(self, sc, table_name, load_id, run_id, data_path, v_timestamp):
        from pyspark.sql import HiveContext

        method_name = self.class_name + ": " + 'sync_hive_spark'
        print_hdr = "[" + method_name + ": " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''
        output = {}

        # num_errors = 1
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        output = {}
        conn_metadata = None
        paths = None
        last_success_incoming_path = None

        input_schema_name, input_table_name = table_name.split('.')
        try:
            sqlContext = HiveContext(sc)

            if (self.config_list == None):
                # print (print_hdr + "Configuration Entry Missing")
                error = 1
                err_msg = method_name + "[{0}]: Configuration Entry Missing".format(error)
                status = 'Job Error'
                output_msg = "Configuration Entry Missing"
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id)
                return output


            # proceed to point everything at the 'branched' resources
            dbUrl                       = self.config_list['mysql_dbUrl']
            dbUser                      = self.config_list['mysql_dbUser']
            dbPwd                       = base64.b64decode(self.config_list['mysql_dbPwd'])
            dbMetastore_dbName          = self.config_list['mysql_dbMetastore_dbName']
            dbApp_dbName                = self.config_list['mysql_dbApp_dbName']
            bucket_name                 = self.config_list['s3_bucket_name']
            metastore_dbName            = self.config_list['meta_db_dbName']
            dbmeta_Url                  = self.config_list['meta_db_dbUrl']
            dbmeta_User                 = self.config_list['meta_db_dbUser']
            dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])
            paths                       = self.config_list['misc_hdfsStagingPath']
            kafka_path                  = self.config_list['misc_kafkaIncomingPath']
            hvr_hdfs_staging_path       = self.config_list['misc_hvrHdfsStagingPath']
            hvr_hdfs_backup_path        = self.config_list['misc_hvrhdfsBackupPath']
            dbtgt_host                  = self.config_list['tgt_db_hive_dbUrl']
            dbtgt_host2                 = self.config_list['tgt_db_hive_dbUrl2']
            dbtgt_Port                  = self.config_list['tgt_db_hive_dbPort']
            dbtgt_Auth                  = self.config_list['tgt_db_hive_authMech']

            # print (print_hdr, dbUrl, dbUser, dbMetastore_dbName, dbApp_dbName)

            run_id_sql = "select nextval('sbdt.edl_run_id_seq')"

            tablename = input_schema_name + "." + input_table_name
            conn_metadata = None

            # Adding psql connection for control table data
            try:
                metadata_sql = "SELECT * FROM sync.control_table \
                        WHERE target_tablename = '" + input_table_name + "' \
                            AND target_schemaname = '" + input_schema_name + "'" + " \
                            AND data_path = " + "'" + self.data_path + "'"

                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metadata_sql: " + metadata_sql)
                conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "before connecting to metastore controls")
                controls = dbQuery(cur_metadata, metadata_sql)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metastore controls:", controls)
            except psycopg2.Error as e:
                error = 2
                err_msg = method_name + "[{0}]: Error connecting to control table database".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            if not controls:
                error = 3
                err_msg = method_name + "[{0}]: No Entry found in control table".format(error)
                status = 'Job Error'
                output_msg = "No Entry found in control table"
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            self.id = str(controls[0]['id'])
            self.source_schema = str(controls[0]['source_schemaname'])
            self.source_tablename = str(controls[0]['source_tablename'])
            self.target_schema = str(controls[0]['target_schemaname'])
            self.target_tablename = str(controls[0]['target_tablename'])
            partitioned = controls[0]['is_partitioned']
            self.load_type = str(controls[0]['load_type'])
            self.s3_backed = controls[0]['s3_backed']
            first_partitioned_column = str(controls[0]['first_partitioned_column'])
            second_partitioned_column = str(controls[0]['second_partitioned_column'])
            partitioned_column_transformation = str(controls[0]['partition_column_transformation'])
            custom_sql = str(controls[0]['custom_sql'])
            self.join_columns = str(controls[0]['join_columns'])
            self.archived_enabled = controls[0]['archived_enabled']
            distribution_columns = str(controls[0]['distribution_columns'])
            dist_col_transformation = str(controls[0]['dist_col_transformation'])
            self.log_mode = str(controls[0]['log_mode'])
            last_run_time  = str(controls[0]['last_run_time'])

            if self.s3_backed and self.load_type <> 'APPEND_ONLY':
                error = 4
                err_msg = method_name + "[{0}]: ERROR: S3 target storage for source table ".format(error) + self.source_tablename + " not allowed for load_type " + self.load_type
                status = 'Job Error'
                output_msg = "S3 target storage for source table " + self.source_tablename + " not allowed for load_type " + self.load_type
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            #######################################################################################################
            print (print_hdr + "distribution_columns :: dist_col_transformation :: partitioned", distribution_columns, dist_col_transformation, partitioned)

            # Connection to the Hive Metastore to get column and partition list
            try:
                connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
            except Exception as e:
                error = 5
                err_msg = method_name + "[{0}]: Error connecting to Hive Metastore".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                return output

            # Establish connection to the hive metastore to get the list of columns
            try:
                cursor = connection.cursor()
                sql_query = "SELECT                                                                   \
                                    c.COLUMN_NAME                                                     \
                            FROM                                                                      \
                                TBLS t                                                                \
                                JOIN DBS d                                                            \
                                    ON t.DB_ID = d.DB_ID                                              \
                                JOIN SDS s                                                            \
                                    ON t.SD_ID = s.SD_ID                                              \
                                JOIN COLUMNS_V2 c                                                     \
                                    ON s.CD_ID = c.CD_ID                                              \
                            WHERE                                                                     \
                                TBL_NAME = " + "'" + self.target_tablename + "' " + "                 \
                                AND d.NAME=" + " '" + self.target_schema + "' " + "                   \
                                ORDER by c.INTEGER_IDX"

                # print (print_hdr + "hive_meta_target_sql: " + sql_query)
                cursor.execute(sql_query)
                target_result = cursor.fetchall()
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "target_result:", target_result)

                sql_query = "SELECT                                                                   \
                                    c.COLUMN_NAME                                                     \
                            FROM                                                                      \
                                TBLS t                                                                \
                                JOIN DBS d                                                            \
                                    ON t.DB_ID = d.DB_ID                                              \
                                JOIN SDS s                                                            \
                                    ON t.SD_ID = s.SD_ID                                              \
                                JOIN COLUMNS_V2 c                                                     \
                                    ON s.CD_ID = c.CD_ID                                              \
                            WHERE                                                                     \
                                TBL_NAME = " + "'" + self.source_tablename + "' " + "                 \
                                AND d.NAME=" + " '" + self.source_schema + "' " + "                   \
                                ORDER by c.INTEGER_IDX"

                # print (print_hdr + "hive_meta_source_sql: " + sql_query)
                cursor.execute(sql_query)
                source_result = cursor.fetchall()
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "source_result:", source_result)

                # if self.data_path == 'SRC2Hive':
                #     file_format_sql = "SELECT s.INPUT_FORMAT from TBLS t JOIN SDS s ON t.SD_ID = s.SD_ID JOIN DBS d ON t.DB_ID = d.DB_ID WHERE t.TBL_NAME = '" \
                #                       + self.source_tablename + "' AND d.NAME = '" + self.source_schema + "'"
                #     print (print_hdr + "file_format_sql: " + file_format_sql)
                #     cursor.execute(file_format_sql)
                #     file_format_output = cursor.fetchall()
                #     print (print_hdr + "file_format_output:", file_format_output)
                #     file_format = [a[0] for a in file_format_output]

            except Exception as e:
                error = 6
                err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database:".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                return output
            finally:
                connection.close()

            new_target_result = target_result

            from ast import literal_eval

            if distribution_columns != 'None':
                col_check = literal_eval("('" + distribution_columns + "',)")
                print (print_hdr + "col_check:", col_check)
                if col_check in target_result:
                    target_result.remove(col_check)
                    print (print_hdr + "Distribution Column Available",)
                else:
                    print (print_hdr + "Didn't find distribution column")
            else:
                print (print_hdr + "No distribution column")

            # Get the column on which the table is partitioned
            source_select_list = ', '.join(map(''.join, target_result))
            target_select_list = ', '.join(map(''.join, target_result))

            if not source_select_list:
                error = 7
                err_msg = method_name + "[{0}]: Hive Table Not Found in metadata database".format(error)
                status = 'Job Error'
                # output_msg = traceback.format_exc()
                output_msg = 'Hive Table Not Found in metadata database'
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                return output

            last_updated_by = str('data_sync')

            audit_date_column = "'" + str(datetime.now()) + "' as hive_updated_date "
            audit_updated_by_column = "'" + last_updated_by + "' as hive_updated_by "

            if dist_col_transformation != 'None' and partitioned:
                target_select = target_select_list + ',' + distribution_columns
                # source_select = source_select_list + ' , ' + dist_col_transformation
                source_select = source_select_list + ', ' + dist_col_transformation
            else:
                target_select = target_select_list
                source_select = source_select_list

            print "======================================================"
            print (print_hdr + "printing source_select...")
            print source_select
            print "======================================================"
            print (print_hdr + "partitioned_column_transformation:", partitioned_column_transformation)
            tmp_source_tablename = self.source_tablename
            if self.data_path.find("MIR2") <> -1:
                tmp_source_tablename = self.source_tablename + '_tmp' + str(run_id)
            if self.data_path == 'KFK2Hive':
                counter         = 0
                from datetime import date, timedelta
                if last_run_time == 'None':
                    last_run_time = str(datetime.now())
                print "Last Run Time : ", last_run_time
                lr_dt, lr_ts    = last_run_time.split()
                last_run_date   = datetime.strptime(lr_dt,"%Y-%m-%d")
                lr_dt           = last_run_date.date()
                today           = datetime.now().date()
                delta           = today - lr_dt
                inprogress_path = 'hdfs://getnamenode' + paths + self.source_schema + "/" + self.source_tablename + "/in_progress/"
                paths = inprogress_path

                # if file_format[0].find('Avro') <> -1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "KFK2Hive .... Kafka ..... AVRO")
                for i in range(delta.days + 2):
                    dt          = (lr_dt - timedelta(days=1) + timedelta(days=i))
                    dtstr       = dt.isoformat()
                    print dtstr
            # today = date.today()
            # todaystr = today.isoformat()
                    year, month, day = dtstr.split('-')
                # if file_format[0].find('Avro') <> -1:
                    incoming_path = 'hdfs://getnamenode' + kafka_path + self.source_schema + "_" + self.source_tablename.replace('_kext','') + "/year=" + str(year) + "/month=" + str(month) + "/day=" + str(day) + "/"
                    (ret, out, err) = run_cmd(['hadoop','fs','-mv',(incoming_path + "*"),inprogress_path])
                    if ret == 0:
                        last_success_incoming_path = incoming_path
                    # if err:
                    else:
                        if err.find("No such file or directory") <> -1:
                            counter = counter + 1
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'WARNING: ' + err)
                        else:
                            error = 1
                            err_msg = method_name + "[{0}]: Error while moving data to in_progress location".format(error)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'RUN CMD err: ' + err + '\n' + 'RUN CMD out: ' + out + '\n' + 'RUN CMD ret: ' + str(ret))
                            status = 'Job Error'
                            # output_msg = traceback.format_exc()
                            output_msg = err
                            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                       'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                            self.error_cleanup(input_schema_name, input_table_name, run_id, inprogress_path, conn_metadata, None, None, last_success_incoming_path)
                            return output
                if counter == (delta.days + 2):
                    error = -1
                    err_msg = method_name + "[{0}]: No Data Found".format(error)
                    status = 'Job Finished'
                    output_msg = 'No Data Found'
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    return output

            if self.data_path == 'SRC2Hive':
            #    if file_format[0].find('Text') <> -1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "SRC2Hive .... HVR ..... Text")
                incoming_path = hvr_hdfs_staging_path + self.target_schema + "/*-" + self.target_tablename + ".csv"
                inprogress_path = paths + self.source_schema + "/" + self.source_tablename + "/in_progress/"
                files = glob.glob(incoming_path)
                if len(files) == 0:
                    error = -1
                    err_msg = method_name + "[{0}]: No Data Found".format(error)
                    status = 'Job Finished'
                    output_msg = 'No Data Found'
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                                   'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    return output
                else:
                    local_inprogress_path = hvr_hdfs_staging_path + self.source_schema + "/" + "/in_progress"
                    move_files(incoming_path, local_inprogress_path)
                    (ret, out, err)     = run_cmd(["hadoop", "fs", "-test", "-e", inprogress_path])
                    if ret:
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Directory does not exist ... Creating...")
                        (ret, out, err) = run_cmd(["hadoop", "fs", "-mkdir", "-p", inprogress_path])
                        if ret:
                            error = 1
                            err_msg = method_name + "[{0}]: Error while creating in_progress location in HDFS".format(error)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                            status = 'Job Error'
                            output_msg = traceback.format_exc()
                            output.update(
                                {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                                 'rows_deleted': rows_deleted,
                                 'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                            self.error_cleanup(input_schema_name, input_table_name, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"),
                                               conn_metadata, None, None, hvr_hdfs_staging_path)
                            return output
                    else:
                        (ret, out, err) = run_cmd(["hadoop", "fs", "-rm", "-r", inprogress_path + "*"])
                        if ret:
                            if err.find("No such file or directory") <> -1:
                                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'WARNING: ' + err)
                                pass
                            else:
                                error = 1
                                err_msg = method_name + "[{0}]: Error while cleaning in_progress location".format(error)
                                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + 'RUN CMD err: ' + err + '\n' + 'RUN CMD out: ' + out)
                                status = 'Job Error'
                                output_msg = traceback.format_exc()
                                output.update(
                                    {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                                     'rows_deleted': rows_deleted,
                                     'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                                self.error_cleanup(input_schema_name, input_table_name, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"),
                                                   conn_metadata, None, None, hvr_hdfs_staging_path)
                                return output
                    # files       = glob.glob((local_inprogress_path + "/*-" + self.target_tablename + ".csv"))
                    # for file in files:
                    (ret, out, err) = run_cmd(['hadoop', 'distcp', 'file:///' + (local_inprogress_path + "/*-" + self.target_tablename + ".csv"), 'hdfs:///' + inprogress_path])
                    print ret, out, err
                    if ret > 0:
                        error = 1
                        err_msg = method_name + "[{0}]: Error while moving data to in_progress location".format(error)
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        print output_msg
                        output.update(
                            {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                             'rows_deleted': rows_deleted,
                             'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        self.error_cleanup(input_schema_name, input_table_name, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"),conn_metadata, None, None, hvr_hdfs_staging_path)
                        return output
                connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl,
                                                     database=dbMetastore_dbName)
                try:
                    cursor              = connection.cursor()
                    column_sql          = "SELECT c.COLUMN_NAME, c.TYPE_NAME FROM hive.TBLS t \
                                            JOIN hive.DBS d \
                                            ON t.DB_ID = d.DB_ID \
                                            JOIN hive.SDS s \
                                            ON t.SD_ID = s.SD_ID \
                                            JOIN hive.COLUMNS_V2 c \
                                            ON s.CD_ID = c.CD_ID \
                                            WHERE TBL_NAME = '" + self.target_tablename +"' \
                                            AND d.NAME='" + self.target_schema + "' \
                                            AND c.COLUMN_NAME not like 'hvr_%' \
                                            AND c.COLUMN_NAME not like 'hive_%' \
                                            ORDER by INTEGER_IDX"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "column_sql: " + column_sql)
                    cursor.execute(column_sql)
                    columns_and_dt      = cursor.fetchall()
                    columns_list        = ',`'.join(map('` '.join, columns_and_dt))
                    columns_list        = '`' + columns_list + ",`hvr_2_kafka_date` timestamp, `op_code` int, `time_key` string"
                    print columns_list
                except Exception as e:
                    error = 8
                    err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database to get column details for HVR External table".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                                   'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    self.error_cleanup(self.source_schema, input_table_name, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"), conn_metadata, None,
                                       None, hvr_hdfs_staging_path)
                    return output
                finally:
                    connection.close()

                try:
                    conn_target, cur_target = dbConnectHive(dbtgt_host, dbtgt_Port, dbtgt_Auth)
                except Exception as e:
                    try:
                        conn_target, cur_target = dbConnectHive(dbtgt_host2, dbtgt_Port, dbtgt_Auth)
                    except Exception as e:
                        error = 15
                        num_errors = 1
                        err_msg = method_name + "[{0}]: Error while connecting to target database".format(error)
                        status = 'Job Error'
                        # print (print_hdr + "Exception: ", e)
                        output_msg = traceback.format_exc()
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name,
                                      self.job_name, tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted,
                                      num_errors, err_msg, 0, 0, output_msg)
                        self.error_cleanup(self.source_schema, self.source_tablename, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"),
                                           conn_metadata, None, conn_target,hvr_hdfs_staging_path)
                        return error, err_msg, output

                drop_hive_ext_table     = "DROP TABLE  `" + self.source_schema + "." + self.source_tablename.replace('$', '_') + "`"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "drop_hive_ext_table: " + drop_hive_ext_table)
                create_hive_ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS `" + self.source_schema + "." + self.source_tablename.replace('$', '_') + "`(" + columns_list.replace('$', '_') + ") \
                                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\n'  \
                                    STORED AS TEXTFILE LOCATION '" + paths + self.source_schema + "/" + self.source_tablename.replace('$', '_') + "/in_progress' TBLPROPERTIES('serialization.null.format'='\\\\N')"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "create_hive_ext_table: " + create_hive_ext_table)

                try:
                    cur_target.execute(drop_hive_ext_table)
                    cur_target.execute(create_hive_ext_table)
                except Exception as e:
                    num_errors =1
                    error = 18
                    err_msg = method_name + "[{0}]: Error while creating Hive External table".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err_msg)
                    print output_msg
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,
                                  tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, num_errors, err_msg, 0, 0,
                                  output_msg)
                    self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, (local_inprogress_path + "/*-" + self.target_tablename + ".csv"), conn_metadata,
                                       None, conn_target,hvr_hdfs_staging_path)
                    return error, err_msg, output
            if self.data_path == 'Talend2Hive':
            #    if file_format[0].find('Text') <> -1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Talend2Hive .... TALEND ..... Text")
                incoming_path   = paths + self.target_schema + "/" + self.target_tablename + "_ext_talend/" + self.target_tablename + ".csv"
            elif self.data_path.find('SQOOP2Hive') <> -1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "SQOOP2Hive .... SQOOP ..... Text")
                incoming_path   = paths + self.target_schema + "/" + self.target_tablename + "_ext_talend/" + "part-*"
            elif self.data_path.find('NiFi2Hive') <> -1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "NiFi2Hive .... NiFi ..... Text")
                print paths
                print self.target_schema
                print self.target_tablename
                print self.source_schema
                print self.source_tablename
                incoming_path   = paths + self.target_schema + "/" + self.target_tablename + "_ext_talend/" + self.target_schema + "." + self.target_tablename + "*.txt"


            if self.data_path == 'Talend2Hive' or self.data_path == 'SQOOP2Hive' or self.data_path == 'NiFi2Hive':
                inprogress_path = paths + self.target_schema + "/" + self.target_tablename + "_ext_talend" + "/in_progress/"

                (ret, out, err) = run_cmd(["hadoop", "fs", "-test", "-e", inprogress_path])
                if ret:
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Directory does not exist ... Creating...")
                    (ret, out, err) = run_cmd(["hadoop", "fs", "-mkdir", "-p", inprogress_path])
                    if ret:
                        error = 1
                        err_msg = method_name + "[{0}]: Error while creating in_progress location in HDFS".format(error)
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        output.update(
                            {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                             'rows_deleted': rows_deleted,
                             'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        self.error_cleanup(input_schema_name, input_table_name, run_id,
                                           (local_inprogress_path + "/*-" + self.target_tablename + ".csv"),
                                           conn_metadata, None, None, hvr_hdfs_staging_path)
                        return output
                else:
                    (ret, out, err) = run_cmd(["hadoop", "fs", "-rm", "-r", inprogress_path + "*"])
                    if ret:
                        if err.find("No such file or directory") <> -1:
                            pass
                        else:
                            error = 1
                            err_msg = method_name + "[{0}]: Error while cleaning in_progress location".format(error)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                            status = 'Job Error'
                            output_msg = traceback.format_exc()
                            output.update(
                                {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                                 'rows_deleted': rows_deleted,
                                 'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                            self.error_cleanup(input_schema_name, input_table_name, run_id,
                                               (local_inprogress_path + "/*-" + self.target_tablename + ".csv"),
                                               conn_metadata, None, None, hvr_hdfs_staging_path)
                            return output
                (ret, out, err) = run_cmd(["hadoop", "fs", "-mv", incoming_path, inprogress_path])
                if ret:
                    error = 1
                    err_msg = method_name + "[{0}]: Error while moving data to in_progress location in HDFS".format(error)
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update(
                        {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                         'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    self.error_cleanup(input_schema_name, input_table_name, run_id,
                                       (local_inprogress_path + "/*-" + self.target_tablename + ".csv"),
                                       conn_metadata, None, None, hvr_hdfs_staging_path)
                    return output
                connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl,database=dbMetastore_dbName)
                try:
                    cursor              = connection.cursor()
                    column_sql          = "SELECT c.COLUMN_NAME, c.TYPE_NAME FROM hive.TBLS t \
                                            JOIN hive.DBS d \
                                            ON t.DB_ID = d.DB_ID \
                                            JOIN hive.SDS s \
                                            ON t.SD_ID = s.SD_ID \
                                            JOIN hive.COLUMNS_V2 c \
                                            ON s.CD_ID = c.CD_ID \
                                            WHERE TBL_NAME = '" + self.target_tablename +"' \
                                            AND d.NAME='" + self.target_schema + "' \
                                            AND c.COLUMN_NAME not like 'edl_%' \
                                            AND c.COLUMN_NAME not like 'hive_%' \
                                            ORDER by INTEGER_IDX"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "column_sql: " + column_sql)
                    cursor.execute(column_sql)
                    columns_and_dt      = cursor.fetchall()
                    columns_list        = ',`'.join(map('` '.join, columns_and_dt))
                    columns_list        = '`' + columns_list
                    print columns_list
                except Exception as e:
                    error = 8
                    err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database to get column details for HVR External table".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                                   'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    self.error_cleanup(self.source_schema, input_table_name, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"), conn_metadata, None,
                                       None, hvr_hdfs_staging_path)
                    return output
                finally:
                    connection.close()

                try:
                    conn_target, cur_target = dbConnectHive(dbtgt_host, dbtgt_Port, dbtgt_Auth)
                except Exception as e:
                    try:
                        conn_target, cur_target = dbConnectHive(dbtgt_host2, dbtgt_Port, dbtgt_Auth)
                    except Exception as e:
                        error = 15
                        num_errors = 1
                        err_msg = method_name + "[{0}]: Error while connecting to target database".format(error)
                        status = 'Job Error'
                        # print (print_hdr + "Exception: ", e)
                        output_msg = traceback.format_exc()
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name,
                                      self.job_name, tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted,
                                      num_errors, err_msg, 0, 0, output_msg)
                        self.error_cleanup(self.source_schema, self.source_tablename, run_id, (local_inprogress_path+ "/*-" + self.target_tablename + ".csv"),
                                           conn_metadata, None, conn_target,hvr_hdfs_staging_path)
                        return error, err_msg, output

                drop_hive_ext_table     = "DROP TABLE  `" + self.target_schema  + "." + self.target_tablename.replace('$', '_') + "_ext_talend`"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "drop_hive_ext_table: " + drop_hive_ext_table)
                if self.data_path == 'NiFi2Hive':
                    create_hive_ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS `" + self.target_schema + "." + self.target_tablename.replace(
                    '$', '_') + "_ext_talend`(" + columns_list.replace('$', '_') + ") \
                                                        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\n'  \
                                                        STORED AS TEXTFILE LOCATION '" + paths + self.target_schema + "/" + self.target_tablename.replace(
                    '$', '_') + "_ext_talend/in_progress' TBLPROPERTIES('serialization.null.format'='')"
                else:
                    create_hive_ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS `" + self.target_schema + "." + self.target_tablename.replace(
                    '$', '_') + "_ext_talend`(" + columns_list.replace('$', '_') + ") \
                                                        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\n'  \
                                                        STORED AS TEXTFILE LOCATION '" + paths + self.target_schema + "/" + self.target_tablename.replace(
                    '$', '_') + "_ext_talend/in_progress' TBLPROPERTIES('serialization.null.format'='\\\\N')"
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "create_hive_ext_table: " + create_hive_ext_table)

                try:
                    cur_target.execute(drop_hive_ext_table)
                    cur_target.execute(create_hive_ext_table)
                except Exception as e:
                    num_errors =1
                    error = 18
                    err_msg = method_name + "[{0}]: Error while creating Hive External table".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err_msg)
                    print output_msg
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,
                                  tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted, num_errors, err_msg, 0, 0,
                                  output_msg)
                    self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, (local_inprogress_path + "/*-" + self.target_tablename + ".csv"), conn_metadata,
                                       None, conn_target,hvr_hdfs_staging_path)
                    return error, err_msg, output

            if (partitioned):
                incremental_sql_query = 'select ' + source_select + ', ' + partitioned_column_transformation + ' from ' + self.source_schema + '.' + tmp_source_tablename
                if second_partitioned_column <> 'None':
                    target_sql_query = 'select ' + target_select + ', ' + first_partitioned_column + ', ' + second_partitioned_column + ' from ' + self.target_schema + '.' + self.target_tablename
                else:
                    target_sql_query = 'select ' + target_select + ', ' + first_partitioned_column + ' from ' + self.target_schema + '.' + self.target_tablename
            else:
                incremental_sql_query = 'select ' + source_select + ' from ' + self.source_schema + '.' + tmp_source_tablename
                target_sql_query = 'select ' + target_select + ' from ' + self.target_schema + '.' + self.target_tablename

            connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
            try:
                cursor = connection.cursor()
                bloom_sql_query = "SELECT e.PARAM_VALUE                                                 \
                                   FROM                                                                 \
                                        TBLS t                                                          \
                                   JOIN DBS d                                                           \
                                      ON t.DB_ID = d.DB_ID                                              \
                                   LEFT OUTER JOIN TABLE_PARAMS e                                       \
                                        ON t.TBL_ID = e.TBL_ID                                          \
                                        AND PARAM_KEY = 'orc.bloom.filter.columns'                      \
                                   WHERE                                                                \
                                        TBL_NAME = " + "'" + self.target_tablename + "' " + "           \
                                        AND d.NAME=" + " '" + self.target_schema + "' "                 \

                cursor.execute(bloom_sql_query)
                bloom_filter = cursor.fetchall()
            except Exception as e:
                error = 8
                err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database to get bloom filter details".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                               'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata, None, None, last_success_incoming_path)
                return output
            finally:
                connection.close()

            bloom_filters_columns = ''
            if len(bloom_filter) > 1:
                bloom_filters_columns = ','.join(map(''.join, bloom_filter))
                bloom_filter_list = bloom_filters_columns.split(",")
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "bloom_filter_list: ", bloom_filter_list)

            # Execute the query to get the data into Spark Memory

            # Figure out if it is incremental or full load process
            # If Full Load then truncate the target table and insert the entire incoming data
            # If Incremental Load then determine if the table is partitioned as the logic needs to be handled differently for partitioned and non-partitioned tables
            #      For Non Partitioned Tables merge the incoming data with the table date and save it to the database
            #      For Partitioned Tables identify the partitions for which there is incremental data and intelligently merge the data and save it to the database table
            table_name = self.target_schema + '.' + self.target_tablename
            sqlContext.setConf("hive.exec.dynamic.partition", "true")
            sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
            sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
            sqlContext.setConf("mapred.input.dir.recursive", "true")
            sqlContext.setConf("hive.mapred.supports.subdirectories", "true")

            sc._jsc.hadoopConfiguration().set('fs.s3a.attempts.maximum', '30')

            # Disabled till assertion issue resolved
            # sqlContext.setConf("spark.sql.orc.enabled", "true")
            # sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "true")
            # sqlContext.setConf("spark.sql.orc.char.enabled", "true")

            target_tmp_table = self.target_tablename + '_tmp' + str(run_id)
            target_tmp_path = self.config_list['misc_hdfsStagingPath'] + self.target_schema + '/' + target_tmp_table + '/'

            # print (print_hdr + "target_path: " + target_path)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "target_tmp_path: " + target_tmp_path)

            (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (target_tmp_path + "*")])
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "removing files from target tmp - STATUS: " + str(ret_rm))

            if second_partitioned_column <> 'None':
                partitioned_columns = first_partitioned_column + "," + second_partitioned_column
            else:
                partitioned_columns = first_partitioned_column

            if distribution_columns != 'None' and partitioned_columns != 'None':
                bucket_columns = partitioned_columns + "," + distribution_columns
                bucket_column_list = bucket_columns.split(",")
            elif partitioned_columns != 'None':
                bucket_columns = partitioned_columns
                bucket_column_list = bucket_columns.split(",")
            else:
                bucket_columns = distribution_columns
                bucket_column_list = bucket_columns.split(",")

            if partitioned_columns != 'None':
                partition_column_list = partitioned_columns.split(",")
                print (print_hdr + "Inside Partition Split: ", partition_column_list)

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "incremental_sql_query: " + incremental_sql_query)
            print "=================================================="
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "target_sql_query: " + target_sql_query)
            print "=================================================="

            from pyspark.sql.functions import col

            from pyspark import HiveContext
            from pyspark.sql.types import *
            from pyspark.sql import Row, functions as F
            from pyspark.sql.window import Window
            from pyspark.sql.functions import desc, lit, current_timestamp

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "load_type: " + self.load_type)
            join_column_list = map(lambda s: s.strip(), self.join_columns.split(","))

            if self.data_path == 'SRC2Hive' or self.data_path == 'KFK2Hive':
                print "INSIDE SRC2Hive/KFK2Hive...................................."
                target_select_hvr_kafka             = ','.join(map(''.join, target_result))
                target_select_hvr_kafka_list        = target_select_hvr_kafka.split(",")
                target_select_list_kafka            = [x for x in target_select_hvr_kafka_list if x not in ['hvr_last_upd_tms','hvr_is_deleted','hive_updated_by', 'hive_updated_date']] + ['hvr_2_kafka_date','time_key','op_code']
                target_select_kafka                 = '`,`'.join(target_select_list_kafka)
                # if partitioned:
                #     if distribution_columns != 'None':
                #         final_target_select_kafka = '`' + target_select_kafka + '`,' + dist_col_transformation + ',' + partitioned_column_transformation
                #     else:
                #         final_target_select_kafka = '`' + target_select_kafka + '`,' + partitioned_column_transformation
                # else:
                final_target_select_kafka           = '`' + target_select_kafka + '`'
                print final_target_select_kafka
                input_tbl_df                        = sqlContext.sql("SELECT " + final_target_select_kafka + " FROM " + self.source_schema + "." + self.source_tablename)
                # print "Input Table DF Count :", input_tbl_df.count()
                rownum_df                           = input_tbl_df.select("*",F.row_number().over(Window.partitionBy(join_column_list).orderBy(desc("time_key"))).alias("rn")).where(input_tbl_df.op_code <> 5)
                # print "rownum_df DF Count :", rownum_df.count()
                if self.target_schema == "proficy" and self.target_tablename == 'local_qualitysteps':
                # if len(join_column_list) > 1:
                    intermediate_df                     = rownum_df.select("*").withColumnRenamed("hvr_2_kafka_date","hvr_last_upd_tms").   \
                                                            withColumn("hvr_is_deleted",F.when(rownum_df.op_code ==0, 1).otherwise(0)).     \
                                                            withColumn("hive_updated_by",lit("datasync")).                                  \
                                                            withColumn("hive_updated_date",lit(current_timestamp()))
                else:
                    intermediate_df                     = rownum_df.select("*").withColumnRenamed("hvr_2_kafka_date","hvr_last_upd_tms").   \
                                                            withColumn("hvr_is_deleted",F.when(rownum_df.op_code ==0, 1).otherwise(0)).     \
                                                            withColumn("hive_updated_by",lit("datasync")).                                  \
                                                            withColumn("hive_updated_date",lit(current_timestamp())).                       \
                                                            where(rownum_df.rn == 1)
                # if partitioned:
                #     if distribution_columns != 'None':
                #         final_target_select_kafka = '`' + target_select_kafka + '`,' + dist_col_transformation + ',' + partitioned_column_transformation
                #     else:
                #         final_target_select_kafka = '`' + target_select_kafka + '`,' + partitioned_column_transformation
                # else:
                # print "intermediate_df DF Count :", intermediate_df.count()
                cols_to_be_removed_list             = ['time_key','op_code','hvr_2_kafka_date']
                cols_to_be_added_list               = ['hvr_is_deleted','hvr_last_upd_tms','hive_updated_by','hive_updated_date']
                target_select_hvr_kafka_list_tmp    = [x for x in target_select_list_kafka if x not in cols_to_be_removed_list]
                if partitioned:
                    intermediate_df.createOrReplaceTempView("intermediate_tbl")
                    if distribution_columns != 'None':
                        # dist_part_cols_list              = ','.join(target_select_hvr_kafka_list_tmp) + ',' + dist_col_transformation + ',' + partitioned_column_transformation
                        # print dist_part_cols_list
                        dist_part_df                    = sqlContext.sql("SELECT *, " + dist_col_transformation + "," + partitioned_column_transformation + " FROM intermediate_tbl" )
                        dist_part_df.printSchema()
                        final_target_select_hvr_kafka = target_select_hvr_kafka_list_tmp + cols_to_be_added_list + distribution_columns.split(',') + first_partitioned_column.split(',')
                    else:
                        dist_part_cols_list = ','.join(target_select_hvr_kafka_list_tmp) + ',' + partitioned_column_transformation
                        dist_part_df = sqlContext.sql("SELECT *, " + partitioned_column_transformation + " FROM intermediate_tbl")
                        final_target_select_hvr_kafka = target_select_hvr_kafka_list_tmp + cols_to_be_added_list + first_partitioned_column.split(',')
                    filter_df                       = dist_part_df.select(final_target_select_hvr_kafka)
                else:
                    final_target_select_hvr_kafka       = target_select_hvr_kafka_list_tmp + cols_to_be_added_list
                    # print final_target_select_hvr_kafka
                    filter_df                           = intermediate_df.select(final_target_select_hvr_kafka)
                # print "filter_df DF Count :", filter_df.count()
            elif self.data_path == 'Talend2Hive' or self.data_path == 'SQOOP2Hive' or self.data_path == 'NiFi2Hive':
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "INSIDE Talend2Hive/SQOOP2Hive/NiFi2Hive ...................................")
                target_select_talend                = ','.join(map(''.join, target_result))
                target_select_talend_list           = target_select_talend.split(",")
                target_select_list_talend           = [x for x in target_select_talend_list if x not in ['edl_is_deleted', 'edl_last_updated_date', 'hive_updated_by', 'hive_updated_date']]
                target_select_list_talend_tmp       = '`,`'.join(target_select_list_talend)
                final_target_select_list_talend     = '`' + target_select_list_talend_tmp + '`'
                # print final_target_select_list_talend
                if partitioned:
                    if distribution_columns != 'None':
                        select_list_dist_col_part_col_trans = final_target_select_list_talend + "," + dist_col_transformation + "," + partitioned_column_transformation
                        print select_list_dist_col_part_col_trans
                    else:
                        select_list_dist_col_part_col_trans = final_target_select_list_talend + "," + partitioned_column_transformation
                        # print select_list_dist_col_part_col_trans
                    input_tbl_df                            = sqlContext.sql("SELECT " + select_list_dist_col_part_col_trans + " FROM " + self.target_schema + "." + self.target_tablename + "_ext_talend")
                else:
                    input_tbl_df                        = sqlContext.sql("SELECT " + final_target_select_list_talend + " FROM " + self.target_schema + "." + self.target_tablename + "_ext_talend")
                # print "Count of input_tbl_df :", input_tbl_df.count()
                if partitioned:
                    if distribution_columns != 'None':
                        intermediate_df                         = input_tbl_df.select("*").withColumn("edl_is_deleted", lit("0")).                          \
                                                                                          withColumn("edl_last_updated_date", lit(current_timestamp())).  \
                                                                                          withColumn("hive_updated_by", lit("datasync")).                 \
                                                                                        withColumn("hive_updated_date", lit(current_timestamp()))
                        select_list_dist_col_part_col           = final_target_select_list_talend.split(',') + ["edl_is_deleted","edl_last_updated_date","hive_updated_by","hive_updated_date"] + distribution_columns.split(',') + first_partitioned_column.split(',')
                        print select_list_dist_col_part_col
                        filter_df                               = intermediate_df.select(select_list_dist_col_part_col)
                    else:
                        intermediate_df                   = input_tbl_df.select("*").withColumn("edl_is_deleted", lit("0")). \
                                                                                withColumn("edl_last_updated_date", lit(current_timestamp())). \
                                                                                withColumn("hive_updated_by", lit("datasync")). \
                                                                                withColumn("hive_updated_date", lit(current_timestamp()))
                        select_list_dist_col_part_col           = final_target_select_list_talend.split(',') + ["edl_is_deleted","edl_last_updated_date","hive_updated_by","hive_updated_date"] + first_partitioned_column.split(',')
                        filter_df = intermediate_df.select(select_list_dist_col_part_col)

                else:
                    filter_df                           = input_tbl_df.select("*").withColumn("edl_is_deleted",lit("0")).                           \
                                                                                    withColumn("edl_last_updated_date",lit(current_timestamp())).   \
                                                                                    withColumn("hive_updated_by", lit("datasync")).                 \
                                                                                    withColumn("hive_updated_date", lit(current_timestamp()))
                # print "Count of filter_df :", filter_df.count()
                # print filter_df.printSchema()
                # output.update({'status': "Job Finished", 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                #                'rows_deleted': rows_deleted,
                #                'error': error, 'err_msg': err_msg, 'output_msg': "I AM DONE FOR NOW"})
                # return output
            else:
                filter_df = sqlContext.sql(incremental_sql_query)

            if self.load_type == 'FULL':
                try:
                    # merge_df = sqlContext.sql(incremental_sql_query)
                    merge_df = filter_df
                    rows_inserted = merge_df.count()
                    if partitioned:
                        # merge_df.cache()
                        if bloom_filters_columns:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),
                                                            bucket_column_list) \
                                .sortWithinPartitions(bloom_filter_list)
                        else:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),
                                                            bucket_column_list)

                        final_df.write.option("compression", "zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(target_tmp_path)
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "FULL: Partitioned: rows_inserted: " + str(rows_inserted))

                    else:
                        if merge_df.rdd.getNumPartitions() > 300:
                            merge_coalesce_df = merge_df.coalesce(300)
                        else:
                            merge_coalesce_df = merge_df

                        if bloom_filters_columns:
                            save_df = merge_coalesce_df.sortWithinPartitions(bloom_filter_list)
                            save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "FULL: Non-Partitioned: Bloom Filter: rows_inserted: " + str(rows_inserted))
                        else:
                            merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "FULL: Non-Partitioned: Non-Bloom Filter: rows_inserted: " + str(rows_inserted))

                    # self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)

                except Exception as e:
                    error = 9
                    err_msg = method_name + "[{0}]: Error while doing a full load".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    if self.data_path == 'SRC2Hive':
                        self.error_cleanup(self.source_schema, input_table_name, run_id,
                                           (local_inprogress_path + "/*-" + self.target_tablename + ".csv"),
                                           conn_metadata, None,
                                           None, hvr_hdfs_staging_path)
                    else:
                        self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata, None, None, last_success_incoming_path)
                    return output
            # Incremental Logic for Append Only table especially for S3
            elif self.load_type == 'APPEND_ONLY':
                print "Inside APPEND_ONLY Logic.............."
                try:
                    # merge_df = sqlContext.sql(incremental_sql_query)
                    merge_df = filter_df
                    rows_inserted = merge_df.count()
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "APPEND_ONLY: Incremental records: " + str(rows_inserted))
                    if partitioned:
                        if bloom_filters_columns:
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()),bucket_column_list) \
                                                .sortWithinPartitions(bloom_filter_list)
                        else:
                            print "Inside Non Bloom Filter and Partitioned"
                            final_df = merge_df.repartition(len(merge_df.select(bucket_column_list).distinct().collect()), bucket_column_list)

                        final_df.write.option("compression", "zlib").mode("overwrite").format("orc").partitionBy(partition_column_list).save(target_tmp_path)
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "APPEND_ONLY: Partitioned: rows_inserted: " + str(rows_inserted))

                    else:
                        if merge_df.rdd.getNumPartitions() > 300:
                            merge_coalesce_df = merge_df.coalesce(300)
                        else:
                            merge_coalesce_df = merge_df

                        if bloom_filters_columns:
                            save_df = merge_coalesce_df.sortWithinPartitions(bloom_filter_list)
                            save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "APPEND_ONLY: Non-Partitioned: Bloom Filter: rows_inserted: " + str(rows_inserted))
                        else:
                            merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "APPEND_ONLY: Non-Partitioned: Non-Bloom Filter: rows_inserted: " + str(rows_inserted))

                except Exception as e:
                    error = 10
                    err_msg = method_name + "[{0}]: Error while loading data in temporary hdfs location for append only load".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    if self.data_path == 'SRC2Hive':
                        self.error_cleanup(self.source_schema, input_table_name, run_id,
                                           (local_inprogress_path + "/*-" + self.target_tablename + ".csv"),
                                           conn_metadata, None,
                                           None, hvr_hdfs_staging_path)
                    else:
                        self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata, None, None, last_success_incoming_path)
                    return output

                # self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)

            else:
                conn_hive = None
                cur_hive = None
                table_lock_sql = "LOCK TABLE `" + self.target_schema + "." + self.target_tablename + "` EXCLUSIVE"
                table_unlock_sql = "UNLOCK TABLE `" + self.target_schema + "." + self.target_tablename + "`"

                if (partitioned):
                    try:
                        # incremental_df = sqlContext.sql(incremental_sql_query)
                        incremental_df = filter_df
                        rows_inserted = incremental_df.count()
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "INCREMENTAL: Partitioned: Incremental records: " + str(rows_inserted))

                        first_partitioned_list = incremental_df.select(first_partitioned_column) \
                            .rdd.flatMap(lambda x: x).distinct().collect()

                        # print (print_hdr + "first_partitioned_list: ", first_partitioned_list)
                        # print second_partitioned_list

                        if second_partitioned_column <> 'None':
                            second_partitioned_list = incremental_df.select(second_partitioned_column) \
                                .rdd.flatMap(lambda x: x).distinct().collect()
                            merge_df = sqlContext.sql(target_sql_query) \
                                .where(col(first_partitioned_column).isin(first_partitioned_list) & \
                                       col(second_partitioned_column).isin(second_partitioned_list))
                        else:
                             merge_df = sqlContext.sql(target_sql_query) \
                                .where(col(first_partitioned_column).isin(first_partitioned_list))

                        # print (print_hdr + "merge_df.count: " + str(merge_df.count()))
                        # join_column_list = self.join_columns.split(",")
                        # join_column_list = map(lambda s: s.strip(), self.join_columns.split(","))
                        output_df = merge_df.join(incremental_df, join_column_list, "leftanti")
                        # print (print_hdr + "output_df.count: " + str(output_df.count()))
                        # print "Output DF :", output_df.printSchema()
                        # print "Incremental DF :", incremental_df.printSchema()
                        final_df = output_df.union(incremental_df)
                        # print (print_hdr + "final_df.count: " + str(final_df.count()))

                        sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

                        if bloom_filters_columns:
                            save_df = final_df.repartition(len(final_df.select(bucket_column_list).distinct().collect()),
                                                           bucket_column_list) \
                                .sortWithinPartitions(bloom_filter_list)
                        else:
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Inside Non Bloom Filter...")
                            save_df = final_df.repartition(len(final_df.select(bucket_column_list).distinct().collect()),
                                                           bucket_column_list)

                        try:
                            conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                        except Exception as e:
                            try:
                                conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                            except Exception as e:
                                raise

                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "table_lock_sql: " + table_lock_sql)
                        cur_hive.execute(table_lock_sql)

                        if second_partitioned_column <> 'None':
                            temp_table = self.target_tablename + '_stg'
                            save_df.createOrReplaceTempView(temp_table)

                            # final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + '_tmp' + \
                            #             ' PARTITION ( ' + first_partitioned_column + ',' + second_partitioned_column + ' ) \
                            #             SELECT * FROM ' + temp_table

                            # final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + target_tmp_table + \
                            final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + \
                                        ' PARTITION ( ' + first_partitioned_column + ',' + second_partitioned_column + ' ) \
                                        SELECT * FROM ' + temp_table

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "final_sql: " + final_sql)
                            sqlContext.sql(final_sql)

                        else:
                            temp_table = self.target_tablename + '_stg'
                            save_df.createOrReplaceTempView(temp_table)

                            sqlContext.setConf("hive.hadoop.supports.splittable.combineinputformat", "true")
                            sqlContext.setConf("tez.grouping.min-size", "1073741824")
                            sqlContext.setConf("tez.grouping.max-size", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.minsize", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.maxsize", "2147483648")
                            sqlContext.setConf("hive.merge.smallfiles.avgsize", "2147483648")
                            sqlContext.setConf("hive.exec.reducers.bytes.per.reducer", "2147483648")

                            # final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + '_tmp' + \
                            #             ' PARTITION ( ' + first_partitioned_column + ' ) \
                            #             SELECT * FROM ' + temp_table

                            # final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + target_tmp_table + \
                            final_sql = 'INSERT OVERWRITE TABLE ' + self.target_schema + '.' + self.target_tablename + \
                                        ' PARTITION ( ' + first_partitioned_column + ' ) \
                                        SELECT * FROM ' + temp_table

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "final_sql: " + final_sql)
                            sqlContext.sql(final_sql)

                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "INCREMENTAL: Partitioned: rows_inserted: " + str(rows_inserted))

                    except Exception as e:
                        error = 13
                        err_msg = method_name + "[{0}]: Error while performing incremental update".format(error)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                       'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        if self.data_path =='SRC2Hive':
                            self.error_cleanup(self.source_schema, input_table_name, run_id, (local_inprogress_path + "/*-" + self.target_tablename + ".csv"), conn_metadata, None, None, hvr_hdfs_staging_path)
                        else:
                            self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata, None, None, last_success_incoming_path)
                        return output
                    finally:
                        try:
                            if cur_hive is not None:
                                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "table_unlock_sql: " + table_unlock_sql)
                                cur_hive.execute(table_unlock_sql)
                        except: pass
                        finally:
                            try:
                                if conn_hive is not None:
                                    conn_hive.close()
                            except: pass
                # Incremental Update of non-partitioned table
                else:
                    try:
                        # incremental_df = sqlContext.sql(incremental_sql_query)
                        incremental_df = filter_df
                        rows_inserted = incremental_df.count()
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "INCREMENTAL: Non-Partitioned: Incremental records: " + str(rows_inserted))

                        current_df = sqlContext.sql(target_sql_query)
                        # join_column_list = self.join_columns.split(",")
                        join_column_list = map(lambda s: s.strip(), self.join_columns.split(","))

                        # Logic to handle null comparison in incremental join
                        # output_df = current_df.join(incremental_df, join_column_list, "leftanti")
                        output_df = None
                        if len(join_column_list) > 1:
                            current_df.createOrReplaceTempView("current_table")
                            incremental_df.createOrReplaceTempView("incremental_table")
                            leftanti_sql = "select cur.* from current_table cur left anti join incremental_table inc on "
                            for idx, column in enumerate(join_column_list):
                                if idx == 0:
                                    leftanti_sql = leftanti_sql + "nvl(inc." + column + ",'-9999999999') = nvl(cur." + column + ",'-9999999999') "
                                else:
                                    leftanti_sql = leftanti_sql + "and nvl(inc." + column + ",'-9999999999') = nvl(cur." + column + ",'-9999999999') "

                            print leftanti_sql
                            output_df = sqlContext.sql(leftanti_sql)
                        else:
                            output_df = current_df.join(incremental_df, join_column_list, "leftanti")

                        output_df_reorder = output_df.select(list(source_select.split(", ")))
                        merge_df = output_df_reorder.union(incremental_df)
                        merge_cnt = merge_df.count()

                        # print path
                        # print (print_hdr + self.load_type + ": Non-partitioned: NumPartitions: ", merge_df.rdd.getNumPartitions())
                        if merge_df.rdd.getNumPartitions() > 300:
                            merge_coalesce_df = merge_df.coalesce(300)
                        else:
                            merge_coalesce_df = merge_df
                        if bloom_filters_columns:
                            save_df = merge_coalesce_df.sortWithinPartitions(bloom_filter_list)
                            save_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "INCREMENTAL: Non-partitioned: Bloom Filter: merge_cnt: " + str(merge_cnt))

                        else:
                            # print path
                            merge_coalesce_df.write.option("compression", "zlib").mode("overwrite").format("orc").save(target_tmp_path)

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "INCREMENTAL: Non-partitioned: Non-Bloom Filter: merge_cnt: " + str(merge_cnt))

                        # self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_INPROGRESS, run_id)

                    except Exception as e:
                        error = 14
                        err_msg = method_name + "[{0}]: Error while doing incremental update".format(error)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                                       'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                        if self.data_path == 'SRC2Hive':
                            self.error_cleanup(self.source_schema, input_table_name, run_id,
                                               (local_inprogress_path + "/*-" + self.target_tablename + ".csv"),
                                               conn_metadata, None,
                                               None, hvr_hdfs_staging_path)
                        else:
                            self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata, None, None, last_success_incoming_path)
                        return output

            if partitioned:
                repair_table_sql = 'MSCK REPAIR TABLE ' + self.target_schema + '.' + target_tmp_table
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "repair_table_sql: " + repair_table_sql)
                sqlContext.sql(repair_table_sql)

            # Move Files to HDFS Staging location
            if self.data_path == 'SRC2Hive':

                try:
                    from datetime import date
                    today = date.today()
                    todaystr = today.isoformat()
                    path = hvr_hdfs_staging_path + self.target_schema + "/in_progress/*-" + self.target_tablename + ".csv"
                    hvr_hdfs_backup_path_tmp = hvr_hdfs_backup_path + self.target_schema + "/" + todaystr + "/" + self.target_tablename + "/"
                    move_files(path, hvr_hdfs_backup_path_tmp)
                except Exception as e:
                    error = 13
                    err_msg = "Error while moving files to HDFS directory"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg)
                    status = "Job Error"
                    path_from = paths + self.source_schema + "/" + self.source_tablename + "/in_progress/"
                    path_to = paths + self.source_schema
                    move_files(path_from, path_to)
                    output_msg = traceback.format_exc()
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr, output_msg)
                    output.update(
                        {'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,
                         'rows_deleted': rows_deleted,
                         'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    self.error_cleanup(input_schema_name, input_table_name, run_id,
                                       (local_inprogress_path + "/*-" + self.target_tablename + ".csv"), conn_metadata,
                                       None, None, hvr_hdfs_staging_path)
                    return output
            error = 0
            err_msg = method_name + "[{0}]: No Errors".format(error)
            status = 'Job Finished'
            output_msg = 'Job Finished successfully'
            # remove_files(paths, self.source_schema, input_table_name)
            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                           'error': error, 'err_msg': err_msg, 'output_msg': output_msg})

        except Exception as e:
            error = 17
            err_msg = method_name + "[{0}]: Error while loading data".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                           'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
            if self.data_path == 'SRC2Hive':
                self.error_cleanup(self.source_schema, input_table_name, run_id,
                                   (local_inprogress_path + "/*-" + self.target_tablename + ".csv"), conn_metadata,
                                   None,
                                   None, hvr_hdfs_staging_path)
            else:
                self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata, None, None, last_success_incoming_path)

        if len(output) == 0:
            output = {'error': error, 'err_msg': err_msg}
        return output


    def load_file2gp_hvr(self):
        print "Inside file2GP_HVR"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'GPDB'
        job_name            = 'SRC-->GPDB'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0

        # proceed to point everything at the 'branched' resources
        metastore_dbName            = self.config_list['meta_db_dbName']
        dbmeta_Url                  = self.config_list['meta_db_dbUrl']
        dbmeta_User                 = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbtgt_Url                   = self.config_list['tgt_db_dbUrl']
        dbtgt_User                  = self.config_list['tgt_db_dbUser']

        dbtgt_Pwd                   = base64.b64decode(self.config_list['tgt_db_dbPwd'])
        dbtgt_dbName                = self.config_list['tgt_db_dbName']

        data_paths                  = self.config_list['misc_dataPath']
        hdfs_data_path              = self.config_list['misc_hdfsPath']

        env                         = self.config_list['env']

        t                           = datetime.fromtimestamp(time.time())
        v_timestamp                 = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print "Run ID for the table", tablename , " is : ", run_id
        except Exception as e:
            err_msg = "Error connecting to database while fetching  metadata"
            error   = 1
            print e
            return

        #Audit entry at start of job
        err_msg       = ''
        err_msg       = ''
        status        = 'Job Started'
        output_msg    = ''
        rows_inserted = 0
        rows_deleted  = 0
        rows_updated  = 0
        num_errors    = 0
        audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)


        # Check if HVR has dumped files. If not we will exit from here. No need to continue the process

        path               = data_paths + self.source_schemaname + "/" + "*-" + self.source_tablename + ".csv"
        files              = glob.glob(path)
        print files
        if len(files) == 0:
            error           = 0
            err_msg         = "No data dumped from source @:" + data_paths + self.source_schemaname + "/"
            status          = 'Job Finished'
            output_msg      = "No data dumped from source @:" +  data_paths + self.source_schemaname + "/"
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status,self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished No Data"
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
            in_progress_path = data_paths + self.source_schemaname + "/in_progress"
            move_files(path,in_progress_path)

        # Get target database access

        try:
            conn_target, cur_target      = txn_dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
        except psycopg2.Error as e:
            error   = 2
            err_msg = "Error connecting to Target database"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Check for the existence of External table in target. If present move on to further checks regarding this

        try:
            check_table_sql = "SELECT n.nspname||'.'||c.relname as ext_table_name FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = '" + self.source_schemaname + "' AND c.relname = '" + self.source_tablename + "_ext_hvr' AND c.relstorage = 'x' "
            found_table     = dbQuery(cur_target, check_table_sql)
        except psycopg2.Error as e:
            error   = 3
            err_msg = "Error while checking for External table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # If External table is found in target database, compare the DDL of the Heap and the External
        try:
            if len(found_table) == 1:
                column_list_sql_ext = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = '" \
                                      + self.source_schemaname + "." + self.source_tablename + "_ext_hvr'" + "::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
                columns_ext         = dbQuery(cur_target, column_list_sql_ext)
        except psycopg2.Error as e:
            error   = 4
            err_msg = "Error while getting column list for External table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Get columns for Heap Table
        try:
            column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                              + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass" \
                                                                                                   " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            columns         = dbQuery(cur_target, column_list_sql)
        except psycopg2.Error as e:
            error   = 5
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            error   = 6
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Prepare column list for Heap Table
        try:

            len_cols    = len(columns) - 2
            columns     = columns[:len_cols]
            select_list = '","'.join(d['attname'] for d in columns)
            select_list = '"' + select_list + '"'
            target_create_list = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
            target_create_list = '"' + target_create_list + ", op_code integer, time_key varchar(100)"

            insert_list        = ',a.'.join(d['attname'] for d in columns)
            insert_list        = 'a.' + insert_list
        except Exception as e:
            error   = 7
            print e
            err_msg = "Error while preparing column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Prepare column list for External Table
        if len(found_table) == 1:
            try:
                len_cols_ext    = len(columns_ext) - 2
                columns_ext     = columns_ext[:len_cols_ext]
                select_list_ext = '","'.join(d['attname'] for d in columns_ext)
                select_list_ext = '"' + select_list_ext + '"'
            except Exception as e:
                error   = 8
                print e
                err_msg = "Error while preparing column list for External table"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
            select_list_ext = []


        if env == 'dev':
            if self.source_schemaname == 'eservice':
                port_number = 8998
                gpfdistHost = "10.228.10.46"
            elif self.source_schemaname == 'rmd':
                port_number = 8999
                gpfdistHost = "10.228.5.150"
            elif self.source_schemaname == 'proficy':
                port_number = 8997
                gpfdistHost = "10.228.5.150"
            elif self.source_schemaname == 'proficy_grr':
                port_number = 8996
                gpfdistHost = "10.228.10.46"
            elif self.source_schemaname == 'odw':
                port_number = 8995
                gpfdistHost = "10.228.10.46"
        else:
            if self.source_schemaname == 'eservice':
                port_number = 8998
                gpfdistHost = "10.230.4.64"
            elif self.source_schemaname == 'rmd':
                port_number = 8999
                gpfdistHost = "10.230.11.201"
            elif self.source_schemaname == 'proficy':
                port_number = 8997
                gpfdistHost = "10.230.11.201"
            elif self.source_schemaname == 'proficy_grr':
                port_number = 8996
                gpfdistHost = "10.230.4.64"
            elif self.source_schemaname == 'odw':
                port_number = 8995
                gpfdistHost = "10.230.4.64"

        if select_list <> select_list_ext:

            try:
                drop_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.target_schemaname + "." + self.target_tablename + "_ext_hvr"
                cur_target.execute(drop_table)
            except psycopg2.Error as e:
                error   = 9
                err_msg = "Error while dropping external table in target"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

            try:
                create_table_sql     = "CREATE EXTERNAL TABLE " + self.target_schemaname + "." + self.target_tablename  + "_ext_hvr ( " + target_create_list + " )" + " LOCATION ('gpfdist://" + gpfdistHost + ":" + str(port_number) + "/in_progress/*-" +  self.target_tablename + ".csv') FORMAT 'CSV' (DELIMITER '|' NULL '\\\\N' ESCAPE '" + "\\\\" + "')"
                print create_table_sql
                cur_target.execute(create_table_sql)
            except psycopg2.Error as e:
                error   = 10
                err_msg = "Error while creating external table in target"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if (self.target_tablename == 'gets_tool_fault' and self.source_schemaname == 'rmd') or (self.target_schemaname== 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca') or (self.target_schemaname== 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca2') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca3') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca') or (self.target_schemaname  == 'rmd' and self.target_tablename  == 'gets_tool_mp_dc_cca2'):
            create_temp_table_sql = "CREATE TEMP TABLE " + self.target_tablename+ "_temp (" + target_create_list + ") DISTRIBUTED BY (" + self.join_columns + ")"
            insert_temp_table_sql = "INSERT INTO " + self.target_tablename+ "_temp SELECT * FROM " + self.target_schemaname + "." + self.target_tablename + "_ext_hvr"
            # print create_temp_table_sql
            try:
                # cur_target.execute("SET gp_autostats_mode = NONE")
                print "CREATING TEMP TABLE"
                cur_target.execute(create_temp_table_sql)
                print "INSERTING INTO TEMP TABLE"
                cur_target.execute(insert_temp_table_sql)
                rows_inserted = cur_target.rowcount
                print "Rows Inserted into Temp Table : ", rows_inserted
            except psycopg2.Error as e:
                error=11
                err_msg = "Error while creating temp table and inserting in target"
                status = 'Job Error'
                path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
                path_to   = data_paths + self.source_schemaname
                move_files(path_from,path_to)
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path , technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Delete logic

        if self.load_type == 'FULL':
            delete_sql   = "TRUNCATE TABLE " + self.target_schemaname + "." + self.target_tablename
        elif (self.target_tablename == 'gets_tool_fault' and self.target_schemaname == 'rmd') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca2') or (self.target_schemaname == 'rmd' and self.target_tablename  == 'gets_tool_mp_ac_cca3') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca2'):
            delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                           + self.target_tablename + "_temp b WHERE 1=1 AND a.objid = b.objid )"
        else:
            delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                           + self.target_schemaname + "." + self.target_tablename + "_ext_hvr b WHERE 1=1 "
            for i in self.join_columns.split(','):
                delete_sql = delete_sql + " and coalesce(cast(a."+ i + " as text),'99999999999999999') = " + "coalesce(cast(b." + i + " as text),'99999999999999999')"
            delete_sql = delete_sql + " )"
        print delete_sql

        try:
            if (self.target_tablename == 'gets_tool_fault' and self.target_schemaname == 'rmd') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca2') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_ac_cca3') or (self.target_schemaname == 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca') or (self.target_schemaname== 'rmd' and self.target_tablename == 'gets_tool_mp_dc_cca2'):
                cur_target.execute("SET enable_seqscan = off")
                cur_target.execute("SET gp_autostats_mode = NONE")
            time.sleep(3)
            cur_target.execute(delete_sql)
            rows_deleted = cur_target.rowcount
            print "Rows deleted : ", rows_deleted
        except psycopg2.Error as e:
            error   = 12
            err_msg = "Error while deleting records that match the new data at target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Insert logic
        if self.load_type == 'APPEND_ONLY':
            insert_sql   = "INSERT INTO "+ self.target_schemaname+ "." + self.target_tablename+ " SELECT " + insert_list + ", CASE WHEN op_code = 0 THEN 1 ELSE 0 END AS hvr_is_deleted, now()::timestamp without time zone FROM " + self.target_schemaname + "." + self.target_tablename + "_ext_hvr a WHERE a.op_code = 1"
        else:
            insert_sql   = "INSERT INTO " + self.target_schemaname + "." + self.target_tablename + " SELECT " + insert_list + ", CASE WHEN op_code = 0 THEN 1 ELSE 0 END AS hvr_is_deleted, now()::timestamp without time zone FROM ( SELECT *, row_number() OVER (PARTITION BY " + self.join_columns + " ORDER BY time_key DESC) as rownum FROM " \
                           + self.target_schemaname + "." + self.target_tablename + "_ext_hvr WHERE op_code <> 5) a WHERE a.rownum = 1"
        print insert_sql

        try:
            cur_target.execute(insert_sql)
            rows_inserted = cur_target.rowcount
            print "Rows inserted : ", rows_inserted
        except psycopg2.Error as e:
            error   = 13
            err_msg = "Error while inserting new records in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename


        # Update Control table after finishing loading table

        try:
            update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "' where source_schemaname = '" + self.source_schemaname  + "' AND source_tablename = '" + self.source_tablename+ "' AND data_path = 'SRC2GP'"
            update_control_info_sql2 = "UPDATE sync.control_table set hvr_last_processed_value = '" + v_timestamp + "' where source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path = 'GP2HDFS'"
            print update_control_info_sql
            print update_control_info_sql2
            cur_metadata.execute(update_control_info_sql)
            cur_metadata.execute(update_control_info_sql2)
        except psycopg2.Error as e:
            error   = 14
            err_msg = "Error while updating the control table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Move Files to HDFS Staging location

        # try:
        #     from datetime import date
        #     today = date.today()
        #     todaystr = today.isoformat()
        #     path               = in_progress_path + "/" + "*-" + self.source_tablename + ".csv"
        #     hdfs_path          = hdfs_data_path + self.source_schemaname + "/" + todaystr + "/" + self.source_tablename + "/"
        #     move_files(path,hdfs_path)
        # except Exception as e:
        #     error= 13
        #     err_msg = "Error while moving files to HDFS directory"
        #     print err_msg
        #     status  = "Job Error"
        #     output_msg = "Error while moving files to HDFS directory"
        #     print output_msg
        #     path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
        #     path_to   = data_paths + self.source_schemaname
        #     move_files(path_from,path_to)
        #     audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
        #     conn_target.rollback()
        #     conn_target.close()
        #     conn_metadata.close()
        #     return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Final log entry
        try:
            error       = 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Job Finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
        except psycopg2.Error as e:
            error   = 15
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename


        conn_target.commit()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, self.source_schemaname + "." + self.source_tablename


    def file2rds_hvr(self):
        print "Inside file2RDS_HVR"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'RDS - Postgres'
        job_name            = 'SRC-->RDS'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0


        # proceed to point everything at the 'branched' resources
        metastore_dbName           = self.config_list['meta_db_dbName']
        dbmeta_Url                 = self.config_list['meta_db_dbUrl']
        dbmeta_User                = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                 = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbtgt_Url                  = self.config_list['tgt_db_combo_dbUrl']
        dbtgt_User                 = self.config_list['tgt_db_combo_dbUser']

        dbtgt_Pwd                  = base64.b64decode(self.config_list['tgt_db_combo_dbPwd'])
        dbtgt_dbName               = self.config_list['tgt_db_combo_dbName']

        data_paths                 = self.config_list['misc_dataPath']
        hdfs_data_path             = self.config_list['misc_hdfsPath']
        env                         = self.config_list['env']

        t                          = datetime.fromtimestamp(time.time())
        v_timestamp                = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print "Run ID for the table", tablename , " is : ", run_id
        except Exception as e:
            err_msg = "Error connecting to database while fetching  metadata"
            error   = 1
            print e
            return

        #Audit entry at start of job
        err_msg       = ''
        err_msg       = ''
        status        = 'Job Started'
        output_msg    = ''
        rows_inserted = 0
        rows_deleted  = 0
        rows_updated  = 0
        num_errors    = 0
        audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        # Check if HVR has dumped files. If we not we will exit from here. No need to continue the process

        path               = data_paths + self.source_schemaname + "_rds/" + "*-" + self.source_tablename + ".csv"
        files              = glob.glob(path)

        if len(files) == 0:
            error           = 0
            err_msg         = "No data dumped from source @:" + data_paths + self.source_schemaname + "_rds/"
            status          = 'Job Finished'
            output_msg      = "No data dumped from source @:" +  data_paths + self.source_schemaname + "_rds/"
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status,self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished No Data"
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename
        else:
            in_progress_path = data_paths + self.source_schemaname + "_rds/in_progress"
            move_files(path,in_progress_path)

        # Get target database access

        try:
            conn_target, cur_target      = txn_dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
        except psycopg2.Error as e:
            error   = 2
            err_msg = "Error connecting to Target database"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Get columns of Target Table for temp table
        try:
            column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                              + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass" \
                                                                                                   " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            columns         = dbQuery(cur_target, column_list_sql)
        except psycopg2.Error as e:
            error   = 5
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            error   = 6
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Prepare column list for Temp Table

        try:
            len_cols            = len(columns) - 2
            columns             = columns[:len_cols]
            select_list         = '","'.join(d['attname'] for d in columns)
            select_list         = '"' + select_list + '"'
            target_create_list  = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
            target_create_list  = '"' + target_create_list + ", op_code integer, time_key varchar(100)"
            insert_list         = ',a.'.join(d['attname'] for d in columns)
            insert_list         = 'a.' + insert_list
        except Exception as e:
            error   = 7
            print e
            err_msg = "Error while preparing column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Create Temp Table for data load

        try:
            create_temp_table_sql     = "CREATE TEMP TABLE " + self.target_tablename  + "_ext_hvr ( " + target_create_list + " )"
            print create_temp_table_sql
            cur_target.execute(create_temp_table_sql)
        except psycopg2.Error as e:
            error       = 8
            err_msg = "Error while creating temp table in target"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Load Data into Temp Table before final table load

        try:
            file_path = in_progress_path  + "/*-"  + self.source_tablename + ".csv"
            files = glob.glob(file_path)
            for file in files:
                f    = open(file)
                if env == 'dev':
                    insert_temp_sql = "COPY " + self.target_tablename + "_ext_hvr" + " FROM STDIN WITH CSV DELIMITER '|' NULL '\\\\N' ESCAPE '\\\\'"
                else:
                    insert_temp_sql = "COPY " + self.target_tablename + "_ext_hvr" + " FROM STDIN WITH CSV DELIMITER '|' NULL '\N' ESCAPE '\\'"
                print insert_temp_sql
                cur_target.copy_expert(insert_temp_sql,f)
                f.close()
        except psycopg2.Error as e:
            error   = 9
            err_msg = "Error while inserting records into temp table for INCREMENTAL load"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            print sql_error_msg
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Delete Logic
        if self.load_type == 'FULL' :
            delete_sql   = "TRUNCATE TABLE " + self.target_schemaname + "." + self.target_tablename
        else:
            delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                           + self.target_tablename + "_ext_hvr b WHERE 1=1 "
            for i in self.join_columns.split(','):
                delete_sql = delete_sql + " and a."+ i + " = " + "b." + i
            delete_sql = delete_sql + ")"

        try:
            print delete_sql
            cur_target.execute(delete_sql)
            rows_deleted = cur_target.rowcount
            print "Rows deleted : ", rows_deleted
        except psycopg2.Error as e:
            print e
            err_msg = "Error while deleting records that match the new data at target"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            error       = 10
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Insert Logic

        insert_sql   = "INSERT INTO " + self.target_schemaname+ "." + self.target_tablename + " SELECT " + insert_list + ", CASE WHEN op_code = 0 THEN 1 ELSE 0 END AS hvr_is_deleted, now()::timestamp without time zone FROM ( SELECT *, row_number() OVER (PARTITION BY " + self.join_columns+ " ORDER BY time_key DESC) as rownum FROM " \
                       + self.target_tablename + "_ext_hvr WHERE op_code <> 5) a WHERE a.rownum = 1"
        print insert_sql

        try:
            cur_target.execute(insert_sql)
            rows_inserted = cur_target.rowcount
            print "Rows inserted : ", rows_inserted
        except psycopg2.Error as e:
            error       = 11
            err_msg = "Error while inserting new records in target"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname + "_rds"
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Update Control table after finishing loading table
        try:
            update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "', hvr_last_processed_value = '" + v_timestamp + "' where source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path in ('SRC2RDS')"
            print update_control_info_sql
            cur_metadata.execute(update_control_info_sql)
        except psycopg2.Error as e:
            error       = 14
            err_msg = "Error while updating the control table"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Move Files to HDFS Staging location

        try:
            from datetime import date
            today = date.today()
            todaystr = today.isoformat()
            path               = in_progress_path + "/" + "*-" + self.source_tablename + ".csv"
            hdfs_path          = hdfs_data_path + self.source_schemaname + "_rds/" + todaystr + "/" + self.source_tablename + "/"
            print path
            print hdfs_path
            move_files(path,hdfs_path)
        except Exception as e:
            error= 13
            err_msg = "Error while moving files to HDFS directory"
            print err_msg
            status  = "Job Error"
            output_msg = "Error while moving files to HDFS directory"
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        # Final log entry
        try:
            error= 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Job Finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
        except psycopg2.Error as e:
            error= 15
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            path_from = in_progress_path + "/*-" + self.source_tablename + ".csv"
            path_to   = data_paths + self.source_schemaname
            move_files(path_from,path_to)
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        conn_target.commit()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, self.source_schemaname + "." + self.source_tablename


    def file2rds(self, control=None):
        print "Inside file2RDS_i360"
        tablename           = self.target_schemaname + "." + self.target_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'RDS - Postgres'
        job_name            = 'File-->RDS'
        technology          = 'Python'
        num_errors          = 0
        rows_inserted       = 0
        rows_updated        = 0
        rows_deleted        = 0

        # proceed to point everything at the 'branched' resources
        metastore_dbName          = self.config_list['meta_db_dbName']
        dbmeta_Url                = self.config_list['meta_db_dbUrl']
        dbmeta_User               = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                = base64.b64decode(self.config_list['meta_db_dbPwd'])
        dbtgt_Url                 = self.config_list['tgt_db_i360_dbUrl']
        dbtgt_User                = self.config_list['tgt_db_i360_dbUser']
        dbtgt_dbName              = self.config_list['tgt_db_i360_dbName']
        dbtgt_Pwd                 = base64.b64decode(self.config_list['tgt_db_i360_dbPwd'])
        dbtgt_Url_predix          = self.config_list['tgt_db_predix_dbUrl']
        dbtgt_User_predix         = self.config_list['tgt_db_predix_dbUser']
        dbtgt_dbName_predix       = self.config_list['tgt_db_predix_dbName']
        dbtgt_Pwd_predix          = base64.b64decode(self.config_list['tgt_db_predix_dbPwd'])
        dbtgt_dbName_port         = self.config_list['tgt_db_predix_dbPort']
        dbtgt_Url_predix_wto          = self.config_list['tgt_db_predix_wto_dbUrl']
        dbtgt_User_predix_wto         = self.config_list['tgt_db_predix_wto_dbUser']
        dbtgt_dbName_predix_wto       = self.config_list['tgt_db_predix_wto_dbName']
        dbtgt_Pwd_predix_wto          = base64.b64decode(self.config_list['tgt_db_predix_wto_dbPwd'])
        dbtgt_dbName_port_wto         = self.config_list['tgt_db_predix_wto_dbPort']
        dbUrl                           = self.config_list['mysql_dbUrl']
        dbUser                          = self.config_list['mysql_dbUser']
        dbPwd                           = base64.b64decode(self.config_list['mysql_dbPwd'])
        dbMetastore_dbName              = self.config_list['mysql_dbMetastore_dbName']
        dbApp_dbName                    = self.config_list['mysql_dbApp_dbName']
        data_paths_i360                 = self.config_list['misc_dataPathi360']

        data_stg_hive                   = self.config_list['misc_hdfsStagingPath']

        t                               = datetime.fromtimestamp(time.time())
        # v_timestamp                = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            # run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            # run_id_list                    = run_id_lists[0]
            # run_id                         = run_id_list['nextval']
            # print "Run ID for the table", tablename , " is : ", run_id
        except Exception as e:
            err_msg = "Error connecting to database while fetching  metadata"
            error   = 1
            print e
            return error, err_msg, control

        run_id = 0
        if control.has_key('run_id') and control['run_id'] is not None:
            run_id          = control['run_id']
        else:
            try:
                run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
                run_id_lists = dbQuery(cur_metadata, run_id_sql)
                run_id_list = run_id_lists[0]
                run_id = run_id_list['nextval']
                print "Run ID for the table", tablename , " is : ", run_id
            except Exception as e:
                print e
                error = 2
                err_msg = "Error while getting Run ID"
                status = "Job Error"
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name, \
                              tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, \
                              rows_deleted, error, err_msg, 0, 0, output_msg)

        if control.has_key('v_timestamp') and  control['v_timestamp'] is not None:
            v_timestamp     = control['v_timestamp']
        else:
            t = datetime.fromtimestamp(time.time())
            v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        #Audit entry at start of job
        err_msg       = ''
        err_msg       = ''
        status        = 'Job Started'
        output_msg    = ''
        num_errors    = 0
        audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        if self.data_path == 'GP2Postgres':
            abs_file_name   = data_paths_i360 + self.target_schemaname + "." + self.target_tablename + ".dat"
            path               = data_paths_i360 + self.target_schemaname + "." + self.target_tablename + "*.dat"
        elif self.data_path == 'Hive2RDS' or self.data_path == 'Hive2PREDIX':
            path            = data_stg_hive + "/" + self.data_path.lower() + "/" + self.target_schemaname + "/" + self.target_tablename + "/*"
        print path
        time.sleep(10)
        files              = glob.glob(path)

        if len(files) == 0:
            error           = 0
            err_msg         = "No data dumped from source @:" + path
            status          = 'Job Finished'
            output_msg      = "No data dumped from source @:" +  path
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status,self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            print "Finished No Data"
            conn_metadata.close()
            return error, err_msg, tablename
        else:
            # Get target database access
            if self.data_path == 'Hive2RDS':
                try:
                    conn_target, cur_target      = txn_dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
                except psycopg2.Error as e:
                    error   = 2
                    err_msg = "Error connecting to Target database"
                    print err_msg
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_metadata.close()
                    return error, err_msg, tablename
            elif self.data_path == 'Hive2PREDIX':
                try:
                    # if input.get('predix_app_name') == 'WTO':
                    if self.source_schemaname.find('wheel') <> -1:
                        print "PREDIX APP NAME IS WTO"
                        conn_target, cur_target = txn_dbConnect(dbtgt_dbName_predix_wto, dbtgt_User_predix_wto, dbtgt_Url_predix_wto,dbtgt_Pwd_predix_wto, dbtgt_dbName_port_wto)
                    else:
                        print "PREDIX APP NAME IS MARK"
                        conn_target, cur_target      = txn_dbConnect(dbtgt_dbName_predix, dbtgt_User_predix, dbtgt_Url_predix, dbtgt_Pwd_predix, dbtgt_dbName_port)
                except psycopg2.Error as e:
                    error   = 2
                    err_msg = "Error connecting to Target database"
                    print err_msg
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_metadata.close()
                    return error, err_msg, tablename
                try:
                    connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
                except Exception as e:
                    error = 5
                    err_msg = "[{0}]: Error connecting to Hive Metastore".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    #output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                     #              'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    # self.error_cleanup(input_schema_name, input_table_name, run_id, paths, conn_metadata)
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                                  self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                                  err_msg, 0, 0, output_msg)
                    return error, err_msg, tablename

                # Establish connection to the hive metastore to get the list of columns
                sql_query = "SELECT                                                                   \
                                    c.COLUMN_NAME                                                     \
                            FROM                                                                      \
                                TBLS t                                                                \
                                JOIN DBS d                                                            \
                                    ON t.DB_ID = d.DB_ID                                              \
                                JOIN SDS s                                                            \
                                    ON t.SD_ID = s.SD_ID                                              \
                                JOIN COLUMNS_V2 c                                                     \
                                    ON s.CD_ID = c.CD_ID                                              \
                            WHERE                                                                     \
                                TBL_NAME = " + "'" + self.source_tablename + "' " + "                 \
                                AND d.NAME=" + " '" + self.source_schemaname + "' " + "                   \
                                ORDER by c.INTEGER_IDX"
                try:
                    cursor = connection.cursor()
                    # print (print_hdr + "hive_meta_source_sql: " + sql_query)
                    cursor.execute(sql_query)
                    source_result = cursor.fetchall()
                    print ( "source_result:", source_result)
                except Exception as e:
                    error = 6
                    err_msg = "[{0}]: Issue running SQL in hive metadata database:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    # output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated, 'rows_deleted': rows_deleted,
                    #                'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                    # self.error_cleanup(self.source_schema, input_table_name, run_id, paths, conn_metadata)
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                                  self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                                  err_msg, 0, 0, output_msg)
                    return error, err_msg, tablename
                finally:
                    connection.close()

                source_select_list = ', '.join(map(''.join, source_result))
                print source_select_list

            # Incremental Logic
            if self.load_type == 'INCREMENTAL' :
                create_temp_table_sql = "CREATE TEMPORARY TABLE " + self.target_tablename + "_temp AS SELECT * FROM " + self.target_schemaname + "." + self.target_tablename + \
                                        " WHERE 1=0"
                print create_temp_table_sql
                try:
                    cur_target.execute(create_temp_table_sql)
                except psycopg2.Error as e:
                    error   = 2
                    err_msg = "Error while creating Temp Table in Target Database for Incremental Load"
                    print err_msg
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_metadata.close()
                    return error, err_msg, tablename

                if self.data_path == 'GP2Postgres':
                    # abs_file_name   = data_paths_i360 + self.source_schemaname + "." + self.source_tablename + ".dat"
                    # print abs_file_name
                    file          = open(abs_file_name)
                    insert_temp_sql = "COPY " + self.target_tablename + "_temp" + " FROM STDIN WITH DELIMITER E'\x01' "
                    print insert_temp_sql
                    try:
                        cur_target.copy_expert(insert_temp_sql,file)
                        file.close()
                    except psycopg2.Error as e:
                        err_msg = "Error while inserting records into temp table for INCREMENTAL load"
                        print err_msg
                        status = 'Job Error'
                        sql_state = e.pgcode
                        sql_error_msg = e.pgerror
                        output_msg = traceback.format_exc()
                        try:
                            os.remove(abs_file_name)
                        except OSError:
                            pass
                        print output_msg
                        audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                        conn_target.rollback()
                        conn_target.close()
                        conn_metadata.close()
                        return error, err_msg, tablename
                elif self.data_path == 'Hive2RDS' or self.data_path == 'Hive2PREDIX':
                    try:
                        # file_path    = data_stg_hive + self.source_schemaname + "/" + self.source_tablename
                        files        = glob.glob(path)
                        for file in files:
                            f        = open(file)
                            insert_temp_sql = "COPY " + self.target_tablename + "_temp" + " FROM STDIN WITH DELIMITER ',' NULL '\\N' "
                            print insert_temp_sql
                            cur_target.copy_expert(insert_temp_sql,f)
                            f.close()
                    except psycopg2.Error as e:
                        error   = 9
                        err_msg = "Error while inserting records into temp table for INCREMENTAL load"
                        status = 'Job Error'
                        sql_state = e.pgcode
                        sql_error_msg = e.pgerror
                        print sql_error_msg
                        output_msg = traceback.format_exc()
                        try:
                            shutil.rmtree(data_stg_hive + "/" + self.source_schemaname + "/" + self.source_tablename)
                            if self.log_mode == 'DEBUG':
                                print "Removed Files"
                        except Exception as e:
                            if e.errno == 2:
                                if self.log_mode == 'DEBUG':
                                    print "File/Directory does not exist"
                                pass
                            else:
                                print e
                        audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                        conn_target.rollback()
                        conn_target.close()
                        conn_metadata.close()
                        return error, err_msg, tablename
            # Delete Logic
            if self.load_type == 'FULL' :
                delete_sql   = "TRUNCATE TABLE " + self.target_schemaname + "." + self.target_tablename
            else:
                delete_sql   = "DELETE FROM "  + self.target_schemaname + "." + self.target_tablename + " a WHERE EXISTS (SELECT 1 FROM  " \
                               + self.target_tablename + "_temp b WHERE 1=1 "
                for i in self.join_columns.split(','):
                    delete_sql = delete_sql + " and a."+ i + " = " + "b." + i
                delete_sql = delete_sql + ")"

            try:
                print delete_sql
                cur_target.execute(delete_sql)
                rows_deleted = cur_target.rowcount
                print "Rows Deleted : ", rows_deleted
            except psycopg2.Error as e:
                error   = 10
                err_msg = "Error while deleting records that match the new data at target"
                print err_msg
                status = 'Job Error'
                sql_state = e.pgcode
                sql_error_msg = e.pgerror
                output_msg = traceback.format_exc()
                print output_msg
                if self.data_path == 'Hive2RDS' or self.data_path == 'Hive2PREDIX':
                    try:
                        shutil.rmtree(data_stg_hive + "/" + self.source_schemaname + "/" + self.source_tablename)
                        if self.log_mode == 'DEBUG':
                            print "Removed Files"
                    except Exception as e:
                        if e.errno == 2:
                            if self.log_mode == 'DEBUG':
                                print "File/Directory does not exist"
                            pass
                        else:
                            print e
                else:
                    try:
                        os.remove(abs_file_name)
                    except OSError:
                        pass
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                print "Error logging done"
                conn_target.rollback()
                print "target connection rolled back"
                conn_target.close()
                print "target connection closed"
                conn_metadata.close()
                print "metadata connection closed"
                return error, err_msg, tablename


            # Final Insert Logic
            if self.data_path == 'GP2Postgres':
                file          = open(abs_file_name)
                insert_sql    = "COPY " + self.target_schemaname + "." + self.target_tablename + " FROM STDIN WITH DELIMITER E'\x01' "
                print  insert_sql
                try:
                    cur_target.copy_expert(insert_sql,file)
                    rows_inserted = cur_target.rowcount
                    print "Rows inserted : ", rows_inserted
                except psycopg2.Error as e:
                    err_msg = "Error while inserting new records in target"
                    print err_msg
                    status = 'Job Error'
                    sql_state = e.pgcode
                    sql_error_msg = e.pgerror
                    output_msg = traceback.format_exc()
                    print output_msg
                    try:
                        os.remove(abs_file_name)
                    except OSError:
                        pass
                    print output_msg
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_target.rollback()
                    conn_target.close()
                    conn_metadata.close()
                    return
            elif self.data_path == 'Hive2RDS':
                print "Inside Hive2RDS Final INSERT Logic"
                try:
                    # file_path    = data_stg_hive + self.source_schemaname + "/" + self.source_tablename
                    files        = glob.glob(path)
                    for file in files:
                        print "Loading File : ", file
                        f        = open(file)
                        insert_sql    = "COPY " + self.target_schemaname + "." + self.target_tablename + " FROM STDIN WITH DELIMITER ',' NULL '\\N' "
                        print insert_sql
                        cur_target.copy_expert(insert_sql,f)
                        rows_inserted += cur_target.rowcount
                        print "Rows inserted : ", rows_inserted
                        f.close()
                except psycopg2.Error as e:
                    print e
                    error   = 9
                    err_msg = "Error while doing Final INSERT"
                    status = 'Job Error'
                    sql_state = e.pgcode
                    sql_error_msg = e.pgerror
                    output_msg = traceback.format_exc()
                    print output_msg
                    try:
                        shutil.rmtree(data_stg_hive+"/"+self.source_schemaname+"/"+self.source_tablename)
                        if self.log_mode == 'DEBUG':
                            print "Removed Files"
                    except Exception as e:
                        if e.errno == 2:
                            if self.log_mode == 'DEBUG':
                                print "File/Directory does not exist"
                            pass
                        else:
                            print e
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_target.rollback()
                    conn_target.close()
                    conn_metadata.close()
                    return error, err_msg, tablename
            elif self.data_path == 'Hive2PREDIX':
                print "Inside Hive2PREDIX Final INSERT Logic"
                try:
                    # file_path    = data_stg_hive + self.source_schemaname + "/" + self.source_tablename
                    files = glob.glob(path)
                    for file in files:
                        print "Loading File : ", file
                        f = open(file)
                        insert_sql = "COPY " + self.target_schemaname + "." + self.target_tablename + "(" + source_select_list + ") FROM STDIN WITH DELIMITER ',' NULL '\\N' "
                        print insert_sql
                        cur_target.copy_expert(insert_sql, f)
                        rows_inserted += cur_target.rowcount
                        print "Rows inserted : ", rows_inserted
                        f.close()
                except psycopg2.Error as e:
                    print e
                    error = 9
                    err_msg = "Error while doing Final INSERT"
                    status = 'Job Error'
                    sql_state = e.pgcode
                    sql_error_msg = e.pgerror
                    output_msg = traceback.format_exc()
                    print output_msg
                    try:
                        shutil.rmtree(data_stg_hive + "/" + self.source_schemaname + "/" + self.source_tablename)
                        if self.log_mode == 'DEBUG':
                            print "Removed Files"
                    except Exception as e:
                        if e.errno == 2:
                            if self.log_mode == 'DEBUG':
                                print "File/Directory does not exist"
                            pass
                        else:
                            print e
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,
                                  status, self.data_path, technology, rows_inserted, rows_updated, rows_deleted,
                                  num_errors, err_msg, 0, 0, output_msg)
                    conn_target.rollback()
                    conn_target.close()
                    conn_metadata.close()
                    return error, err_msg, tablename

                # except Exception as e:
                #     print e
                #     error   = 9
                #     err_msg = "Error while inserting records into temp table for INCREMENTAL load"
                #     status = 'Job Error'
                #     try:
                #         shutil.rmtree(data_stg_hive+"/"+self.source_schemaname+"/"+self.source_tablename)
                #         if self.log_mode == 'DEBUG':
                #             print "Removed Files"
                #     except Exception as e:
                #         if e.errno == 2:
                #             if self.log_mode == 'DEBUG':
                #                 print "File/Directory does not exist"
                #             pass
                #         else:
                #             print e
                #     audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                #     conn_target.rollback()
                #     conn_target.close()
                #     conn_metadata.close()
                #     return error, err_msg, tablename


            # Update Control table after finishing loading table

            try:
                update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "' where id = " + str(self.id) + " AND source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path = '"+ self.data_path +"'"
                print update_control_info_sql
                cur_metadata.execute(update_control_info_sql)
            except psycopg2.Error as e:
                print e
                error   = 14
                err_msg = "Error while updating the control table"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                try:
                    os.remove(abs_file_name)
                except OSError:
                    pass
                print output_msg
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_target.rollback()
                conn_target.close()
                conn_metadata.close()
                return error, err_msg, tablename


            if self.data_path == 'GP2Postgres':
                try:
                    os.remove(abs_file_name)
                except OSError:
                    pass
            elif self.data_path == 'Hive2RDS':
                try:
                    shutil.rmtree(data_stg_hive+"/"+self.source_schemaname+"/"+self.source_tablename)
                    if self.log_mode == 'DEBUG':
                        print "Removed Files"
                except Exception as e:
                    if e.errno == 2:
                        if self.log_mode == 'DEBUG':
                            print "File/Directory does not exist"
                        pass
                    else:
                        print e
        # Final log entry
        try:
            error= 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Job Finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename, \
                          status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, \
                          err_msg ,rows_inserted,rows_deleted,output_msg)
        except psycopg2.Error as e:
            error= 15
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, tablename

        conn_target.commit()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, tablename


if __name__ == "__main__":
    print "ERROR: Direct execution on this file is not allowed"

# if __name__ == "__main__":
#
#
#     APP_NAME='incremental_update_' + str(sys.argv[1])
#     table_name = sys.argv[1]
#     # Configure OPTIONS
#     conf = SparkConf().setAppName(APP_NAME)
#     #in cluster this will be like
#     #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
#     sc   = SparkContext(conf=conf)
#     load_id = int(sys.argv[2])
#     run_id  = int(sys.argv[3])
#     data_path = str(sys.argv[4])
#     v_timestamp = sys.argv[5]
#     print data_path
#     sc.setLogLevel('ERROR')
#     # Execute Main functionality
#     # error, err_msg = merge_data(sc,table_name,load_id,run_id,data_path,v_timestamp)
#     if data_path == "GP2HDFS":
#         file2db_sync = File2DBSync()
#         error, err_msg = file2db_sync.load_file2hive_spark(sc, table_name, load_id, run_id, data_path, v_timestamp)
#
#     print error
#     print err_msg
#
#     sys.exit(error)



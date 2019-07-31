import datetime
import sys
sys.path.append("/apps/common")
from utils import run_cmd,load_config,dbConnect, dbQuery, move_files, get_kerberos_token
import traceback
from datetime import datetime
import psycopg2
import base64
import glob
import gzip
import shutil
import json
import requests
import os
import subprocess
from datasync_init import DataSyncInit
import time
from auditLog import audit_logging


class FStoHDFS(DataSyncInit):
    def __init__(self, input=None):
        print "Inside Class FS --> HDFS"
        self.config_list            = load_config()
        self.paths                  = self.config_list['hive_log_hiveLogPath']
        self.staging_path           = self.config_list['misc_hdfsStagingPath']
        self.metastore_dbName       = self.config_list['meta_db_dbName']
        self.dbmeta_Url             = self.config_list['meta_db_dbUrl']
        self.dbmeta_User            = self.config_list['meta_db_dbUser']
        self.dbmeta_Pwd             = base64.b64decode(self.config_list['meta_db_dbPwd'])
        self.hive_warehouse_path    = self.config_list['misc_hiveWarehousePath']

        get_kerberos_token()

        if input is not None:
            self.id                     = input['id']
            self.source_schemaname      = input['source_schemaname']
            self.source_tablename       = input['source_tablename']
            self.target_schemaname      = input['target_schemaname']
            self.target_tablename       = input['target_tablename']
            self.load_type              = input['load_type']
            self.incremental_column     = input['incremental_column']
            self.last_run_time          = input['last_run_time']
            self.second_last_run_time   = input['second_last_run_time']
            self.join_columns           = input['join_columns']
            self.log_mode               = input['log_mode']
            self.data_path              = input['data_path']
            self.load_id                = input['load_id']
            self.plant_name             = input['plant_name']

        else:
            print "[FStoHDFS: __init__] - No data defined"

    def fs2hdfs(self):
        self.technology = 'Python'
        self.system_name = 'HDFS'
        self.job_name = 'FS-->HDFS'
        t = datetime.fromtimestamp(time.time())
        v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))
        tablename = self.target_schemaname + "." + self.target_tablename

        try:
            conn_metadata, cur_metadata = dbConnect(self.metastore_dbName, self.dbmeta_User, self.dbmeta_Url,self.dbmeta_Pwd)
        except psycopg2.Error as e:
            error = 1
            err_msg = "Error connecting to control table database".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            return error, err_msg, output_msg

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
            audit_logging(cur_metadata, self.load_id, 0, self.plant_name, self.system_name, self.job_name, tablename, status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
        status = 'Job Started'
        error = 0
        err_msg = ''
        output_msg = ''
        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,tablename, status, self.data_path, self.technology, 0, 0,0, error, err_msg, 0, 0, output_msg)


        if len(self.source_schemaname) > 0 and len(self.source_tablename) > 0:
            local_file_name = self.source_schemaname + self.source_tablename
        elif len(self.source_schemaname) > 0 and len(self.source_tablename) == 0:
            local_file_name = self.source_schemaname
        elif len(self.source_schemaname) == 0 and len(self.source_tablename) > 0:
            local_file_name = self.source_tablename
        else:
            error = 2
            err_msg = "No source to run this program"
            output = "No source to run this program"
            status = 'Job Error'
            print err_msg
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            return error, err_msg, tablename
        print local_file_name
        try:
            files = glob.glob(local_file_name)
            if len(files) == 0:
                error = 3
                err_msg = "No data found"
                output = "No data found"
                print err_msg
                return error, err_msg, tablename
            else:
                self.target_path = self.hive_warehouse_path + "/" + self.target_schemaname + ".db/" + self.target_tablename + "/"
                (ret, out, err)         = run_cmd(['hadoop','fs','-rm','-r', (self.target_path+ "*")])
                if ret:
                    if err.find("No such file or directory") <> -1:
                        (ret, out, err) = run_cmd(['hadoop','fs','-mkdir',self.target_path])
                        if ret:
                            pass
                    else:
                        error = 4
                        err_msg = "Error in cleaning in target path"
                        output = traceback.format_exc()
                        return  error, err_msg, tablename
        except Exception as e:
            error = 5
            err_msg = "Error while checking the local file path or cleaning the target location in HDFS"
            output  = traceback.format_exc()
            status = 'Job Error'
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            return error, err_msg, tablename

        try:
            files = glob.glob(local_file_name)
            for file in files:
                (ret, out, err) = run_cmd(['hadoop','fs','-copyFromLocal', file, self.target_path])
                if ret > 0:
                    error = 5
                    err_msg = "Error in ingesting into HDFS"
                    output = traceback.format_exc()
                    return error, err_msg, tablename
        except Exception as e:
            error = 6
            err_msg = "Error while loading data into HDFS"
            output  = traceback.format_exc()
            status = 'Job Error'
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            return error, err_msg, tablename

        try:
            update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "' where id = " + str(self.id) + " AND target_schemaname = '" + self.target_schemaname+ "' AND target_tablename = '" + self.target_tablename+ "' AND data_path = '" + self.data_path + "'"
            print update_control_info_sql
            cur_metadata.execute(update_control_info_sql)
        except psycopg2.Error as e:
            print e
            error = 7
            err_msg = "Error while updating the control table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            return error, err_msg, tablename

        # Final log entry
        try:
            error= 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Job Finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)

        except psycopg2.Error as e:
            error= 15
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
            return error, err_msg, tablename
        finally:
            conn_metadata.close()
        return error, err_msg, tablename

if __name__ == "__main__":
    print "ERROR: Direct execution on this file is not allowed"
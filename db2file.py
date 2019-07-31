import psycopg2
import base64
import sys
import traceback
import datetime
import time
from datetime import datetime
import shutil
sys.path.append("/apps/common/")
sys.path.append("/data/analytics/common/")
from utils import remove_files, dbConnect, dbQuery, sendMail, dbConnectHive, txn_dbConnect, run_cmd, dbConnectOra, dbConnectSQLServer
from auditLog import audit_logging
from datasync_init import DataSyncInit
import os
import glob
from datetime import datetime as logdt
import mysql.connector

class Db2FileSync(DataSyncInit):


    def __init__(self, input = None):
        print ("[File2DBSync: __init__] -  Entered")
        super(Db2FileSync, self).__init__()

        self.class_name = self.__class__.__name__
        method_name = self.class_name + ": " + "__init__"

        if input is not None:
            self.id = input['id']
            self.source_schemaname = input['source_schemaname']
            self.source_tablename = input['source_tablename']
            self.target_schemaname = input['target_schemaname']
            self.target_tablename = input['target_tablename']
            self.load_type = input['load_type']
            self.incremental_column = input['incremental_column']
            self.last_run_time = input['last_run_time']
            self.second_last_run_time = str(input['second_last_run_time'])
            self.join_columns = input['join_columns']
            self.log_mode = input['log_mode']
            self.data_path = input['data_path']
            self.load_id = input['load_id']
            self.plant_name = input['plant_name']
            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            self.is_special_logic = input.get('is_special_logic', False)
            self.is_partitioned = input.get('is_partitioned', False)
            self.system_name_ct = input['system_name_ct']
            self.custom_sql = input['custom_sql']
        else:
            print (method_name + "No data defined")
        # print (method_name, self.config_list)


    def extract_gp2file_hive(self):

        method_name = self.class_name + ": " + "extract_gp2file_hive"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + "] - "
        print (print_hdr + "Entered")

        t=datetime.fromtimestamp(time.time())
        v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        tablename = self.source_schemaname + '.' + self.source_tablename
        # self.input_schema_name, self.source_table_name = table.split('.')

        run_id_sql  = "select nextval('sbdt.edl_run_id_seq')"
        self.technology  = 'Python'
        self.system_name = 'GPDB'
        self.job_name    = 'GPDB-->HDFS'

        num_errors  = 1
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0

        metastore_dbName           = self.config_list['meta_db_dbName']
        dbmeta_Url                 = self.config_list['meta_db_dbUrl']
        dbmeta_User                = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                 = base64.b64decode(self.config_list['meta_db_dbPwd'])
        src_dbName                 = self.config_list['src_db_gp_dbName']
        dbsrc_Url                  = self.config_list['src_db_gp_dbUrl']
        dbsrc_User                 = self.config_list['src_db_gp_dbUser']
        dbsrc_Pwd                  = base64.b64decode(self.config_list['src_db_gp_dbPwd'])
        dbtgt_host                 = self.config_list['tgt_db_hive_dbUrl']
        dbtgt_host2                 = self.config_list['tgt_db_hive_dbUrl2']
        # dbtgt_Url                  = self.config_list['tgt_db_beeline_dbUrl']
        dbtgt_Port                 = self.config_list['tgt_db_hive_dbPort']
        dbtgt_Auth                 = self.config_list['tgt_db_hive_authMech']
        # dbtgt_classpath            = self.config_list['tgt_db_beeline_classPath']
        emailSender                = self.config_list['email_sender']
        emailReceiver              = self.config_list['email_receivers']
        executorMemory             = self.config_list['spark_params_executorMemory']
        executorCores              = self.config_list['spark_params_executorCores']
        driverMemory               = self.config_list['spark_params_driverMemory']
        loadScript                 = self.config_list['spark_params_loadScript']
        sparkVersion               = self.config_list['spark_params_sparkVersion']
        # paths                      = "/apps/staging/"
        paths                      = self.config_list['misc_hdfsStagingPath']
        t=datetime.fromtimestamp(time.time())
        v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        error = 0
        err_msg = ''
        output = {}
        conn_metadata = None
        conn_source = None
        conn_target = None

        print (print_hdr + "MetaDB details: ", metastore_dbName, dbmeta_Url, dbmeta_User, dbmeta_Pwd, paths)
        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print (print_hdr + "Run ID for the table " + tablename, " is: " + str(run_id))
        except Exception as e:
            error = 1
            err_msg = "Error: connecting to database while fetching metadata "
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata)
            sendMail(emailSender, emailReceiver, err_msg, tablename, self.load_id, self.config_list['env'], "ERROR", self.data_path, self.load_type)
            return error, err_msg, output

        #Audit entry at start of job

        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + ": " + tablename + ": " + str(run_id) + "] - "
        status = 'Job Started'
        output_msg = ''
        audit_logging(cur_metadata, self.load_id,run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                      self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        self.update_control(self.source_schemaname, self.source_tablename, self.CONTROL_STATUS_INPROGRESS, run_id)

        try:
            conn_source, cur_source      = dbConnect(src_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
            if self.log_mode == 'DEBUG':
                status  = "Connected to Source database"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 4
            err_msg = method_name + "[{0}]: Error connecting to source database".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            #continue
            return error, err_msg, output

        try:

            column_list_sql = "SELECT case when ((a.attname like '%comments%' or a.attname like '%desc%' or a.attname like '%disposition%' or a.attname like 'reason1') AND atttypid in (1043,25,11736)) then 'regexp_replace('||attname||',''[\\\\n\\\\t\\\\r]+'', '' '',''g'')'::text else attname::text end as attname from pg_attribute a WHERE  attrelid = " \
                                + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass" \
                                " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            print column_list_sql
            columns         = dbQuery(cur_source, column_list_sql)
        except psycopg2.Error as e:
            error   = 5
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology, rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            error   = 6
            audit_logging(cur_metadata, self.load_id,  run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, self.source_schemaname + "." + self.source_tablename

        select_list = ','.join(d['attname'] for d in columns)
        # select_list = '"' + select_list + '"'
        print "SELECT LIST : ", select_list
        try:
            drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.source_schemaname + "." + self.source_tablename + "_wext"
            print (print_hdr + "drop_src_table: " + drop_src_table)
            cur_source.execute(drop_src_table)
            if self.log_mode == 'DEBUG':
                status  = "Dropped Writable External Table on Source"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 5
            err_msg = method_name + "[{0}]: Error while dropping Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            # conn_metadata.close()
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            #continue
            return error, err_msg, output

        try:
            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            tmp_source_schemaname = self.source_schemaname
            if self.data_path.find("GP2HDFS") <> -1 and self.target_schemaname <> self.source_schemaname and self.is_special_logic:
                tmp_source_schemaname = self.target_schemaname

            create_writable_sql = "CREATE WRITABLE EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext \
                        (LIKE " + self.source_schemaname + "." + self.source_tablename + ") \
                        LOCATION('gphdfs://getnamenode:8020/apps/staging/" + tmp_source_schemaname + "/" + self.source_tablename.replace('$','_') + "_ext') \
                        FORMAT 'TEXT' (DELIMITER E'\x1A' NULL '' ESCAPE '\\\\')"

            print (print_hdr + "create_writable_sql: " + create_writable_sql)
            cur_source.execute(create_writable_sql)
            if self.log_mode == 'DEBUG':
                status  = "Created Writable External Table on Source"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 6
            err_msg = method_name + "[{0}]: Error while creating Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            # continue
            return error, err_msg, output

        try:
            add_column_list = ["hive_updated_by text", "hive_updated_date timestamp without time zone", "op_code integer", "time_key varchar(100)"]

            for add_column in add_column_list:
                hive_add_colum_sql = "ALTER EXTERNAL TABLE " + self.source_schemaname + "." + self.source_tablename + "_wext ADD COLUMN " + str(add_column)
                print (print_hdr + "hive_add_colum_sql: " + str(add_column) + ": " + hive_add_colum_sql)
                cur_source.execute(hive_add_colum_sql)
                if self.log_mode == 'DEBUG':
                    status  = "Added column" + str(add_column) + " to Writable External Table"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        except psycopg2.Error as e:
            error = 7
            err_msg = method_name + "[{0}]: Error while adding columns to Writable External Table".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            # continue
            return error, err_msg, output

        print (print_hdr + "load_type: " + self.load_type)

        try:
            if self.load_type == 'FULL':
                incremental_sql = "INSERT INTO " + self.source_schemaname + "." + self.source_tablename + "_wext" + " SELECT " + select_list + ", 'datasync', now(), 1, 1  FROM " + self.source_schemaname + "." + self.source_tablename
            elif self.load_type == 'INCREMENTAL':
                incremental_sql = "INSERT INTO " + self.source_schemaname + "." + self.source_tablename + "_wext" + " SELECT *, 'datasync', now(), 1, 1 FROM " + self.source_schemaname + "." + self.source_tablename + " WHERE " + self.incremental_column + " > '" + self.second_last_run_time + "' AND " + self.incremental_column + " <= '" + v_timestamp + "'"
            else:
                incremental_sql = "INSERT INTO " + self.source_schemaname + "." + self.source_tablename + "_wext" + " SELECT *, 'datasync', now(), 1, 1 FROM " + self.source_schemaname + "." + self.source_tablename + " WHERE " + self.incremental_column + " > '" + self.second_last_run_time + "' AND " + self.incremental_column + " <= '" + v_timestamp + "' AND hvr_is_deleted = 0"

            print (print_hdr + "incremental_sql: " + incremental_sql)

            # Special logic to mirror table from one schema in GP to a different schema in HIVE
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            remove_files(paths, tmp_source_schemaname, self.source_tablename)
            if self.log_mode == 'DEBUG':
                status  = "Removed files from HDFS before dumping data from GPDB"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            cur_source.execute(incremental_sql)
            rows_inserted  = cur_source.rowcount
            if self.log_mode == 'DEBUG':
                status  = "Dumped data into HDFS through Writable External Table"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            print (print_hdr + "Rows inserted into Writable External Table : " + str(rows_inserted))

        except psycopg2.Error as e:
            error = 11
            err_msg = method_name + "[{0}]: Error while inserting data into Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            return error, err_msg, output
        except Exception as e:
            error = 12
            err_msg = method_name + "[{0}]: Error before inserting data into Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            return error, err_msg, output

        try:
            drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.source_schemaname + "." + self.source_tablename + "_wext"
            print (print_hdr + "drop_src_table: " + drop_src_table)
            cur_source.execute(drop_src_table)
            if self.log_mode == 'DEBUG':
                status  = "Dropped Writable External Table on Source"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 13
            err_msg = method_name + "[{0}]: Error while dropping Writable External table in source".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            return error, err_msg, output

        try:
            status     = "Finished GPDB part"
            num_errors = 0
            err_msg    = "No Errors"
            output_msg = "No Errors"
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 14
            err_msg = method_name + "[{0}]: Error while updating Log Table after GPDB part".format(error)
            status = 'Job Error'
            conn_source.rollback()
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source)
            return error, err_msg, output

        # HIVE
        print (print_hdr + "HIVE............")

        try:
                conn_target, cur_target   = dbConnectHive(dbtgt_host, dbtgt_Port, dbtgt_Auth)
                if self.log_mode == 'DEBUG':
                    status  = "Connected to Target Database"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            try:
                conn_target, cur_target   = dbConnectHive(dbtgt_host2, dbtgt_Port, dbtgt_Auth)
            except Exception as e:
                error = 15
                err_msg      = method_name + "[{0}]: Error while connecting to target database".format(error)
                status       = 'Job Error'
                # print (print_hdr + "Exception: ", e)
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
                return error, err_msg, output

        hive_columns_sql = "SELECT ARRAY_TO_STRING(ARRAY(SELECT '`' || COLUMN_NAME||'` '||( \
                                case when data_type in('numeric') and numeric_precision is not null and numeric_scale=0 then 'bigint' \
                                when data_type in('numeric','double precision') or numeric_scale > 0 then 'double' \
                                when data_type in('character varying','character','text') then 'string' \
                                when data_type in('timestamp without time zone') then 'timestamp' \
                                when data_type in('bigint') then 'bigint' when data_type in('integer') then 'int' \
                                when data_type in('smallint') then 'smallint' \
                                when data_type in('date') then 'date' \
                                when data_type in('name') then 'string' \
                                when data_type in ('real') then 'double' \
                                when data_type in ('ARRAY') then 'array<string>' " \
                                "when data_type in ('boolean') then 'boolean' " \
                                "else 'string' end) \
                            FROM INFORMATION_SCHEMA.COLUMNS \
                            WHERE TABLE_NAME='" + self.source_tablename + "' \
                            and table_schema='" + self.source_schemaname + "' ORDER BY ORDINAL_POSITION), ', ')"

        print (print_hdr + "hive_columns_sql: " + hive_columns_sql)

        try:
            columns      = dbQuery(cur_source,hive_columns_sql)
            if self.log_mode == 'DEBUG':
                status  = "Got column list from GPDB with data type conversion"
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except psycopg2.Error as e:
            error = 16
            err_msg      = method_name + "[{0}]: Error while getting column list with data type for target table from GPDB".format(error)
            status       = 'Job Error'
            # remove_files(paths,self.source_schemaname,self.source_tablename)
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
            return error, err_msg, output

        column_list_orig                = ",".join(d['array_to_string'] for d in columns)
        column_list                     = column_list_orig + ",`hive_updated_by` string, `hive_updated_date` timestamp, `op_code` int, `time_key` string "
        column_list_full_load           = column_list_orig + ",`hive_updated_by` string, `hive_updated_date` timestamp "
        print (print_hdr, column_list)

        # Special logic to mirror table from one schema in GP to a different schema in HIVE
        self.source_schemaname = tmp_source_schemaname

        drop_hive_ext_table             = "DROP TABLE IF EXISTS `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "_ext`"
        print (print_hdr + "drop_hive_ext_table: " + drop_hive_ext_table)
        try:
                cur_target.execute(drop_hive_ext_table)
                if self.log_mode == 'DEBUG':
                    status  = "Dropped Hive External table"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            # print e
            error = 17
            err_msg      = method_name + "[{0}]: Error while dropping Hive External table".format(error)
            status       = 'Job Error'
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                          self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
            return error, err_msg, output

        create_hive_ext_table           = "CREATE EXTERNAL TABLE `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "_ext`(" + column_list.replace('$','_') + ") \
                                    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u001A' ESCAPED BY '\\\\' LINES TERMINATED BY '\n'  \
                                    STORED AS TEXTFILE LOCATION '" + paths + self.source_schemaname + "/" + self.source_tablename.replace('$','_') + "_ext' TBLPROPERTIES('serialization.null.format'='')"

        print (print_hdr + "create_hive_ext_table: " + create_hive_ext_table)

        try:
                cur_target.execute(create_hive_ext_table)
                if self.log_mode == 'DEBUG':
                    status  = "Created Hive External Table"
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                  self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

        except Exception as e:
            error = 18
            err_msg      = method_name + "[{0}]: Error while creating Hive External table".format(error)
            status       = 'Job Error'
            output_msg   = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, self.data_path, \
                          self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
            return error, err_msg, output

        print (print_hdr + "load_type: " + self.load_type)
        # if self.load_type == 'FULL':
        if self.load_type == 'FULL' and not self.is_partitioned:
            drop_hive_mngd_table        = "DROP TABLE IF EXISTS `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "`"
            print (print_hdr + "drop_hive_mngd_table: " + drop_hive_mngd_table)
            try:
                    cur_target.execute(drop_hive_mngd_table)
                    if self.log_mode == 'DEBUG':
                        status  = "Dropped Managed Table in Hive for Full Load"
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                      self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            except Exception as e:
                error = 19
                err_msg      = method_name + "[{0}]: Error while dropping Hive Managed table".format(error)
                status       = 'Job Error'
                # remove_files(paths,self.source_schemaname,self.source_tablename)
                output_msg   = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
                return error, err_msg, output

            # create_hive_mngd_table      = "CREATE EXTERNAL TABLE `" + self.source_schemaname + "." + self.source_tablename.replace('$','_') + "`(" + column_list.replace('$','_') + ") \
            create_hive_mngd_table = "CREATE EXTERNAL TABLE `" + self.source_schemaname + "." + self.source_tablename.replace('$', '_') + "`(" + column_list_full_load.replace('$', '_') + ") \
                                    STORED AS ORC LOCATION '/apps/hive/warehouse/" + self.source_schemaname + ".db/" + self.source_tablename.replace('$','_') + "' \
                                    TBLPROPERTIES ('orc.compress'='ZLIB')"

            print (print_hdr + "create_hive_mngd_table: " + create_hive_mngd_table)
            try:
                    cur_target.execute(create_hive_mngd_table)
                    if self.log_mode == 'DEBUG':
                        status  = "Created Table in Hive for Full Load"
                        audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                                      self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)

            except Exception as e:
                error = 20
                err_msg      = method_name + "[{0}]: Error while Creating Hive External table".format(error)
                status       = 'Job Error'
                # remove_files(paths,self.source_schemaname,self.source_tablename)
                output_msg   = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, tablename,status, \
                              self.data_path, self.technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
                return error, err_msg, output

        try:
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()

            if conn_source is not None and not conn_source.closed:
                conn_source.close()

            if conn_target is not None:
                conn_target.close()
        except Exception as e:
            error = 21
            err_msg = "Db2FileSync: extract_gp2file_hive[{0}]: Error while closing open DB connections".format(error)
            status = 'Job Error'
            # remove_files(paths, self.source_schemaname, self.source_tablename)
            output_msg = traceback.format_exc()
            audit_logging(None, self.load_id, run_id, self.plant_name, self.system_name, self.job_name,
                          tablename, status, self.data_path, self.technology, rows_inserted, rows_updated, rows_deleted,
                          num_errors, err_msg, 0, 0, output_msg)
            self.error_cleanup(self.source_schemaname, self.source_tablename, run_id, paths, conn_metadata, conn_source, conn_target)
            return error, err_msg, output

        output = {'run_id':run_id, 'v_timestamp':v_timestamp, 'rows_inserted':rows_inserted, 'rows_updated':rows_updated, 'rows_deleted':rows_deleted, 'num_errors':num_errors}

        return error, err_msg, output


    def gp2file(self):
        print "Inside GP2FILE"
        tablename           = self.source_schemaname + "." + self.source_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'GP - File'
        job_name            = 'GP-->File'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0

        # proceed to point everything at the 'branched' resources
        metastore_dbName           = self.config_list['meta_db_dbName']
        dbmeta_Url                 = self.config_list['meta_db_dbUrl']
        dbmeta_User                = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                 = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbtgt_Url                  = self.config_list['tgt_db_dbUrl']
        dbtgt_User                 = self.config_list['tgt_db_dbUser']

        dbsrc_Url                   = self.config_list['src_db_i360_dbUrl']
        dbsrc_User                  = self.config_list['src_db_i360_dbUser']
        dbsrc_dbName                = self.config_list['src_db_i360_dbName']
        dbsrc_Pwd                   = base64.b64decode(self.config_list['src_db_i360_dbPwd'])

        dbtgt_Url                  = self.config_list['tgt_db_i360_dbUrl']
        dbtgt_User                 = self.config_list['tgt_db_i360_dbUser']
        dbtgt_dbName                = self.config_list['tgt_db_i360_dbName']
        dbtgt_Pwd                 = base64.b64decode(self.config_list['tgt_db_i360_dbPwd'])

        data_paths                 = self.config_list['misc_dataPath']
        hdfs_data_path             = self.config_list['misc_hdfsPath']
        data_paths_i360             = self.config_list['misc_dataPathi360']

        source_gpfdist_host         = self.config_list['src_db_i360_gpfdistHost']
        source_gpfdist_port         = self.config_list['src_db_i360_portRange']
        print "GPFDIST DETAILS:" , source_gpfdist_host, source_gpfdist_port


        t                          = datetime.fromtimestamp(time.time())
        v_timestamp                = str(t.strftime('%Y-%m-%d %H:%M:%S'))

        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print "Run ID for the table", tablename , " is : ", run_id
            output = {"run_id" : run_id, "v_timestamp" : v_timestamp, "tablename" : tablename}
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

# Get source database access

        try:
            conn_source, cur_source  = txn_dbConnect(dbsrc_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
        except psycopg2.Error as e:
            error   = 1
            err_msg = "Error connecting to Source Database"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, output

        # Get columns for Writable External Table
        try:
            column_list_sql = "SELECT attrelid::regclass, attnum, attname,format_type(a.atttypid, a.atttypmod) AS data_type FROM pg_attribute a WHERE  attrelid = " \
                              + "'" + self.source_schemaname + "." + self.source_tablename + "'" + "::regclass" \
                                                                                                   " AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
            columns         = dbQuery(cur_source, column_list_sql)
        except psycopg2.Error as e:
            error   = 2
            err_msg = "Error while getting column list for Heap table"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, output

        if not columns:
            err_msg = "Column list not found in pg_attribute system table "
            status = 'Job Error'
            output_msg = "Column list not found in pg_attribute system table "
            print output_msg
            error   = 3
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            return error, err_msg, output

        abs_file_name   = data_paths_i360 + self.source_schemaname + "." + self.source_tablename + ".dat"
        print abs_file_name

        # Remove data files from gpfdist directory
        try:
            os.remove(abs_file_name)
        except OSError:
            pass

        # Drop Writable External Table if it already exists

        try:
            drop_src_table   = "DROP EXTERNAL TABLE IF EXISTS " + self.target_schemaname + "." + self.target_tablename + "_wext"
            cur_source.execute(drop_src_table)
        except psycopg2.Error as e:
            error=4
            err_msg = "Error while dropping writable external table in source"
            print e
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            print sql_state
            print sql_error_msg
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.rollback()
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, output

        # Prepare column list for Writable External Table
        create_list     = ',"'.join(d['attname']+'" '+d['data_type'] for d in columns)
        create_list     = '"' + create_list

        try:
            create_writable_sql     = "CREATE WRITABLE EXTERNAL TABLE " + self.target_schemaname + "." + self.target_tablename + "_wext (" + create_list + ") LOCATION ('gpfdist://" + source_gpfdist_host + ":" + source_gpfdist_port + "/" + self.target_schemaname + "." + self.target_tablename + ".dat"
            create_writable_sql = create_writable_sql + " ') FORMAT 'TEXT' (DELIMITER E'\x01')"
            print create_writable_sql
            cur_source.execute(create_writable_sql)
        except psycopg2.Error as e:
            error=5
            err_msg = "Error while creating writable external table in source"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.rollback()
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, output

        if self.load_type == 'FULL' :
            incremental_sql = "INSERT INTO " + self.target_schemaname + "." + self.target_tablename + "_wext" + " SELECT * FROM " + self.source_schemaname + "." + self.source_tablename
        else:
            incremental_sql = "INSERT INTO " + self.target_schemaname+ "." + self.target_tablename+ "_wext" + " SELECT * FROM " + self.source_schemaname + "." + self.source_tablename \
                              + " WHERE " + self.incremental_column + " > '"  + str(self.last_run_time) + "' AND " + self.incremental_column + " <= '" + v_timestamp + "'"
        try:
            print incremental_sql
            cur_source.execute(incremental_sql)
            rows_inserted = cur_source.rowcount
            print "Rows Inserted : ", rows_inserted
        except psycopg2.Error as e:
            error= 6
            err_msg = "Error while inserting data into writable external table in source"
            status = 'Job Error'
            sql_state = e.pgcode
            sql_error_msg = e.pgerror
            if not sql_state:
                output_msg = sql_state + ':' + sql_error_msg
            else:
                output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.rollback()
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, output

        # Create the Target Table if the Load Type is FULL

        if self.load_type == 'FULL':
            # Get target database access
            try:
                conn_target, cur_target      = dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url, dbtgt_Pwd)
            except psycopg2.Error as e:
                error   = 7
                err_msg = "Error connecting to Target database"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                try:
                    os.remove(abs_file_name)
                except OSError:
                    pass
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_metadata.close()
                return error, err_msg, output

            try:
                create_tgt_table_sql    = "CREATE TABLE " + self.target_schemaname + "." + self.target_tablename + "(" + create_list + ")"
                print create_tgt_table_sql
                cur_target.execute(create_tgt_table_sql)
                conn_target.close()
            except psycopg2.Error as e:
                error   = 8
                if e.pgcode == '42P07':
                    err_msg =  "Table already exists in Target"
                    print err_msg
                    # audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    pass
                else:
                    err_msg = "Error while creating Target Table for FULL Load"
                    print err_msg
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    try:
                        os.remove(abs_file_name)
                    except OSError:
                        pass
                    audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_metadata.close()
                    return error, err_msg, output

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
            try:
                os.remove(abs_file_name)
            except OSError:
                pass
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_target.rollback()
            conn_target.close()
            conn_metadata.close()
            return error, err_msg, output

        conn_metadata.close()
        conn_source.close()
        error =0
        return error, err_msg, output

    def hive2file(self):
        print "Inside Hive2FILE"
        method_name = self.class_name + ": " + "hive2file"
        tablename_src       = self.source_schemaname + "." + self.source_tablename
        tablename           = self.target_schemaname + "." + self.target_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'Hive - File'
        job_name            = 'Hive-->File'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0

        # proceed to point everything at the 'branched' resources
        metastore_dbName            = self.config_list['meta_db_dbName']
        dbmeta_Url                  = self.config_list['meta_db_dbUrl']
        dbmeta_User                 = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbsrc_host                  = self.config_list['src_db_hive_dbUrl']
        dbsrc_host2                 = self.config_list['src_db_hive_dbUrl2']
        dbsrc_Port                  =  self.config_list['src_db_hive_dbPort']
        dbsrc_Auth                  = self.config_list['src_db_hive_authMech']

        data_stg_hive               = self.config_list['misc_hdfsStagingPath']

        t                           = datetime.fromtimestamp(time.time())
        v_timestamp                 = str(t.strftime('%Y-%m-%d %H:%M:%S'))
        env                         = self.config_list['env']
        s3_bucket_name              = self.config_list['s3_bucket_name']
        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print "Run ID for the table", tablename , " is : ", run_id
            output = {"run_id" : run_id, "v_timestamp" : v_timestamp, "tablename" : tablename}
        except Exception as e:
            err_msg = "Error connecting to database while fetching  metadata"
            error   = 1
            print e
            return error, err_msg, output

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

        # Get access to source database .... Hive
        try:
            conn_source, cur_source   = dbConnectHive(dbsrc_host, dbsrc_Port, dbsrc_Auth)
            if self.log_mode == 'DEBUG':
                status  = "Connected to Source Database"
                print status
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, \
                              self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        except Exception as e:
            try:
                conn_source, cur_source   = dbConnectHive(dbsrc_host2, dbsrc_Port, dbsrc_Auth)
            except Exception as e:
                error = 15
                err_msg      = method_name + "[{0}]: Error while connecting to source database".format(error)
                status       = 'Job Error'
                # print (print_hdr + "Exception: ", e)
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, system_name, job_name, tablename,status, \
                              self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                return error, err_msg, output

        # Clear staging directory before dumping data
        if self.data_path == 'Hive2RDS' or self.data_path == 'Hive2PREDIX':
            try:
                shutil.rmtree(data_stg_hive + "/" + self.data_path.lower() + "/" + self.target_schemaname + "/" + self.target_tablename)
                if self.log_mode == 'DEBUG':
                    print "Removed Files"
            except Exception as e:
                if e.errno == 2:
                    if self.log_mode == 'DEBUG':
                        print "File/Directory does not exist"
                    pass
                else:
                    print e
                    error = 2
                    err_msg = "Error while initially clearing staging directory"
                    status = "Job Error"
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, system_name, job_name, tablename,status, \
                                  self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    return error, err_msg, output


            try:
                data_path_dir = data_stg_hive + "/" + self.data_path.lower()
                print "Data Path Directory : ", data_path_dir
                data_path_dir_check = glob.glob(data_path_dir)
                if len(data_path_dir_check) == 0:
                    (ret, out, err) = run_cmd(['mkdir',data_path_dir])
                    if err:
                        error= 4
                        err_msg = "Error while creating data_path directory in Local FS"
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        print output_msg
                        audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                        conn_source.close()
                        conn_metadata.close()
                        return error, err_msg, output

                schema_dir = data_path_dir + "/" + self.target_schemaname
                print "Schema Directory : ", schema_dir
                schema_dir_check = glob.glob(schema_dir)
                if len(schema_dir_check) == 0:
                    (ret, out, err) = run_cmd(['mkdir',schema_dir])
                    if err:
                        error= 4
                        err_msg = "Error while creating schema directory in Local FS"
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        print output_msg
                        audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                        conn_source.close()
                        conn_metadata.close()
                        return error, err_msg, output

                table_dir = schema_dir + "/" + self.target_tablename
                print "Table Directory : ", table_dir
                table_dir_check = glob.glob(table_dir)
                if len(table_dir_check) == 0:
                    (ret, out, err) = run_cmd(['mkdir',table_dir])
                    if err:
                        error= 4
                        err_msg = "Error while creating table directory in Local FS"
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        print output_msg
                        audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                        conn_source.close()
                        conn_metadata.close()
                        return error, err_msg, output
            except Exception as e:
                error= 5
                err_msg = "Error while creating directory in Local FS"
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_source.close()
                conn_metadata.close()
                return error, err_msg, output
        elif self.data_path == 'Hive2S3':
            table_dir     = "s3://" + s3_bucket_name + "/sisense/" + self.target_tablename
            print "Table Directory for S3 : ", table_dir
            (ret, out, err) = run_cmd(['aws','s3','rm', table_dir, '--recursive'])
            if err:
                error= 4
                err_msg = "Error while creating table directory in Local FS"
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_source.close()
                conn_metadata.close()
                return error, err_msg, output

# Unload data from Hive

        if self.load_type == 'FULL':
            incremental_sql = "INSERT OVERWRITE DIRECTORY 'hdfs://" + data_stg_hive + "/" + self.data_path.lower() + "/" + self.target_schemaname + "/" \
                              + self.target_tablename + "' ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' ESCAPED BY '\\\\'            \
                              LINES TERMINATED BY '\n' SELECT * FROM " + tablename_src
        else:
            incremental_sql = "INSERT OVERWRITE DIRECTORY 'hdfs://" + data_stg_hive + "/" + self.data_path.lower() + "/" + self.target_schemaname + "/" \
                              + self.target_tablename + "' ROW FORMAT DELIMITED  FIELDS TERMINATED BY ','  ESCAPED BY '\\\\'       \
                              LINES TERMINATED BY '\n' SELECT * FROM " + tablename_src + " WHERE " + self.incremental_column \
                              + " > '"  + str(self.last_run_time)  + "' AND " + self.incremental_column + " <= '" + v_timestamp + "'"
        print incremental_sql
        try:
            cur_source.execute(incremental_sql)
        except Exception as e:
            error= 3
            err_msg = "Error while writing data in HDFS directory"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_source.close()
            conn_metadata.close()
            return error, err_msg, output

        if self.data_path == 'Hive2RDS' or self.data_path == 'Hive2PREDIX':
            print "Moving Files to Local FS....."
            try:
                hdfs_table_dir = data_stg_hive + "/" + self.data_path.lower() + "/" + self.target_schemaname + "/" + self.target_tablename + "/*"
                print "HDFS Table Directory Path : ", hdfs_table_dir
                print "LOCAL FS Table Directory Path : ", table_dir
                (ret, out, err) = run_cmd(['hadoop','fs','-copyToLocal',hdfs_table_dir, table_dir])
                if err:
                    error= 4
                    err_msg = "Error while moving file from HDFS to Local FS"
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_source.close()
                    conn_metadata.close()
                    return error, err_msg, output
            except Exception as e:
                error= 6
                err_msg = "Error while moving file from HDFS to Local FS"
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_source.close()
                conn_metadata.close()
                return error, err_msg, output
        elif self.data_path == 'Hive2S3':
            print "Moving Files to S3 FS....."
            try:
                hdfs_table_dir = data_stg_hive + "/" + self.data_path.lower() + "/" + self.target_schemaname + "/" + self.target_tablename
                print "HDFS Table Directory Path : ", hdfs_table_dir
                print "S3 FS Table Directory Path : ", table_dir
                (ret, out, err) = run_cmd(['hadoop','distcp',('hdfs://getnamenode:8020/' + hdfs_table_dir), ('s3a://' + s3_bucket_name + "/sisense/" + self.target_tablename)])
                if err and ret > 0:
                    error= 4
                    err_msg = "Error while moving file from HDFS to S3 FS"
                    print err
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                    conn_source.close()
                    conn_metadata.close()
                    return error, err_msg, output
            except Exception as e:
                error= 6
                err_msg = "Error while moving file from HDFS to Local FS"
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_source.close()
                conn_metadata.close()
                return error, err_msg, output

        if self.data_path == 'Hive2S3':
            try:
                update_control_info_sql = "UPDATE sync.control_table set last_run_time = '" + v_timestamp + "' where id = " + str(self.id) + " AND source_schemaname = '" + self.source_schemaname + "' AND source_tablename = '" + self.source_tablename + "' AND data_path = '"+ self.data_path +"'"
                print update_control_info_sql
                cur_metadata.execute(update_control_info_sql)
            except psycopg2.Error as e:
                print e
                error   = 7
                err_msg = "Error while updating the control table"
                print err_msg
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
                conn_metadata.close()
                return error, err_msg, output


# Final log entry
        try:
            error= 0
            err_msg     = 'No Errors'
            status      = 'Job Finished'
            output_msg  = 'Extraction from Hive finished successfully'
            print output_msg
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,source_row_count,target_row_count,output_msg)
        except psycopg2.Error as e:
            error= 4
            err_msg = "Error while dropping external table in target"
            print err_msg
            status = 'Job Error'
            output_msg = traceback.format_exc()
            print output_msg
            try:
                shutil.rmtree(data_stg_hive+"/"+self.source_schemaname+"/"+self.source_tablename)
            except OSError:
                pass
            audit_logging(cur_metadata, self.load_id,  run_id, plant_name, system_name, job_name, tablename,status, self.data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.close()
            return error, err_msg, output

        conn_metadata.close()
        conn_source.close()
        return error, err_msg, output

    def sqoop2hdfs(self):
        print "Inside Sqoop --> HDFS"
        method_name = self.class_name + ": " + "hive2file"
        tablename           = self.target_schemaname + "." + self.target_tablename
        run_id_sql          = "select nextval('sync.datasync_seq')"
        plant_name          = 'DATASYNC'
        system_name         = 'SRC - HDFS'
        job_name            = 'SRC-->HDFS'
        technology          = 'Python'
        num_errors          = 0
        source_row_count    = 0
        target_row_count    = 0
        app_name            = "DataSync_SQOOP2Hive-" + self.target_schemaname + "." + self.target_tablename

        # proceed to point everything at the 'branched' resources
        metastore_dbName            = self.config_list['meta_db_dbName']
        dbmeta_Url                  = self.config_list['meta_db_dbUrl']
        dbmeta_User                 = self.config_list['meta_db_dbUser']
        dbmeta_Pwd                  = base64.b64decode(self.config_list['meta_db_dbPwd'])

        dbsrc_host                  = self.config_list['src_db_hive_dbUrl']
        dbsrc_host2                 = self.config_list['src_db_hive_dbUrl2']
        dbsrc_Port                  =  self.config_list['src_db_hive_dbPort']
        dbsrc_Auth                  = self.config_list['src_db_hive_authMech']

        data_stg_hive               = self.config_list['misc_hdfsStagingPath']

        t                           = datetime.fromtimestamp(time.time())
        v_timestamp                 = str(t.strftime('%Y-%m-%d %H:%M:%S'))
        env                         = self.config_list['env']
        s3_bucket_name              = self.config_list['s3_bucket_name']
        edl_private_key             = base64.b64decode(self.config_list['misc_edlPrivateKey'])
        oracle_sqoop_jar            = self.config_list['sqoop_libjars_ora_jars']
        sql_server_sqoop_jar        = self.config_list['sqoop_libjars_sql_jars']

        dbUrl                       = self.config_list['mysql_dbUrl']
        dbUser                      = self.config_list['mysql_dbUser']
        dbPwd                       = base64.b64decode(self.config_list['mysql_dbPwd'])
        dbMetastore_dbName          = self.config_list['mysql_dbMetastore_dbName']
        dbApp_dbName                = self.config_list['mysql_dbApp_dbName']

        try:
            conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
            run_id_lists                   = dbQuery(cur_metadata,run_id_sql)
            run_id_list                    = run_id_lists[0]
            run_id                         = run_id_list['nextval']
            print "Run ID for the table", tablename , " is : ", run_id
            output = {"run_id" : run_id, "v_timestamp" : v_timestamp, "tablename" : tablename}
        except Exception as e:
            err_msg = "Error connecting to database while fetching  metadata"
            error   = 1
            print e
            return error, err_msg, output

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

# Getting the source connection details from the control table in database
        src_conn_sql = "SELECT system_name, host_name, database, port, user_name, sbdt.edl_decrypt_password(system_name, '" + edl_private_key + "') as pgp_pass " \
                      "FROM sbdt.edl_connection c " \
                       "WHERE LOWER(system_name) = '" + str(self.system_name_ct) + "'"
        src_conn_det = dbQuery(cur_metadata, src_conn_sql)

        # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "results: ", conn_results)

        if len(src_conn_det) == 0:
            error = 2
            status = 'Job Error'
            err_msg = method_name + "[{0}]: No connnection details available for schema: ".format(error) + self.system_name_ct+ " in edl connection table"
            output_msg = "No connnection details available for schema: " + self.system_name_ct + " in edl connection table"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + output_msg)
            audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors, err_msg, 0,0, output_msg)
            return error, err_msg, output_msg
        else:
            self.src_host   = src_conn_det[0]['host_name']
            self.src_port   = src_conn_det[0]['port']
            self.src_db     = src_conn_det[0]['database']
            self.src_user   = src_conn_det[0]['user_name']
            self.src_pass   = src_conn_det[0]['pgp_pass']

################################################################################################################################################################
# This is for getting the select list that would fired along with the Sqoop command to the Source system                                                       #
# In general, connect to the target database to fetch the column list                                                                                          #
# For reliance there are special characters in the table names and the column names like $. For this, we are connecting to the source to get the column list   #
################################################################################################################################################################
            if self.target_schemaname in ['reliance','ncmr','gets_rin']:
                try:
                    conn_src, cur_src     = dbConnectOra(self.src_host,self.src_port, self.src_db, self.src_user, self.src_pass)
                except Exception as e:
                    error = 5
                    err_msg = method_name + "[{0}]: Error connecting to Oracle DB".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,err_msg, 0, 0, output_msg)
                    return error, err_msg, output_msg
                try:
                    if self.target_schemaname in ['gets_rin']:
                        sql_query = "select " \
                                    "CASE WHEN DATA_TYPE IN ('VARCHAR', 'VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR') THEN 'translate('||COLUMN_NAME||', ' ||'chr(10)||chr(11)||chr(13), chr(32))'  " \
                                    "ELSE COLUMN_NAME END " \
                                    "FROM " \
                                    "all_tab_cols " \
                                    "WHERE " \
                                    "TABLE_NAME = '" + self.source_tablename.upper() + "' " \
                                    "AND OWNER = '" + self.source_schemaname.upper() + "' " \
                                    "AND HIDDEN_COLUMN = 'NO' " \
                                    "ORDER BY COLUMN_ID  "
                    else:
                        sql_query = "select " \
                                    "CASE WHEN DATA_TYPE IN ('VARCHAR', 'VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR') THEN 'translate('||COLUMN_NAME||', ' ||'chr(10)||chr(11)||chr(13), chr(32))'  " \
                                    "ELSE COLUMN_NAME END " \
                                    "FROM " \
                                    "all_tab_cols " \
                                    "WHERE " \
                                    "TABLE_NAME = '" + self.source_tablename.upper() + "' " \
                                    "AND OWNER = '" + self.source_schemaname.upper() + "' " \
                                    "AND USER_GENERATED = 'YES' " \
                                    "AND HIDDEN_COLUMN = 'NO' " \
                                    "ORDER BY COLUMN_ID  "
                    print sql_query
                    cur_src.execute(sql_query)
                    target_result = cur_src.fetchall()
                except Exception as e:
                    error = 6
                    err_msg = method_name + "[{0}]: Issue running SQL in Oracle Meta tables:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,err_msg, 0, 0, output_msg)
                    return error, err_msg, output_msg
                finally:
                    conn_src.close()

            elif self.target_schemaname in ['adw', 'ats', 'erp_oracle', 'eservice', 'get_dm', 'gets_msa', 'nucleus', 'odw', 'odw_dyn_prcg', 'pan_ins_erp', 'sas_adw', 'scp', 'sdr', 'teamcenter', 'todw', 'tscollp', 'rmd']:
                # Connection to the Hive Metastore to get column list
                try:
                    connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
                    print "Connected Successfully to Hive Metastore"
                except Exception as e:
                    error = 5
                    err_msg = method_name + "[{0}]: Error connecting to Hive Metastore".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,err_msg, 0, 0, output_msg)
                    return error, err_msg, output_msg

                # Establish connection to the hive metastore to get the list of columns
                try:
                    cursor = connection.cursor()
                    print "Got cursor"

                    sql_query   =   "SELECT " \
                                    "CASE " \
                                    "WHEN c.TYPE_NAME in ('string') THEN concat('translate(',UPPER(c.COLUMN_NAME),'\, chr(10)||chr(11)||chr(13)\, chr(32))') " \
                                    "ELSE UPPER(c.COLUMN_NAME) " \
                                    "END as COLUMN_NAME " \
                                    "FROM TBLS t " \
                                    "JOIN DBS d " \
                                    "ON t.DB_ID = d.DB_ID " \
                                    "JOIN SDS s " \
                                    "ON t.SD_ID = s.SD_ID " \
                                    "JOIN COLUMNS_V2 c " \
                                    "ON s.CD_ID = c.CD_ID " \
                                    "WHERE " \
                                    "TBL_NAME = " + "'" + self.target_tablename + "' "                \
                                    "AND d.NAME=" + " '" + self.target_schemaname + "' " \
                                    "AND c.COLUMN_NAME NOT IN ('edl_is_deleted','edl_last_updated_date','hive_updated_by','hive_updated_date') " \
                                    "ORDER by c.INTEGER_IDX "
                    print ( "hive_meta_target_sql: " + sql_query)
                    cursor.execute(sql_query)
                    target_result = cursor.fetchall()
                    # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "target_result:", target_result)
                except Exception as e:
                    error = 6
                    err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,err_msg, 0, 0, output_msg)
                    return error, err_msg, output_msg
                finally:
                    connection.close()

            elif self.target_schemaname in ['cas', 'erp_sqlwh', 'pan_ins', 'shop_supt','srs','ras_audit']:
                # Connection to the Hive Metastore to get column list
                try:
                    connection = mysql.connector.connect(user=dbUser, password=dbPwd, host=dbUrl, database=dbMetastore_dbName)
                    print "Connected Successfully to Hive Metastore"
                except Exception as e:
                    error = 5
                    err_msg = method_name + "[{0}]: Error connecting to Hive Metastore".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,err_msg, 0, 0, output_msg)
                    return error, err_msg, output_msg

                # Establish connection to the hive metastore to get the list of columns
                try:
                    cursor = connection.cursor()

                    sql_query   =   "SELECT " \
                                    "CASE " \
                                    "WHEN c.TYPE_NAME in ('string') THEN concat('replace(replace(replace(\"',UPPER(c.COLUMN_NAME),'\"\, CHAR(10), CHAR(32)),CHAR(11),CHAR(32)),CHAR(13),CHAR(32)) AS \"', UPPER(c.COLUMN_NAME),'\"') " \
                                    "ELSE UPPER(c.COLUMN_NAME) " \
                                    "END as COLUMN_NAME " \
                                    "FROM TBLS t " \
                                    "JOIN DBS d " \
                                    "ON t.DB_ID = d.DB_ID " \
                                    "JOIN SDS s " \
                                    "ON t.SD_ID = s.SD_ID " \
                                    "JOIN COLUMNS_V2 c " \
                                    "ON s.CD_ID = c.CD_ID " \
                                    "WHERE " \
                                    "TBL_NAME = " + "'" + self.target_tablename + "' "                \
                                    "AND d.NAME=" + " '" + self.target_schemaname + "' " \
                                    "AND c.COLUMN_NAME NOT IN ('edl_is_deleted','edl_last_updated_date','hive_updated_by','hive_updated_date') " \
                                    "ORDER by c.INTEGER_IDX "
                    print ( "hive_meta_target_sql: " + sql_query)
                    cursor.execute(sql_query)
                    target_result = cursor.fetchall()
                    # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "target_result:", target_result)
                except Exception as e:
                    error = 6
                    err_msg = method_name + "[{0}]: Issue running SQL in hive metadata database:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,err_msg, 0, 0, output_msg)
                    return error, err_msg, output_msg
                finally:
                    connection.close()

            target_select_list  = ', '.join(map(''.join, target_result))
#########################################################################################################
# When firing the Sqoop command with a SQL query, it is mandatory to have a --split-by column           #
# We are using join_columns for this purpose                                                            #
# Some tables might not have the join_columns (null), picking up the first column in that case          #
#########################################################################################################
            # backup_split_by_col = target_select_list.split(",")[0]
            # print "TARGET SELECT LIST :",  target_select_list
            # print "BACKUP SPLIT BY COLUMN : ", backup_split_by_col

        if self.target_schemaname in ['reliance','adw', 'ats', 'erp_oracle', 'eservice', 'get_dm', 'gets_msa', 'gets_rin', 'ncmr', 'nucleus', 'odw', 'odw_dyn_prcg', 'pan_ins_erp', 'sas_adw', 'scp', 'sdr', 'teamcenter', 'todw', 'tscollp', 'rmd']:
            try:
                connect_parm        = "jdbc:oracle:thin:@" + str(self.src_host) + ":" + str(self.src_port) + "/" + self.src_db
                sqoop_jar           = oracle_sqoop_jar
                username_parm       = "\"" + self.src_user + "\""
                password_parm       = "\"" + self.src_pass + "\""
                output_file_type_parm           = "--as-textfile"
                output_file_delim_parm          = "|"
                output_file_esc_parm            = "\\"
                target_dir_parm     = "hdfs://getnamenode/apps/staging/" + self.target_schemaname + "/" + self.target_tablename + "_ext_talend"
                num_mappers_parm     = '8'
                fetch_size_parm     = '5000'

                if self.custom_sql:
                    self.custom_sql = self.custom_sql.lower()
                    print "CUSTOM SQL = ", self.custom_sql
                    if 'select *' in self.custom_sql:
                        print "Found SELECT *"
                        custom_sql_list = self.custom_sql.split()
                        new_custom_sql_list = [target_select_list if x == '*' else x for x in custom_sql_list]
                        tmp_custom_sql    = '   '.join(new_custom_sql_list)
                        if tmp_custom_sql.lower().find('where') <> -1:
                            # final_custom_sql    = "\"" + tmp_custom_sql + " AND $CONDITIONS \""
                            final_custom_sql = tmp_custom_sql + " AND $CONDITIONS "
                        else:
                            # final_custom_sql = "\"" + tmp_custom_sql + " WHERE $CONDITIONS \""
                            final_custom_sql = tmp_custom_sql + " WHERE $CONDITIONS "
                        print "FINAL CUSTOM SQL :", final_custom_sql
                    elif 'case' in self.custom_sql.lower():
                        error = 7
                        err_msg = "Found Case statement.. Your custom_sql might be too complex.. Try to write a Sqoop and DataSync friendly custom_sql"
                        output = "Complex Custom_SQL"
                        print err_msg
                        final_custom_sql =  self.custom_sql.upper()
                        # return error, err_msg, output
                    # else:
                    #     print "Comma Split"
                    #     custom_sql_list = self.custom_sql.split(",")
                    else:
                        final_custom_sql = self.custom_sql.upper()

                    query_parm = final_custom_sql.upper()
                else:
                #print "CUSTOM SQL LIST :", custom_sql_list
                    query_parm = "\"SELECT " + target_select_list + " FROM " + self.source_schemaname.upper() + "." + self.source_tablename.upper()

                if self.load_type == 'INCREMENTAL' or self.load_type == 'APPEND_ONLY':
                    incremental_parm = str(self.incremental_column.upper()) + " > TO_TIMESTAMP('" + str(self.last_run_time) + "','YYYY-MM-DD HH:MI:SS') AND "
                elif self.load_type =='FULL':
                    incremental_parm = " "

                if self.custom_sql:
                    conditions_parm = " "
                else:
                    conditions_parm = "WHERE " + incremental_parm + "$CONDITIONS \""

            except Exception as e:
                error = 7
                err_msg = method_name + "[{0}]: Error while creating SQOOP command for Source System ORACLE:".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                              self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                              err_msg, 0, 0, output_msg)
                return error, err_msg, output_msg

            if self.join_columns:
                if len(self.join_columns.split(",")) == 1:
                    split_by_parm       = self.join_columns.upper()
                elif len(self.join_columns.split(",")) > 1:
                    split_by_parm       = self.join_columns.split(",")[0].upper() # Picking up the first column of the multiple join columns. Not checking if the column type is string which will cause failures
                (ret, out, err) = run_cmd(['/bin/sqoop', 'import', '-D', ('mapred.job.name='+app_name),'--libjars', sqoop_jar, '--connect', connect_parm, '--username',username_parm, '--password', password_parm, output_file_type_parm, '--fields-terminated-by',output_file_delim_parm, \
                     '--escaped-by', output_file_esc_parm, '--delete-target-dir', '--target-dir', target_dir_parm,'-m', num_mappers_parm, '--direct', '--fetch-size', fetch_size_parm, '--split-by',split_by_parm, '--query', (query_parm + " " + conditions_parm)])
                if ret > 0:
                    error = 8
                    err_msg = method_name + "[{0}]: Error while Sqooping data from Source System ORACLE:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                                  self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                                  err_msg, 0, 0, output_msg)
                    return error, err_msg, (out+err)

            else:
                num_mappers_parm = '1'
                (ret, out, err) = run_cmd(['/bin/sqoop', 'import', '-D', ('mapred.job.name='+app_name),'--libjars', sqoop_jar, '--connect', connect_parm, '--username',username_parm, '--password', password_parm, output_file_type_parm, '--fields-terminated-by',output_file_delim_parm, \
                     '--escaped-by', output_file_esc_parm, '--delete-target-dir', '--target-dir', target_dir_parm,'-m', num_mappers_parm, '--direct', '--fetch-size', fetch_size_parm, '--query', (query_parm + " " + conditions_parm)])
                if ret > 0:
                    error = 9
                    err_msg = method_name + "[{0}]: Error while Sqooping data from Source System ORACLE:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                                  self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                                  err_msg, 0, 0, output_msg)
                    return error, err_msg, (out+err)

        elif self.target_schemaname in ['cas', 'erp_sqlwh', 'pan_ins', 'shop_supt','srs','ras_audit']:
            try:
                connect_parm = "jdbc:sqlserver://" + str(self.src_host) + ":" + str(self.src_port) + ";database=" + self.src_db
                sqoop_jar = sql_server_sqoop_jar
                username_parm = "\"" + self.src_user + "\""
                password_parm = "\"" + self.src_pass + "\""
                output_file_type_parm = "--as-textfile"
                output_file_delim_parm = "|"
                output_file_esc_parm = "\\"
                target_dir_parm = "hdfs://getnamenode/apps/staging/" + self.target_schemaname + "/" + self.target_tablename + "_ext_talend"
                num_mappers_parm = '8'
                fetch_size_parm = '5000'

                if self.custom_sql:
                    self.custom_sql = self.custom_sql.lower()
                    print "CUSTOM SQL = ", self.custom_sql
                    if 'select *' in self.custom_sql:
                        print "Found SELECT *"
                        custom_sql_list = self.custom_sql.split()
                        new_custom_sql_list = [target_select_list if x == '*' else x for x in custom_sql_list]
                        tmp_custom_sql    = '   '.join(new_custom_sql_list)
                        if tmp_custom_sql.lower().find('where') <> -1:
                            # final_custom_sql    = "\"" + tmp_custom_sql + " AND $CONDITIONS \""
                            final_custom_sql = tmp_custom_sql + " AND $CONDITIONS "
                        else:
                            # final_custom_sql = "\"" + tmp_custom_sql + " WHERE $CONDITIONS \""
                            final_custom_sql = tmp_custom_sql + " WHERE $CONDITIONS "
                        print "FINAL CUSTOM SQL :", final_custom_sql
                    elif 'case' in self.custom_sql.lower():
                        error = 7
                        err_msg = "Found Case statement.. Your custom_sql might be too complex.. Try to write a Sqoop and DataSync friendly custom_sql"
                        output = "Complex Custom_SQL"
                        print err_msg
                        final_custom_sql =  self.custom_sql.upper()
                        # return error, err_msg, output
                    # else:
                    #     print "Comma Split"
                    #     custom_sql_list = self.custom_sql.split(",")
                    else:
                        final_custom_sql = self.custom_sql.upper()

                    query_parm = final_custom_sql.upper()
                else:
                #print "CUSTOM SQL LIST :", custom_sql_list
                    query_parm = "\"SELECT " + target_select_list + " FROM " + self.source_schemaname.upper() + "." + self.source_tablename.upper()

                if self.load_type == 'INCREMENTAL' or self.load_type == 'APPEND_ONLY':
                    incremental_parm = str(self.incremental_column.upper()) + " > TO_TIMESTAMP('" + str(self.last_run_time) + "','YYYY-MM-DD HH:MI:SS') AND "
                else:
                    incremental_parm = " "
                conditions_parm = "WHERE " + incremental_parm + "$CONDITIONS \""

                if self.custom_sql:
                    conditions_parm = " "
                else:
                    conditions_parm = "WHERE " + incremental_parm + "$CONDITIONS \""
                    
            except Exception as e:
                error = 7
                err_msg = method_name + "[{0}]: Error while Sqooping data from Source System MSSQL:".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                              self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                              err_msg, 0, 0, output_msg)
                return error, err_msg, output_msg
            if self.join_columns:
                if len(self.join_columns.split(",")) == 1:
                    split_by_parm = self.join_columns.upper()
                elif len(self.join_columns.split(",")) > 1:
                    split_by_parm = self.join_columns.split(",")[0].upper()  # Picking up the first column of the multiple join columns. Not checking if the column type is string which will cause failures
                (ret, out, err) = run_cmd(['/bin/sqoop', 'import', '-D', ('mapred.job.name='+app_name),'--libjars', sqoop_jar, '--connect', connect_parm, '--username', username_parm,'--password', password_parm, output_file_type_parm, '--fields-terminated-by', output_file_delim_parm, \
                     '--escaped-by', output_file_esc_parm, '--delete-target-dir', '--target-dir', target_dir_parm, '-m',num_mappers_parm, '--direct', '--fetch-size', fetch_size_parm, '--split-by', split_by_parm, '--query',(query_parm + " " + conditions_parm)])
                if ret > 0:
                    error = 9
                    err_msg = method_name + "[{0}]: Error while Sqooping data from Source System MSSQL:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                                  self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                                  err_msg, 0, 0, output_msg)
                    return error, err_msg, (out+err)
            else:
                num_mappers_parm = '1'
                (ret, out, err) = run_cmd(['/bin/sqoop', 'import', '-D', ('mapred.job.name='+app_name), '--libjars', sqoop_jar, '--connect', connect_parm, '--username', username_parm,'--password', password_parm, output_file_type_parm, '--fields-terminated-by', output_file_delim_parm, \
                     '--escaped-by', output_file_esc_parm, '--delete-target-dir', '--target-dir', target_dir_parm, '-m',num_mappers_parm, '--direct', '--fetch-size', fetch_size_parm, '--query',(query_parm + " " + conditions_parm)])
                if ret > 0:
                    error = 9
                    err_msg = method_name + "[{0}]: Error while Sqooping data from Source System ORACLE:".format(error)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    audit_logging(cur_metadata, self.load_id, run_id, plant_name, system_name, job_name, tablename, status,
                                  self.data_path, technology, rows_inserted, rows_updated, rows_deleted, num_errors,
                                  err_msg, 0, 0, output_msg)
                    return error, err_msg, (out+err)

        conn_metadata.close()
        return 0, 'No Error', (out+err)
if __name__ == "__main__":
    print "ERROR: Direct execution on this file is not allowed"


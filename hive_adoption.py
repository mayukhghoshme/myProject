import datetime
import sys
sys.path.append("/apps/common")
from utils import run_cmd,load_config,dbConnect, dbQuery, move_files
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

class FStoHDFS(object):
    def __init__(self):
        print "Inside Class FS --> HDFS"
        self.config_list            = load_config()
        self.paths                  = self.config_list['hive_log_hiveLogPath']
        self.staging_path           = self.config_list['misc_hdfsStagingPath']
        self.metastore_dbName       = self.config_list['meta_db_dbName']
        self.dbmeta_Url             = self.config_list['meta_db_dbUrl']
        self.dbmeta_User            = self.config_list['meta_db_dbUser']
        self.dbmeta_Pwd             = base64.b64decode(self.config_list['meta_db_dbPwd'])
        # self.local_inprogress_path  = self.config_list['hive_log_local_inprogress_path']
        self.hive_hosts             = self.config_list['hive_log_hosts']
        self.local_staging_path     = self.config_list['hive_log_local_staging_path']
        self.yarn_host              = self.config_list['yarn_host']
        env                         = self.config_list['env']

        kinit_cmd                   = "/usr/bin/kinit"
        kinit_args                  = "-kt"
        keytab                      = "/home/hdp/gphdfs.service.keytab"
        if env == 'prod':
            realm                   = 'gpadmin@TRANSPORTATION-HDPPROD.GE.COM'
        elif env == 'dev':
            realm                   = 'gpadmin@TRANSPORTATION-HDPDEV.GE.COM'
        else:
            print "Environment variable not found ! "
        try:
            java_home = os.environ["JAVA_HOME"]
        except KeyError:
            java_home = "/usr/lib/jvm/java"
            os.environ["JAVA_HOME"] = java_home
        try:
            print kinit_cmd, kinit_args, keytab, realm
            subprocess.call([kinit_cmd, kinit_args, keytab, realm])
        except Exception as e:
            print e
            err_msg = "Error while doing kinit"
            return err_msg
    def fs2hdfs_hive_log(self):
            hosts = []
            # Get information about the table to load
            try:
                metadata_sql = "SELECT * FROM sync.control_table \
                        WHERE target_tablename = 'hive_log_ext' \
                            AND target_schemaname = 'default'" + " \
                            AND data_path = " + "'FS2HDFS'"

                print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "metadata_sql: " + metadata_sql)
                conn_metadata, cur_metadata = dbConnect(self.metastore_dbName, self.dbmeta_User, self.dbmeta_Url, self.dbmeta_Pwd)
                print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "before connecting to metastore controls")
                controls = dbQuery(cur_metadata, metadata_sql)
                # print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "metastore controls:", controls)
            except psycopg2.Error as e:
                error = 2
                err_msg = "Error connecting to control table database".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                return output_msg
                sys.exit(error)
            finally:
                conn_metadata.close()


            if not controls:
                error = 3
                err_msg = "No Entry found in control table".format(error)
                status = 'Job Error'
                output_msg = "No Entry found in control table"
                return output_msg
                sys.exit(error)

            self.id                     = str(controls[0]['id'])
            self.source_schema          = str(controls[0]['source_schemaname'])
            self.source_tablename       = str(controls[0]['source_tablename'])
            self.target_schema          = str(controls[0]['target_schemaname'])
            self.target_tablename       = str(controls[0]['target_tablename'])
            partitioned                 = controls[0]['is_partitioned']
            self.load_type              = str(controls[0]['load_type'])
            self.s3_backed              = controls[0]['s3_backed']
            first_partitioned_column    = str(controls[0]['first_partitioned_column'])
            second_partitioned_column   = str(controls[0]['second_partitioned_column'])
            partitioned_column_transformation = str(controls[0]['partition_column_transformation'])
            custom_sql                  = str(controls[0]['custom_sql'])
            self.join_columns           = str(controls[0]['join_columns'])
            self.archived_enabled       = controls[0]['archived_enabled']
            distribution_columns        = str(controls[0]['distribution_columns'])
            dist_col_transformation     = str(controls[0]['dist_col_transformation'])
            self.log_mode               = str(controls[0]['log_mode'])
            self.last_run_time          = str(controls[0]['last_run_time'])

            incoming_path               = self.paths + "/hiveserver2.log"
            local_inprogress_path       = self.local_staging_path + "/in_progress/"
            inprogress_path             = self.staging_path + self.target_schema + "/" + self.target_tablename  + "/in_progress/"
            hosts                       = self.hive_hosts.split(',')
            print hosts
# Creating the Local in_progress and/or clearing that location for new incoming files
            for host in hosts:
                print "Inside Host path check"
                path_to_check           = self.local_staging_path + host
                print path_to_check
                path_check              = glob.glob(path_to_check)
                print path_check
                if len(path_check) > 0:
                    print "Path exists... Clearing the directory"
                    (ret, out, err) = run_cmd(['rm', '-rf', (path_to_check)])
                    print (ret, out, err)
                    if ret:
                        error = 1
                        err_msg = "Error while cleaning in_progress location in Local FS".format(error)
                        print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                        status = 'Job Error'
                        output_msg = traceback.format_exc()
                        print output_msg
                        sys.exit(error)
                        return output_msg

                (ret, out, err) = run_cmd(['mkdir', '-p', path_to_check])
                if ret:
                    error = 1
                    err_msg = "Error while creating in_progress location in Local FS".format(error)
                    print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    sys.exit(error)
                    return output_msg

            path_check              = glob.glob(local_inprogress_path)
            if len(path_check) > 0:
                print "Path exists... Clearing the directory"
                (ret, out, err) = run_cmd(['rm', '-rf', (local_inprogress_path)])
                print (ret, out, err)
                if ret:
                    error           = 1
                    err_msg         = "Error while cleaning in_progress location in Local FS".format(error)
                    print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                    status          = 'Job Error'
                    output_msg      = traceback.format_exc()
                    print output_msg
                    sys.exit(error)
                    return output_msg
            (ret, out, err)     = run_cmd(['mkdir', '-p',local_inprogress_path])
            if ret:
                error           = 1
                err_msg         = "Error while creating in_progress location in Local FS".format(error)
                print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                status          = 'Job Error'
                output_msg      = traceback.format_exc()
                print output_msg
                sys.exit(error)
                return output_msg

 # Creating the HDFS in_progress location and/or clearing that location for new incoming files
            (ret, out, err) = run_cmd(["hadoop", "fs", "-test", "-e", inprogress_path])
            if ret:
                print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "Directory does not exist ... Creating...")
                (ret, out, err) = run_cmd(["hadoop", "fs", "-mkdir", "-p", inprogress_path])
                if ret:
                    error = 1
                    err_msg = "Error while creating in_progress location in HDFS".format(error)
                    print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print output_msg
                    sys.exit(error)
                    return output_msg
            # else:
            #     (ret, out, err) = run_cmd(["hadoop", "fs", "-rm", "-r", inprogress_path + "*"])
            #     if ret:
            #         if err.find("No such file or directory") <> -1:
            #             pass
            #         else:
            #             error = 1
            #             err_msg = "Error while cleaning in_progress location in HDFS".format(error)
            #             print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
            #             status = 'Job Error'
            #             output_msg = traceback.format_exc()
            #             print output_msg
            #             return output_msg

# Checking the last run time of the table.
# Bringing the files from each host since the last run time
            from datetime import date, timedelta
            if self.last_run_time == 'None':
                self.last_run_time = str(datetime.now())
            print "Last Run Time : ", self.last_run_time
            lr_dt, lr_ts = self.last_run_time.split()
            lr_dt = datetime.strptime(lr_dt, "%Y-%m-%d").date()
            today = datetime.now().date()
            delta = today - lr_dt
            # hosts = self.hive_hosts.split(',')
            print hosts
            for host in hosts:
                (ret, out, err) = run_cmd(['scp', ('hdp@' + host + ':' + incoming_path),(self.local_staging_path + host + "/" )])
                print ret, out, err
                if ret > 0:
                    error = 1
                    err_msg = "Error while moving Current Log File to Local in_progress location".format(error)
                    print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print err_msg, output_msg
                    sys.exit(error)
                    return output_msg
                for i in range(delta.days):
                    dt = (lr_dt + timedelta(days=i))
                    dtstr = dt.isoformat()
                    print dtstr
                    (ret, out, err) = run_cmd(['scp', ('hdp@' + host + ':' + incoming_path + '.' + dtstr + '*'), (self.local_staging_path + host + "/")])
                    print ret, out, err
                    if ret > 0:
                        if err.find('No such file or directory') <> -1:
                            pass
                        else:
                            error       = 1
                            err_msg     = "Error while moving data to in_progress location".format(error)
                            print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                            status      = 'Job Error'
                            output_msg  = traceback.format_exc()
                            print output_msg
                            sys.exit(error)
                            return output_msg

# Unzipping the files if there are any zipped files
            for host in hosts:
                files           = glob.glob((self.local_staging_path + host + "/*"))
                for file in files:
                    if file.find(".gz") <> -1:
                        try:
                            with gzip.open(file, 'rb') as f_in:
                                with open((file.replace('.gz', '_') + host), 'wb') as f_out:
                                    shutil.copyfileobj(f_in, f_out)
                        except Exception as e:
                            error = 4
                            err_msg = "Error while unzipping file in Local FS"
                            output_msg = traceback.format_exc()
                            print err_msg, output_msg
                            sys.exit(error)
                            return output_msg
                        #(ret,out,err)       = run_cmd(['gunzip', '-c', file, ' > ','test')])
                        # (ret, out, err) = run_cmd(['gunzip', file])
                        #(ret, out, err) = run_cmd(['zcat',  file, '>', (file.replace('.gz', '_') + host)])
                        # if ret > 0:
                        #     error = 1
                        #     err_msg = "Error while unzipping file in Local FS".format(error)
                        #     print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                        #     status = 'Job Error'
                        #     output_msg = traceback.format_exc()
                        #     print err_msg, output_msg
                        #     return output_msg
                        (ret, out, err) = run_cmd(['rm','-f', file])
                        if ret > 0:
                            error = 1
                            err_msg = "Error while removing zipped file in Local FS".format(error)
                            print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                            status = 'Job Error'
                            output_msg = traceback.format_exc()
                            print err_msg, output_msg
                            sys.exit(error)
                            return output_msg
                    else:
                        (ret, out, err) = run_cmd(['mv', file, (file + '_' + host)])
                        if ret > 0:
                            error = 1
                            err_msg = "Error while renaming file in Local FS".format(error)
                            print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                            status = 'Job Error'
                            output_msg = traceback.format_exc()
                            print err_msg, output_msg
                            sys.exit(error)
                            return output_msg

# Moving the final set of files to the in_progress location to send it to HDFS
                move_files((self.local_staging_path + host + "/*"),local_inprogress_path)
                if ret > 0:
                    error = 1
                    err_msg = "Error while moving files to in_progress location in Local FS".format(error)
                    print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                    status = 'Job Error'
                    output_msg = traceback.format_exc()
                    print err_msg, output_msg
                    sys.exit(error)
                    return output_msg

# Ingesting to HDFS

            (ret,out,err)       = run_cmd(['hadoop','distcp', '-overwrite', 'file:///' + (local_inprogress_path + "/*"), 'hdfs:///' + inprogress_path])
            if ret > 0:
                error = 1
                err_msg = "Error while moving files to HDFS from Local in_progress path".format(error)
                print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + err)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print err_msg, output_msg
                sys.exit(error)
                return output_msg

            try:
                metadata_sql = "UPDATE sync.control_table SET last_run_time = now() \
                        WHERE target_tablename = 'hive_log' \
                            AND target_schemaname = 'default'" + " \
                            AND data_path = " + "'FS2HDFS'"

                print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "metadata_sql: " + metadata_sql)
                conn_metadata, cur_metadata = dbConnect(self.metastore_dbName, self.dbmeta_User, self.dbmeta_Url, self.dbmeta_Pwd)
                cur_metadata.execute(metadata_sql)
                # print (datetime.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "metastore controls:", controls)
            except psycopg2.Error as e:
                error = 2
                err_msg = "Error connecting to control table database".format(error)
                status = 'Job Error'
                output_msg = traceback.format_exc()
                print output_msg
                sys.exit(error)
                return output_msg
            finally:
                conn_metadata.close()

if __name__ == "__main__":
    obj1 = FStoHDFS()
    out = obj1.fs2hdfs_hive_log()
    print out


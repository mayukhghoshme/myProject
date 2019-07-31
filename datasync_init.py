
import sys
sys.path.append("/apps/common/")
sys.path.append("/data/analytics/common/")
from utils import read_config, dbConnect, remove_files, load_config, move_hdfs_files,move_files
import base64
import traceback
from auditLog import audit_logging
from datetime import datetime as logdt


class DataSyncInit(object):

    CONTROL_STATUS_INPROGRESS = 'R'
    CONTROL_STATUS_COMPLETED = 'C'
    CONTROL_STATUS_ERROR = 'E'

    def __init__(self):
        print ("[DataSyncInit: __init__]: Entered")
        self.class_name = self.__class__.__name__
        self.config_list = load_config()

        self.data_path = None
        self.load_id = None
        self.id = None
        self.source_schemaname = None
        self.source_tablename = None
        self.target_schemaname = None
        self.target_tablename = None
        self.load_type = None
        self.incremental_column = None
        self.last_run_time = None
        self.second_last_run_time = None
        self.join_columns = None
        self.log_mode = None
        self.archived_enabled = False
        self.s3_backed = False
        self.plant_name = None
        self.system_name = None
        self.technology = None
        self.job_name = None
        # Special change to handle Mirror layer schema mismatch
        self.is_special_logic = False
        self.is_partitioned = False


    def update_control(self, input_schema_name, input_table_name, update_code, run_id):
        method_name = self.class_name + ": " + "update_control"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        conn_metadata = None
        cur_metadata = ''
        try:
            metastore_dbName = self.config_list['meta_db_dbName']
            dbmeta_Url = self.config_list['meta_db_dbUrl']
            dbmeta_User = self.config_list['meta_db_dbUser']
            dbmeta_Pwd = base64.b64decode(self.config_list['meta_db_dbPwd'])
            conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)

            update_control_table_sql = "UPDATE sync.control_table \
                                SET load_status_cd = '" + update_code + "' \
                                WHERE target_schemaname = '" + input_schema_name + "'"
            if self.data_path in ['GP2HDFS','HDFS2MIR']:
                update_control_table_sql = update_control_table_sql \
                                            + " AND target_tablename in( '" + input_table_name + "','" + input_table_name + "_ext')" \
                                            + " AND data_path in ('GP2HDFS','HDFS2MIR')"
            else:
                update_control_table_sql = update_control_table_sql \
                                            + " AND target_tablename in( '" + input_table_name + "')" \
                                            + " AND data_path in ('" + self.data_path + "')"

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "update_control_table_sql: " + update_control_table_sql)
            cur_metadata.execute(update_control_table_sql)

        except Exception as e:
            error = 1
            err_msg = method_name + "[{0}]: Error while updating status in control table".format(error)
            status = 'Job Error'
            output_msg = traceback.format_exc()
            audit_logging(cur_metadata, self.load_id, run_id, self.plant_name, self.system_name, self.job_name, (input_schema_name + '.' + input_table_name), status,
                          self.data_path, self.technology, 0, 0, 0, error, err_msg, 0, 0, output_msg)
        finally:
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()


    def error_cleanup(self, input_schema_name, input_table_name, run_id, path=None, conn_metadata=None, conn_source=None, conn_target=None, target_path=None):
        method_name = self.class_name + ": " + "error_cleanup"
        print_hdr = "[" + method_name + ": " + self.data_path + ": " + str(self.load_id) + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        if path is None:
            path = self.config_list['misc_hdfsStagingPath']
        if self.data_path.find("GP2HDFS") <> -1 or self.data_path.find("HDFS2MIR") <> -1:
            remove_files(path, input_schema_name, input_table_name)

        if self.data_path.find("KFK2Hive") <> -1:
            if path is not None and target_path is not None:
                move_hdfs_files(path, target_path)

        if self.data_path.find("SRC2Hive") <> -1:
            if path is not None and target_path is not None:
                move_files(path, (target_path+input_schema_name))

        if input_table_name is not None:
            self.update_control(input_schema_name, input_table_name, self.CONTROL_STATUS_ERROR, run_id)

        if conn_metadata is not None and not conn_metadata.closed:
            conn_metadata.close()

        if conn_source is not None and not conn_source.closed:
            conn_source.close()

        if conn_target is not None:
            conn_target.close()


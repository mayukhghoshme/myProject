
import sys
sys.path.append("/apps/common/")
sys.path.append("/data/analytics/common/")
import base64
from utils import dbConnect, dbQuery, sendMail, load_config, get_kerberos_token, run_cmd, sendMailHTML
import multiprocessing
import traceback
from datasync_init import DataSyncInit
from db2file import Db2FileSync
from file2db import File2DBSync
from datetime import datetime as logdt
from file2hdfs import FStoHDFS

def run_sync_process(input):
    print "[datasync_driver: run_sync_process] - Entered"

    data_path = input["data_path"]
    if data_path.find("MIR2") <> -1:
        table_name = input['target_schemaname'] + "." + input['target_tablename']
    else:
        table_name = input['source_schemaname'] + "." + input['source_tablename']

    print_hdr = "[datasync_driver: run_sync_process: " + data_path + ": " + table_name + ": " + str(input["load_id"]) + "] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    if data_path.find("GP2HDFS") <> -1:
        db2file_sync = Db2FileSync(input)
        error, err_msg, output = db2file_sync.extract_gp2file_hive()
        if error == 0:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "GP Extraction successful")

            file2db_sync = File2DBSync(input)
            # Overwrite data path for GP-->Hive load
            file2db_sync.data_path = "HDFS2MIR"
            h_error, h_err_msg = file2db_sync.load_hive(output)
            if h_error > 1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR: " + h_err_msg)
        else:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR: " + err_msg)

    elif data_path == "SRC2GP":
        file2db_sync = File2DBSync(input)
        error, err_msg, tablename = file2db_sync.load_file2gp_hvr()

    elif data_path == 'SRC2RDS':
        file2db_sync = File2DBSync(input)
        error, err_msg, tablename = file2db_sync.file2rds_hvr()

    elif data_path == 'GP2Postgres':
        db2file_sync  = Db2FileSync(input)
        error, err_msg, output = db2file_sync.gp2file()
        if error == 0:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Finished to GP2File Extraction Successfully")
            file2db_sync    = File2DBSync(input)
            d_error, d_err_msg, d_tablename = file2db_sync.file2rds(output)
    elif data_path == 'SQOOP2Hive':
        db2file_sync  = Db2FileSync(input)
        error, err_msg, output = db2file_sync.sqoop2hdfs()
        print error, err_msg, output
        if error == 0:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Finished Extraction from Source Successfully")
            file2db_sync = File2DBSync(input)
            file2db_sync.source_schemaname = input['target_schemaname']
            file2db_sync.source_tablename = input['target_tablename']

            h_error, h_err_msg = file2db_sync.load_hive()
            if h_error > 1:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR: " + h_err_msg)
    elif data_path == 'FS2HDFS':
        file2hdfs_sync = FStoHDFS(input)
        error, err_msg, tablename = file2hdfs_sync.fs2hdfs()
        print error, err_msg, tablename

    elif data_path == 'Hive2RDS' or data_path == 'Hive2PREDIX':
        db2file_sync  = Db2FileSync(input)
        error, err_msg, output = db2file_sync.hive2file()
        if error == 0:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Finished to Hive2File Extraction Successfully")
            file2db_sync    = File2DBSync(input)
            d_error, d_err_msg, d_tablename = file2db_sync.file2rds(output)
            print d_error, d_err_msg, d_tablename
    elif data_path == 'Hive2S3':
        db2file_sync  = Db2FileSync(input)
        error, err_msg, output = db2file_sync.hive2file()
    elif data_path.find("MIR2") <> -1 or data_path.find("SRC2Hive") <> -1 or data_path.find("KFK") <> -1 or data_path.find("Talend2Hive") <> -1 or data_path.find("HDFS2MIR") <> -1 or data_path.find("NiFi2Hive") <> -1:
        file2db_sync = File2DBSync(input)
        file2db_sync.source_schemaname = input['target_schemaname']
        file2db_sync.source_tablename = input['target_tablename']

        h_error, h_err_msg = file2db_sync.load_hive()
        if h_error > 1:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR: " + h_err_msg)


def run_datasync_quality(data_path, schema_name, load_type, load_id, emailReceiver):

    print_hdr = "[ datasync_driver: run_datasync_quality : " + data_path + ": " ": " + schema_name + ": " + str(load_id) + "] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    try:
        cmd_args = ['python', '-u', '/apps/datasync/scripts/datasync_quality_wrapper.py', schema_name, data_path, str(load_id), load_type]
        (ret_cmd, out_cmd, err_cmd) = run_cmd(cmd_args)
        if ret_cmd <> 0:
            mail_subject = "ERROR: Datasync Quality: " + schema_name + ": " + data_path + ": " + load_type + ": " + str(load_id)
            output_msg = err_cmd[0:3997]
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            sendMailHTML(emailReceiver, mail_subject, output_msg)

        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + out_cmd)

    except Exception as e:
        mail_subject = "ERROR: Datasync Quality: " + schema_name + ": " + data_path + ": " + load_type + ": " + str(load_id)
        output_msg = "ERROR details:\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(emailReceiver, mail_subject, output_msg)


if __name__ == "__main__":
    print_hdr = "[datasync_driver: main] - "
    print (print_hdr + "Entered")
    # data_sync = DataSyncInit()
    # config_list = data_sync.config_list
    config_list = load_config()
    # print (print_hdr + "config_list: ", config_list)

    metastore_dbName           = config_list['meta_db_dbName']
    dbmeta_Url                 = config_list['meta_db_dbUrl']
    dbmeta_User                = config_list['meta_db_dbUser']
    dbmeta_Pwd                 = base64.b64decode(config_list['meta_db_dbPwd'])
    dbmeta_Port                = config_list['meta_db_dbPort']
    emailSender                = config_list['email_sender']
    emailReceiver              = config_list['email_receivers']
    # dbtgt_classpath            = config_list['tgt_db_beeline_classPath']

    if len(sys.argv) < 4:
        error = 1
        err_msg = "ERROR: Mandatory input arguments not passed"
        print print_hdr + err_msg
        error_table_list = ""
        sendMail(emailSender, emailReceiver, err_msg, error_table_list, 0, config_list['env'], "ERROR")
        sys.exit(1)

    input_schema                = sys.argv[1]
    load_type                   = sys.argv[2]
    data_path                   = sys.argv[3]
    load_group_id               = None
    input_tablename_list        = None
    input_multiprocessor        = None
    # Special logic to mirror table from one schema in GP to a different schema in HIVE
    is_special_logic            = False
    load_id                     = None
    system_name                 = None

    print_hdr = "[datasync_driver: main: " + data_path + ": " + input_schema + "] - "

    if (len(sys.argv) > 4 is not None) and (str(sys.argv[4]).upper() <> 'NONE'):
        load_group_id = int(sys.argv[4])

    if (len(sys.argv) > 5 is not None) and (str(sys.argv[5]).upper() <> 'NONE'):
        input_tablename_list = sys.argv[5].split(',')
        tablename_filter = ",".join("'" + l + "'" for l in input_tablename_list)
        schema_tablename_filter = ",".join("'" + input_schema + "." + l + "'" for l in input_tablename_list)
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "table list: " + tablename_filter)

    # if len(sys.argv) > 6 is not None:
    if len(sys.argv) > 6 and sys.argv[6] <> 'None':
        input_multiprocessor = int(sys.argv[6])

    if input_multiprocessor is None:
        input_multiprocessor = int(config_list['misc_multiprocessor_run'])
    else:
        input_multiprocessor = input_multiprocessor if input_multiprocessor < int(config_list['misc_multiprocessor_max']) else int(config_list['misc_multiprocessor_max'])

    # Special logic to mirror table from one schema in GP to a different schema in HIVE
    if len(sys.argv) > 7 is not None:
        arg = str(sys.argv[7])
        if arg == 'Y':
            is_special_logic = True

    if sys.argv[3] in ['SQOOP2Hive', 'NiFi2Hive'] :
        if len(sys.argv) < 10:
            error = 1
            print "#" *150
            print "For the data path SQOOP2Hive/NiFi2Hive you need to pass the system_name. The possible system names are --> "
            print "adw, adw_wkly, ats, cas, cas_2, erp_oracle, erp_sqlwh, erp_sqlwh_2, eservices, externaldata, get_dm, gets_msa, gets_rin, Meridium, mfgdata, ncmr, nucleus, odw, odw_2, odw_dyn_prcg, odw_gl_det, odw_hrly, pan_ins, pan_ins_erp, proficy, ras_audit, reliance, reliance4hours, reliance_ppap, reliance_ppap_sd, scp, sdr, shop_supt, shop_supt_2, srs, teamcenter, todw, tscollp, tspbtprd"
            print "Example command : /usr/bin/python2.7 /apps/datasync/scripts/datasync_driver.py <target_schemaname> <load_type> <data_path> None None None None None <system_name>"
            print "#" *150
            sys.exit(error)
        else:
            system_name = sys.argv[9]
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "load_type: " + load_type + " with " + str(input_multiprocessor) + " multiprocessor")


    error = 0
    load_id = -1
    conn_metadata = None
    try:
        conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)

        metadata_sql = "SELECT id, source_schemaname, source_tablename, target_schemaname, target_tablename, load_type, \
                incremental_column, last_run_time, second_last_run_time, join_columns, log_mode, data_path, s3_backed, is_partitioned, system_name as system_name_ct, custom_sql \
                FROM sync.control_table \
                WHERE data_path = '" + data_path + "' \
                    AND status_flag = 'Y'"
        if load_group_id is not None:
            metadata_sql = metadata_sql + " AND load_group_id = " + str(load_group_id)
        if data_path.find("FS2HDFS") <> -1 or data_path.find("KFK") <> -1 or data_path.find("MIR2") <> -1 or data_path.find("GP2Postgres") <> -1 or data_path.find("Hive2RDS") <> -1 or data_path.find("Hive2PREDIX") <> -1 or data_path.find("SRC2Hive") <> -1  or data_path.find("Talend2Hive") <> -1 or data_path.find("Hive2S3") <> -1 or data_path.find("HDFS2MIR") <> -1:
            metadata_sql = metadata_sql + " AND target_schemaname = '" + input_schema + "'"
        elif data_path.find("SQOOP2Hive") <> -1 or data_path.find("NiFi2Hive") <> -1:
            metadata_sql = metadata_sql + " AND target_schemaname = '" + input_schema + "' AND system_name = '" + system_name.lower() + "'"
        elif data_path.find("GP2HDFS") <> -1:
            metadata_sql = metadata_sql + " AND source_schemaname = '" + input_schema + "'" \
                           + " AND (hvr_last_processed_value > last_run_time OR last_run_time IS NULL)"
        else:
            metadata_sql = metadata_sql + " AND source_schemaname = '" + input_schema + "'"
        if input_tablename_list is not None:
            if data_path.find("FS2HDFS") <> -1 or data_path.find("KFK") <> -1 or data_path.find("MIR2") <> -1 or data_path.find("GP2Postgres") <> -1 or data_path.find("Hive2RDS") <> -1 or data_path.find("Hive2PREDIX") <> -1 or data_path.find("SRC2Hive") <> -1  or data_path.find("Talend2Hive") <> -1 or data_path.find("Hive2S3") <> -1 or data_path.find("HDFS2MIR") <> -1:
                metadata_sql = metadata_sql + " AND target_tablename in (" + tablename_filter + ")"
            elif data_path.find("SQOOP2Hive") <> -1 or data_path.find("NiFi2Hive") <> -1:
                metadata_sql = metadata_sql + " AND target_tablename in (" + tablename_filter + ") AND system_name = '" + system_name.lower() + "'"
            else:
                metadata_sql = metadata_sql + " AND source_tablename in (" + tablename_filter + ")"

        metadata_sql = metadata_sql + " AND load_type = '" + load_type + "'"

        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "metadata_sql: " + metadata_sql)

        # if len(sys.argv) > 8 is not None:
        if len(sys.argv) > 8 and sys.argv[8] <> 'None':
            load_id = int(sys.argv[8])
        else:
            load_id_sql     = "select nextval('sbdt.edl_load_id_seq')"
            load_id_lists   = dbQuery(cur_metadata,load_id_sql)
            load_id_list    = load_id_lists[0]
            load_id         = load_id_list['nextval']
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "load_id: " + str(load_id))
        controls = dbQuery(cur_metadata, metadata_sql)
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "controls: ", controls)
        input = []
        if len(controls) > 0:
            get_kerberos_token(config_list.get('env','prod'))
            for control in controls:
                # Special logic to mirror table from one schema in GP to a different schema in HIVE
                if (data_path.find("GP2HDFS") <> -1 and control['source_schemaname'] <> control['target_schemaname'] and not is_special_logic) \
                        or (data_path.find("GP2HDFS") <> -1 and control['source_schemaname'] <> control['target_schemaname'] and control['target_schemaname'] <> 'gpmirror'):
                    error = 2
                    err_msg = "ERROR: Mirror loading between different schemas for data path " + data_path + " not allowed for source table " + control['source_tablename'] + ": input schema"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg + ": " + input_schema)
                    if conn_metadata is not None and not conn_metadata.closed:
                        conn_metadata.close()
                    sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'], "ERROR", data_path, load_type, input_schema)
                    sys.exit(error)

                if bool(control['s3_backed']) and control['load_type'] <> 'APPEND_ONLY':
                    error = 2
                    err_msg = "ERROR: S3 target storage for source table " + control['source_tablename'] + " not allowed for load_type " + load_type + ": input schema"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg + ": " + input_schema)
                    if conn_metadata is not None and not conn_metadata.closed:
                        conn_metadata.close()
                    sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'], "ERROR", data_path, load_type, input_schema)
                    sys.exit(error)

                # input.append({'table': control['source_tablename'], 'data_path': data_path, 'load_id' : load_id})
                input.append({'id': control['id'], 'source_schemaname': control['source_schemaname'], 'source_tablename' : control['source_tablename'], \
                              'target_schemaname': control['target_schemaname'], 'target_tablename':control['target_tablename'], 'load_type': control['load_type'],\
                              'incremental_column': control['incremental_column'], 'last_run_time': control['last_run_time'], \
                              'second_last_run_time': control['second_last_run_time'], 'join_columns': control['join_columns'], 'log_mode': control['log_mode'], \
                              'data_path': control['data_path'], 'load_id' : load_id, 'plant_name':"DATASYNC", 'is_special_logic': is_special_logic, \
                              'system_name_ct': control['system_name_ct'],'custom_sql': control['custom_sql'],'is_partitioned': bool(control['is_partitioned'])})
        else:
            error = 3
            err_msg = "WARNING: No " + load_type + " " + data_path + " tables to load in this cycle for schema"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg + ": " + input_schema)
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'], "WARNING", data_path, load_type, input_schema)
            sys.exit(error)

        try:
            pool = multiprocessing.Pool(processes=input_multiprocessor)
            result = pool.map(run_sync_process, input, 1)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "result: ", result)
            pool.close()
            pool.join()
        except Exception as e:
            error = 4
            err_msg = "ERROR: in multiprocessing load jobs for " + load_type + " " + data_path + " in Schema"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg + ": " + input_schema)
            print ("ERROR details: " + traceback.format_exc())
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            #sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'], "ERROR", data_path, load_type, input_schema)
            sendMail(emailSender, emailReceiver, err_msg, traceback.format_exc(), load_id, config_list['env'], "ERROR", data_path, load_type, input_schema)
            if data_path in ['SRC2Hive','KFK2Hive'] and load_type <> 'APPEND_ONLY':
                run_datasync_quality(data_path, input_schema, load_type, load_id, emailReceiver)
            sys.exit(error)

        if input_tablename_list:
            error_log_sql = "SELECT concat(concat(table_name, '     ==> '), message) as table_name FROM sbdt.edl_log WHERE load_id = " + str(load_id) + " AND status = 'Job Error'  AND table_name IN ( " + schema_tablename_filter + ")"
        else:
            error_log_sql = "SELECT concat(concat(table_name, '     ==> '), message) as table_name FROM sbdt.edl_log WHERE load_id = " + str(load_id) + " AND status = 'Job Error'"
        error_tables    = dbQuery(cur_metadata, error_log_sql)
        # error_table_list = ','.join(d['table_name'] for d in error_tables)
        error_table_list = '\n\t'.join(d['table_name'] for d in error_tables)
        if len(error_table_list) > 0:
            error = 5
            error_table_list = '\n\r\t' + error_table_list
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR encountered for tables: ", error_tables)
            err_msg = "ERROR: Failed in " + load_type + " " + data_path + " to load tables"
            sendMail(emailSender, emailReceiver, err_msg, error_table_list, load_id, config_list['env'], "ERROR", data_path, load_type, input_schema)
        else:
            err_msg = "SUCCESS: " + load_type + " " + data_path +  " Finished successfully for Schema"
            error_table_list = input_schema
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg + ": " + input_schema)
            # sendMail(emailSender, emailReceiver, err_msg, error_table_list, load_id, config_list['env'], "SUCCESS", data_path, load_type, input_schema)

        if data_path in ['SRC2Hive', 'KFK2Hive'] and load_type <> 'APPEND_ONLY':
            run_datasync_quality(data_path, input_schema, load_type, load_id, emailReceiver)
        if data_path in ['SQOOP2Hive'] and load_type <> 'APPEND_ONLY':
            run_datasync_quality(data_path, system_name, load_type, load_id, emailReceiver)

    except Exception as e:
        error = 6
        err_msg = "ERROR: while loading data for Schema"
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + err_msg + ": " + input_schema)
        print ("ERROR details: " + traceback.format_exc())
        sendMail(emailSender, emailReceiver, err_msg, input_schema, load_id, config_list['env'], "ERROR", data_path, load_type, input_schema)
    finally:
        if conn_metadata is not None and not conn_metadata.closed:
            conn_metadata.close()

    sys.exit(error)


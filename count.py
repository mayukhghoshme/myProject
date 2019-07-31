
import psycopg2
import ConfigParser
import os
import base64
import sys
sys.path.append("/apps/common/")
import subprocess
from psycopg2.extras import RealDictCursor
from itertools import chain
import pandas as pd
import traceback
import datetime
from smtplib import SMTPException
import smtplib
import textwrap
import time
from datetime import datetime
import glob
import shutil
import psutil
from auditLog import audit_logging
from utils import dbConnectHive, dbConnect,txn_dbConnect, sendMail, dbQuery, load_config, run_cmd
sys.path.append("/data/datasync/hvr/scripts")

# def dbQuery (cur, query):
#     cur.execute(query)
#     rows = cur.fetchall()
#     return rows

def count(schemaname,loadtype):

    config_list                 = load_config()
    metastore_dbName            = config_list['meta_db_dbName']
    dbmeta_Url                  = config_list['meta_db_dbUrl']
    dbmeta_User                 = config_list['meta_db_dbUser']
    dbmeta_Pwd                  = base64.b64decode(config_list['meta_db_dbPwd'])

    dbtgt_host                  = config_list['src_db_hive_dbUrl']
    dbtgt_host2                 = config_list['src_db_hive_dbUrl2']

    dbtgt_Port                  = config_list['src_db_hive_dataPort']
    dbtgt_Auth                  = config_list['src_db_hive_authMech']

    src_dbName                  = config_list['src_db_gp_dbName']
    dbsrc_Url                   = config_list['src_db_gp_dbUrl']
    dbsrc_User                  = config_list['src_db_gp_dbUser']
    dbsrc_Pwd                   = base64.b64decode(config_list['src_db_gp_dbPwd'])

    emailSender                 = config_list['email_sender']
    emailReceiver               = config_list['email_receivers']

    t=datetime.fromtimestamp(time.time())
    v_timestamp = str(t.strftime('%Y-%m-%d %H:%M:%S'))

    input_source_schema         = schemaname
    load_type                   = loadtype
    print input_source_schema

    # try:
    #     count = 0
    #     for pid in psutil.pids():
    #         p = psutil.Process(pid)
    #         if p.name() == "python2.7" and  p.cmdline()[2] == input_source_schema:
    #             print p.name(), p.cmdline()[1], p.cmdline()[2]
    #             count = count +1
    # except Exception as e:
    #     print e
    #     return
    # print count
    # if count > 0:
    #     err_msg = "Exiting Count program as Loads are running . . ."
    #     print err_msg
    #     load_id = "None"
    #     error_table_list = input_source_schema
    #     sendMail(emailSender,emailReceiver,err_msg,error_table_list,load_id)
    #     return
    # else:

    try:
        conn_metadata, cur_metadata    = txn_dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
    except Exception as e:
        err_msg = "Error connecting to database while fetching  metadata"
        # Send Email
        print e
        return

    plant_name      = "DATASYNC"
    system_name     = "GPDB-Hive"
    job_name        = "COUNT " + input_source_schema
    tablename       = input_source_schema
    data_path       = "GP2HDFS"
    technology      = "Python"
    rows_inserted   = 0
    rows_deleted    = 0
    rows_updated    = 0
    num_errors      = 0
    count_sql_gpdb  = ""
    count_sql_hive  = ""

    load_id_sql     = "select nextval('sbdt.edl_load_id_seq')"
    load_id_lists   = dbQuery(cur_metadata,load_id_sql)
    load_id_list    = load_id_lists[0]
    load_id         = load_id_list['nextval']
    print "Load ID for this run is : " , load_id

    run_id_sql      = "select nextval('sync.datasync_seq')"
    run_id_lists    = dbQuery(cur_metadata,run_id_sql)
    run_id_list     = run_id_lists[0]
    run_id          = run_id_list['nextval']
    print "Run ID for this run is : " , run_id

    metadata_sql        = "SELECT source_schemaname||'.'||source_tablename||'-'||incremental_column as table_name "     \
                          "FROM sync.control_table  where data_path = 'GP2HDFS'  "                \
                          " and source_schemaname = '" + input_source_schema + "' AND load_type = '" + load_type + "'"
    print metadata_sql
    control             = dbQuery(cur_metadata, metadata_sql)
    control_df          = pd.DataFrame(control)
    control_df.columns  = ['table_name']
    new_control         = control_df['table_name'].tolist()

    status      = 'Job Started'
    output_msg  = ''
    err_msg     = ''
    audit_logging(cur_metadata, load_id,run_id, plant_name, system_name, job_name, tablename,status, \
              data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    q = 0
    for j in new_control:
        table_name, incremental_col = j.split('-')
        if q < len(new_control)-1:
            count_sql_gpdb += "SELECT " + str(run_id) + " as run_id, COUNT(*) as COUNT,'" + table_name + "' as table_name, 'GPDB' as db_name , '" + v_timestamp + "' as end_date, max(" + incremental_col + "::timestamp without time zone) as max_incr_col FROM " + table_name + " WHERE " + incremental_col + " > '1900-01-01' AND " + incremental_col + " <= '" + v_timestamp + "' UNION ALL "
            count_sql_hive += "SELECT " + str(run_id) + " as run_id, COUNT(*) as COUNT,'" + table_name + "' as table_name, 'Hive' as db_name , cast('" + v_timestamp + "' as timestamp) as end_date,max(hive_updated_date) as max_incr_col FROM " + table_name + " WHERE hive_updated_date > '1900-01-01' AND hive_updated_date <= '" + v_timestamp + "' UNION ALL "
            q =q +1
        else:
            count_sql_gpdb += "SELECT " + str(run_id) + " as run_id, COUNT(*) as COUNT,'" + table_name + "' as table_name , 'GPDB' as db_name , '" + v_timestamp + "' as end_date, max(" + incremental_col + "::timestamp without time zone) as max_incr_col FROM " + table_name + " WHERE " + incremental_col + " > '1900-01-01' AND " + incremental_col + " <= '" + v_timestamp + "'"
            count_sql_hive += "SELECT " + str(run_id) + " as run_id, COUNT(*) as COUNT,'" + table_name + "' as table_name , 'Hive' as db_name, cast('" + v_timestamp + "' as timestamp) as end_date, max(hive_updated_date) as max_incr_col FROM " + table_name + " WHERE hive_updated_date > '1900-01-01' AND hive_updated_date <= '" + v_timestamp + "'"

    print "Running GPDB Count . . . . ."
    # print count_sql_gpdb

    try:
        conn_source, cur_source      = dbConnect(src_dbName, dbsrc_User, dbsrc_Url, dbsrc_Pwd)
    except psycopg2.Error as e:
        err_msg = "Error connecting to source database"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.close()
        #continue
        return



    try:
        temp_table_sql = "CREATE TEMP TABLE count_" + input_source_schema + " AS " + count_sql_gpdb
        # print temp_table_sql
        cur_source.execute(temp_table_sql)
    except psycopg2.Error as e:
        print e
        err_msg = "Error while creating temp table in source"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.close()
        #continue
        return

    try:
        file = "/apps/staging/g00003/counts_" + input_source_schema + ".txt"
        gpdb_count_op_sql = "COPY count_" + input_source_schema + " TO STDOUT DELIMITER '|' NULL ''"
        pg_count_ip_sql   = "COPY counts FROM STDIN DELIMITER '|' NULL ''"
        fo   = open(file,'w')
        cur_source.copy_expert(gpdb_count_op_sql,fo)
        fo.close()
        fi   = open(file,'r')
        cur_metadata.copy_expert(pg_count_ip_sql,fi)
        fi.close()
    except psycopg2.Error as e:
        err_msg = "Error while copying"
        print err_msg
        print e
        status = 'Job Error'
        output_msg = traceback.format_exc()
        conn_metadata.close()
        conn_source.close()
        #continue
        return
    conn_source.close()

    print "Running Hive Count. . . . . "

    try:
        conn_target, cur_target   = dbConnectHive(dbtgt_host, dbtgt_Port, dbtgt_Auth)
    except Exception as e:
        try:
            conn_target, cur_target   = dbConnectHive(dbtgt_host2, dbtgt_Port, dbtgt_Auth)
        except Exception as e:
            err_msg      = "Error while connecting to target database"
            status       = 'Job Error'
            print e
            output_msg   = e
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
            conn_metadata.rollback()
            conn_metadata.close()
            conn_source.close()
            return

    count_view_sql = "CREATE OR REPLACE VIEW counts_" + input_source_schema + " AS " + count_sql_hive
    # print count_view_sql
    try:
        cur_target.execute(count_view_sql)
    except Exception as e:
        print e
        err_msg      = "Error while creating  view"
        status       = 'Job Error'
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.rollback()
        conn_metadata.close()
        conn_source.close()
        conn_target.close()
        return

    count_query = "SELECT * FROM counts_" + input_source_schema


    try:
        cur_target.execute(count_query)
    except Exception as e:
        print e
        err_msg      = "Error while executing count query"
        print err_msg
        status       = 'Job Error'
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.rollback()
        conn_metadata.close()
        conn_source.close()
        conn_target.close()
        return

    #results = {}
    #column = 0
    #for d in cur_target.description:
    #    results[d[0]] = column
    #    column = column + 1

    columnNames = [a['columnName'] for a in  cur_target.getSchema()]
    # print columnNames
    try:
        count_df  = pd.DataFrame(cur_target.fetchall(), columns = columnNames)
        file      = "/apps/staging/g00003/counts_" + input_source_schema + ".txt"
        f1        = open(file,'w')
        count_df.to_csv(path_or_buf=f1,sep='\t',header=False,index=False)
        f1.close()
    except Exception as e:
        print e
        err_msg      = "Error while writing Data Frame into file"
        print err_msg
        status       = 'Job Error'
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.rollback()
        conn_metadata.close()
        conn_source.close()
        conn_target.close()
        return


    try:
        copy_sql  = "COPY public.counts FROM STDIN WITH DELIMITER '\t'"
        fo        = open(file)
        cur_metadata.copy_expert(copy_sql,fo)
        run_cmd(['rm','-f','/apps/staging/g00003/counts_'+ input_source_schema + '.txt'])
        err_msg = "Count completed successfully . . ."
        print err_msg
        error_table_list = input_source_schema
        conn_target.close()
    except Exception as e:
        print e
        err_msg      = "Error while inserting data into final table"
        print err_msg
        status       = 'Job Error'
        output_msg   = traceback.format_exc()
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_metadata.rollback()
        conn_metadata.close()
        conn_source.close()
        conn_target.close()
        return

    # Final log entry
    try:
        error= 0
        err_msg     = 'No Errors'
        status      = 'Job Finished'
        output_msg  = 'Job Finished successfully'
        print output_msg
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, \
                      data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
    except psycopg2.Error as e:
        error= 15
        err_msg = "Error while dropping external table in target"
        print err_msg
        status = 'Job Error'
        output_msg = traceback.format_exc()
        print output_msg
        audit_logging(cur_metadata, load_id,  run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, num_errors, err_msg ,0,0,output_msg)
        conn_target.rollback()
        conn_target.close()
        conn_metadata.close()
        return error, err_msg, tablename

    conn_metadata.commit()
    conn_metadata.close()
if __name__ == "__main__":
    count(sys.argv[1],sys.argv[2])

import os
import textwrap
from smtplib import SMTPException
import smtplib
import subprocess
from datetime import datetime as logdt
import shutil
import glob
import psycopg2
from psycopg2.extras import RealDictCursor
import ConfigParser
import base64
import traceback
from datetime import datetime

def audit_logging (cur,load_id, run_id, plant_name, system_name , job_name, tablename, status, data_path, technology, rows_inserted, rows_updated, rows_deleted, number_of_errors, error_message ,source_row_count, target_row_count, error_category):

    #
    # if cur == '':
    #     print ("audiLog: audit_logging: Connection closed - reconnecting...")
    #     cur = get_cursor()

    try:
        #cur = get_cursor()
        query = "select * from sbdt.edl_log(" + str(load_id) + "," + str(run_id) +",'"+ str(plant_name)+"','"+ str(system_name) +"','"+ str(job_name)+"','"+ str(tablename)+"','"+ str(status)+"','"+ str(data_path)+"','"+ str(technology)+"',"+ str(rows_inserted) +"," \
                 + str(rows_updated) +","+ str(rows_deleted) +","+ str(number_of_errors) +",'"+ str(error_message) +"',"+str(source_row_count)+","+ str(target_row_count)+",'"+ str(error_category.replace('"','').replace('\'','')) + "')"
        print query
        cur.execute(query)
    except psycopg2.Error as e:
        err_msg = "Error connecting while inserting audit message"
        status = 'Job Error'
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        try:
           output_msg = traceback.format_exc()
           print output_msg
           audit_logging(cur, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, number_of_errors, err_msg ,0,0,output_msg)
        except KeyError as e:
           sql_state = e.pgcode
           audit_logging(cur, load_id, run_id, plant_name, system_name, job_name, tablename,status, data_path, technology,rows_inserted,rows_updated, rows_deleted, number_of_errors, err_msg ,0,0,sql_state)

def dbQuery (cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def read_config(cfg_files):
    # sys.path.append("/apps/incremental")
    if(cfg_files != None):
        config = ConfigParser.RawConfigParser()
        config.optionxform = str
        # merges all files into a single config
        for i, cfg_file in enumerate(cfg_files):
            if(os.path.exists(cfg_file)):
                config.read(cfg_file)
        return config

def sendMail(sender_parm, receivers_parm, message, error_table_list, load_id_parm, env="dev", load_status="", data_path="DataSync", load_type="", input_schema=""):
    sender          = sender_parm
    receivers       = receivers_parm.split(',')
    env             = env.upper()
    message         = textwrap.dedent("""\
From: %s
To: %s
Subject: %s : %s : %s : %s : %s

%s : %s

Load ID : %s

Best Regards,
EDGE Node""" %(sender_parm, receivers_parm, env, data_path, load_type, input_schema, load_status, message, error_table_list, load_id_parm))
    try:
       smtpObj = smtplib.SMTP('localhost')
       smtpObj.sendmail(sender, receivers, message)
       print ("[utils: sendMail] - Successfully sent email")
    except SMTPException:
       print ("[utils: sendMail] - Error: unable to send email")

def run_cmd(args_list):
    print(logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + '[utils: run_cmd] - Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err

def move_files(path_from, path_to):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: move_files] - Entered")
    print ("Path From : " + path_from)
    print ("Path To : " + path_to)
    files    = glob.glob(path_from)
    # for file in files:
    #     shutil.move(file,path_to)
    try:
        os.makedirs(path_to)
        for file in files:
            shutil.move(file, path_to)
    except Exception as e:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: move_files] - ", e, e.errno)
        if e.errno == 17:
            for file in files:
                print file, path_to
                shutil.move(file, path_to)
        else:
            print e
            err_msg = 'Error while moving files to destination path'
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: move_files] - " + err_msg)

def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = True  # auto-commit each entry to the database
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnect] - Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return conn, cur

def dataiku_backup():
    config         = read_config(['/apps/common/environ.properties'])
    env             = config.get('branch', 'env')
    metastore_dbName = config.get(env + '.meta_db', 'dbName')
    dbmeta_Url = config.get(env + '.meta_db', 'dbUrl')
    dbmeta_User = config.get(env + '.meta_db', 'dbUser')
    dbmeta_Pwd = base64.b64decode(config.get(env + '.meta_db', 'dbPwd'))
    emailSender = config.get(env + '.email', 'sender')
    emailReceiver = config.get(env + '.email', 'receivers')

    try:
        conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        load_id_sql = "select nextval('sbdt.edl_load_id_seq')"
        load_id_lists = dbQuery(cur_metadata, load_id_sql)
        load_id_list = load_id_lists[0]
        load_id = load_id_list['nextval']
        run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
        run_id_lists = dbQuery(cur_metadata, run_id_sql)
        run_id_list = run_id_lists[0]
        run_id = run_id_list['nextval']
        print "Load ID, Run ID for the backup job is " + str(load_id), str(run_id)
        status= 'Job Started'
        plant_name = 'GE Transportation'
        system_name = 'DataIKU'
        job_name = 'DataIKU Data Dir Backup'
        tablename = 'DataIKU'
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, '', 'Python', 0, 0, 0, 0, '',0, 0, '')
    except Exception as e:
        error = 1
        err_msg = "Error: connecting to logging database while fetching Load ID, Run ID and making first audit entry"
        print err_msg
        sendMail(emailSender, emailReceiver, err_msg, tablename, load_id, env, "ERROR","DataIKU Backup", '')
        return error, err_msg

    ret, out, err     = run_cmd(['hostname'])
    if ret > 0:
        error = 2
        print ret, out, err
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = err
        print "Error while getting hostname ", err
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg

    if out.find("163") <> -1:
        backup_file_name = "/apps/dataiku/data_dir_des"
        print "I am running on the Designer Node"
    elif out.find("107") <> -1:
        backup_file_name = "/apps/dataiku/data_dir_auto"
        print "I am running on the Automation Node"
    elif out.find("55") <> -1:
        backup_file_name = "/apps/dataiku/data_dir_poc"
        print "I am running on the PoC Node"
    elif out.find("220") <> -1:
        backup_file_name = "/apps/dataiku/data_dir_api_designer"
        print "I am running on the API Designer Node"
    elif out.find("83") <> -1:
        backup_file_name = "/apps/dataiku/data_dir_api_automation"
        print "I am running on the API Automation Node"
    else:
        error = 3
        err_msg = "I don't know what to do. Run me on the correct node"
        print err_msg
        return error, err_msg

    ret, out, err     = run_cmd(['service','dataiku','stop'])
    if ret > 0:
        error = 4
        print ret, out, err
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = err
        print "Error while stopping DataIKU service ", err
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg


# Remove old Backups
    old_backups = glob.glob(backup_file_name + "*")
    for old_backup in old_backups:
        ret, out, err = run_cmd(['rm', '-f', old_backup])
        if ret > 0:
            error = 5
            print ret, out, err
            status = 'Job Error'
            output_msg = traceback.format_exc()
            err_msg = err
            print "Error while cleaning old backups", err
            ret, out, err = run_cmd(['service', 'dataiku', 'start'])
            if ret > 0:
                error = 5
                print ret, out, err
                status = 'Job Error'
                output_msg = traceback.format_exc()
                err_msg = err
                print "Error while starting DataIKU service ", err
                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '','Python', 0, 0, 0, 0, err_msg, 0, 0, output_msg)
                return error, err_msg
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '','Python', 0, 0, 0, 0, err_msg, 0, 0, output_msg)

# Take new backup
    ret, out, err   = run_cmd(['tar','-czvf',(backup_file_name + '.tar.gz'), '-C', '/dss_data/','data_dir/'])
    if ret > 0:
        print ret, out, err
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = err
        error = 6
        print "Error while creating tarball", err
        ret, out, err = run_cmd(['service', 'dataiku', 'start'])
        if ret > 0:
            error = 7
            print ret, out, err
            status = 'Job Error'
            output_msg = traceback.format_exc()
            err_msg = err
            print "Error while starting DataIKU service ", err
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '','Python', 0, 0, 0, 0, err_msg, 0, 0, output_msg)
            return error, err_msg
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg

# Restart Dataiku service
    ret, out, err     = run_cmd(['service','dataiku','start'])
    if ret > 0:
        error = 8
        print ret, out, err
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = err
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg

#Move backup to S3
    ret, out, err     = run_cmd(['aws','s3','cp',(backup_file_name + '.tar.gz'),'s3://tr-datalake-prod/dataiku_prod/'])
    if ret > 0:
        error = 9
        print ret, out, err
        print "Error while moving files to S3"
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = err
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg
    status = 'Job Finished'
    err_msg = 'Finished Backup'
    audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python', 0,0, 0, 0, '', 0, 0, '')
    sendMail(emailSender, emailReceiver, err_msg, tablename, load_id, env, "SUCCESS", "DataIKU Backup", '')

if __name__ == "__main__":

    dataiku_backup()

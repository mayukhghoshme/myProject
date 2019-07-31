##################################################
#               UTILITIES                        #
##################################################
import sys
import ConfigParser
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from smtplib import SMTPException
import smtplib
import textwrap
import glob
import shutil
import subprocess
import pyhs2
import inspect
import traceback
import time
import base64
import mysql.connector
import re
import getpass
from datetime import datetime as logdt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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


def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = True  # auto-commit each entry to the database
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnect] - Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return conn, cur


def txn_dbConnect (db_parm, username_parm, host_parm, pw_parm, port_parm=5432):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm, 'port': port_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = False  # auto-commit each entry to the database
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: txn_dbConnect] - Connected Successfully to DB: " + str(db_parm) + "@" + str(host_parm))
    return conn, cur


def dbQuery (cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows


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
    except SMTPException as se:
       print ("[utils: sendMail] - Error: unable to send email" + str(se))
    except Exception as e:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[utils: sendMailHTML] - ERROR details: " + traceback.format_exc())
    finally:
        try:
            smtpObj.quit()
        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[utils: sendMailHTML] - ERROR details: " + traceback.format_exc())


def sendMailHTML(receivers_parm, mail_subject_parm, html_msg):
    config_list = load_config()
    sender          = config_list['email_sender']
    receivers       = receivers_parm.split(',')     # sendmail method expects receiver parameter as list
    mail_subject = str(config_list['env']).upper() + ": " + mail_subject_parm

    message = MIMEMultipart("alternative")
    message['Subject'] = mail_subject
    message['From'] = sender
    message['To'] = receivers_parm   # This attribute expects string and not list
    message.add_header('Content-Type', 'text/html')

    try:
        message.attach(MIMEText(html_msg, 'html'))
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message.as_string())
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[utils: sendMailHTML] - Successfully sent email")
    except SMTPException as se:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[utils: sendMailHTML] - Error: unable to send email" + str(se))
    except Exception as e:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[utils: sendMailHTML] - ERROR details: " + traceback.format_exc())
    finally:
        try:
            del message
            smtpObj.quit()
        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[utils: sendMailHTML] - ERROR details: " + traceback.format_exc())


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


def dbConnectHive (host_parm, port_parm, auth_parm, user_name=None):
    # Parse in connection information
    if user_name is None:
        credentials = {'host': host_parm, 'port': port_parm, 'authMechanism': auth_parm}
    else:
        env = os.environ.get('ENVIRONMENT_TAG', 'prod')
        if env == 'prod':
            user_realm = user_name + "@TRANSPORTATION-HDPPROD.GE.COM"
        elif env == 'dev':
            user_realm = user_name + "@TRANSPORTATION-HDPDEV.GE.COM"
        credentials = {'host': host_parm, 'port': port_parm, 'authMechanism': auth_parm, 'user':user_realm}

    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnectHive] - credentials: ", credentials)
    conn = pyhs2.connect(**credentials)                     # Change to Impyla
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnectHive] - Connected Successfully to Hive: " + str(host_parm))
    return conn, cur


def dbConnectOra (db_hostname, db_portname, db_database, db_username, db_pw):
    import cx_Oracle as cx_ora

    ora_dsn = cx_ora.makedsn(host=db_hostname, port=db_portname, service_name=db_database)
    try:
        conn = cx_ora.connect(db_username, db_pw, ora_dsn)
    except cx_ora.DatabaseError as dbe:
        ora_error, = dbe.args
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnectOra]: DB Connection Error: : " + str(ora_error.code) + ": " + ora_error.message)
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnectOra]: DB Connection retrying with SID......")
        ora_dsn = cx_ora.makedsn(host=db_hostname, port=db_portname, sid=db_database)
        conn = cx_ora.connect(db_username, db_pw, ora_dsn)

    # conn.autocommit = True  # auto-commit each entry to the database
    # conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnectOra] - Connected Successfully to Oracle DB: " + str(db_database) + "@" + str(db_hostname))
    return conn, cur


def dbConnectSQLServer (db_hostname, db_port, db_database, db_username, db_pw):
    import pyodbc

    conn = pyodbc.connect(driver='{ODBC driver 17 for SQL Server}',
                         server=db_hostname + ',' + str(db_port),
                         database=db_database,
                         uid=db_username,
                         pwd=db_pw)

    # conn.autocommit = True  # auto-commit each entry to the database
    cur = conn.cursor()
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: dbConnectSQLServer] - Connected Successfully to SQL Server DB: " + str(db_database) + "@" + str(db_hostname))
    return conn, cur


def call_spark_submit(sparkVersion,sparkMaster,deployMode,executorMemory,executorCores,driverMemory,driverMaxResultSize,sparkAppName,confFiles,loadScript,source_schemaname,source_tablename,load_id,run_id,data_path,v_timestamp):
    spark_cmd      = "spark-submit"
    name           = "--name"
    master         = "--master"
    # yarn           = "yarn"
    deploy_mode    = "--deploy-mode"
    # client         = "client"
    executor_mem   = "--executor-memory"
    executor_cores = "--executor-cores"
    driver_mem     = "--driver-memory"
    conf           = "--conf"
    driver_max_resultSize       = "spark.driver.maxResultSize=" + driverMaxResultSize
    env_spark_major_version     = "spark.yarn.appMasterEnv.SPARK_MAJOR_VERSION=" + sparkVersion
    env_environment_tag         = "spark.yarn.appMasterEnv.ENVIRONMENT_TAG=" + os.environ.get('ENVIRONMENT_TAG', 'prod')
    files          = "--files"
    #data_path      = "HDFS2MIR"
    load_table     =  source_schemaname + "." + source_tablename.replace('$','_')
    timestamp      = v_timestamp
    #Setting spark version before submitting job
    os.environ["SPARK_MAJOR_VERSION"] = sparkVersion
    #Preparing Spark submit string and submitting
    # print spark_cmd, master, yarn, deploy_mode, client, executor_mem, executorMemory, executor_cores, executorCores, driver_mem,driverMemory,loadScript,load_table,load_id,run_id, data_path, timestamp
    # subprocess.call([ spark_cmd,master,yarn, deploy_mode,client, executor_mem, executorMemory, executor_cores, executorCores, driver_mem,driverMemory,loadScript,load_table,str(load_id),str(run_id),data_path,timestamp])
    # subprocess.call([ spark_cmd,master, sparkMaster, deploy_mode,deployMode, executor_mem, executorMemory, executor_cores, executorCores, driver_mem,driverMemory,loadScript,load_table,str(load_id),str(run_id),data_path,timestamp])
    if deployMode == 'client':
        print spark_cmd, master, sparkMaster, deploy_mode, deployMode, executor_mem, executorMemory, executor_cores, executorCores, \
                driver_mem, driverMemory, loadScript, load_table, load_id, run_id, data_path, timestamp
        subprocess.check_call([spark_cmd, master, sparkMaster, deploy_mode, deployMode, executor_mem, executorMemory, executor_cores, executorCores, \
                               driver_mem, driverMemory, loadScript, load_table, str(load_id), str(run_id), data_path, timestamp])

    elif deployMode == 'cluster':
        print spark_cmd, name, sparkAppName, master, sparkMaster, deploy_mode, deployMode, executor_mem, executorMemory, executor_cores, executorCores, \
                driver_mem, driverMemory, conf, driver_max_resultSize, conf, env_spark_major_version, conf, env_environment_tag, files, confFiles, loadScript, \
                load_table, load_id, run_id, data_path, timestamp
        # subprocess.check_call([spark_cmd, master, sparkMaster, deploy_mode, deployMode, executor_mem, executorMemory, executor_cores, executorCores, driver_mem, driverMemory, files, confFiles, loadScript, load_table, str(load_id), str(run_id), data_path,timestamp])
        out_sp = ''
        try:
            out_sp = subprocess.check_output([spark_cmd, name, sparkAppName, master, sparkMaster, deploy_mode, deployMode, executor_mem, executorMemory, executor_cores, executorCores, \
                                              driver_mem, driverMemory, conf, driver_max_resultSize, conf, env_spark_major_version, conf, env_environment_tag, files, confFiles, loadScript, \
                                              load_table, str(load_id), str(run_id), data_path, timestamp], \
                                             stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            out_sp = e.output
            raise
        finally:
            print out_sp
            yarn_application_ids = re.findall(r"application_\d{13}_\d{4,10}", out_sp)
            if yarn_application_ids is not None and len(yarn_application_ids) > 0:
                yarn_application_id = yarn_application_ids[0]
                yarn_cmd_list = ['yarn', 'logs', '-applicationId', yarn_application_id, '-log_files', 'stdout']
                print ' '.join(yarn_cmd_list)
                subprocess.check_call(yarn_cmd_list)
            else:
                print ("[utils: call_spark_submit] -  No YARN Application Id available in spark-submit subprocess output")


# def get_kerberos_token(env_p='prod', osuser="gpadmin", keytab=None):
def get_kerberos_token(env_p='prod'):

    kinit_cmd  = "/usr/bin/kinit"
    kinit_args = "-kt"
    try:
        config_list = load_config()
        osuser = getpass.getuser()
        keytab = config_list['krb_keytab_' + osuser]
        # if keytab is None:
        #     keytab = "/home/gpadmin/gphdfs.service.keytab"
        env = os.environ.get('ENVIRONMENT_TAG', env_p)
        if env == 'prod':
            realm = osuser + "@TRANSPORTATION-HDPPROD.GE.COM"
        elif env == 'dev':
            realm = osuser + "@TRANSPORTATION-HDPDEV.GE.COM"
        try:
            java_home = os.environ["JAVA_HOME"]
            # print ("[utils: get_kerberos_token] - JAVA_HOME env: " + java_home)
        except KeyError:
            java_home = "/usr/lib/jvm/java"
            os.environ["JAVA_HOME"] = java_home
            # print ("[utils: get_kerberos_token] - JAVA_HOME set: " + java_home)

        print ("[utils: get_kerberos_token] - realm: " + realm)
        try:
            print kinit_cmd, kinit_args, keytab, realm
            subprocess.check_call([kinit_cmd, kinit_args, keytab, realm])
        except Exception as e:
            print ("[utils: get_kerberos_token] - ERROR for realm " + realm + ": ", e)
            if env <> 'dev':
                realm = osuser + "@TRANSPORTATION-HDPDEV.GE.COM"
                print ("[utils: get_kerberos_token] - Trying for realm: " + realm)
                print kinit_cmd, kinit_args, keytab, realm
                subprocess.call([kinit_cmd, kinit_args, keytab, realm])
    except OSError as ose:
        print ("[utils: get_kerberos_token] - OSError: ", ose)
    except Exception as e:
        output_msg = traceback.format_exc()
        print ("[utils: get_kerberos_token] - ERROR: ", output_msg)


def remove_files(rootpath, schemaname, tablename, data_path=None, target_tablename=None):
    # kinit_cmd  = "/usr/bin/kinit"
    # kinit_args = "-kt"
    # keytab     = "/home/gpadmin/gphdfs.service.keytab"
    # env = os.environ.get('ENVIRONMENT_TAG', 'prod')
    # if env == 'prod':
    #     realm = "gpadmin@TRANSPORTATION-HDPPROD.GE.COM"
    # elif env == 'dev':
    #     realm = "gpadmin@TRANSPORTATION-HDPDEV.GE.COM"
    # try:
    #     java_home  = os.environ["JAVA_HOME"]
    # except KeyError:
    #     java_home  = "/usr/lib/jvm/java"
    #     os.environ["JAVA_HOME"] = java_home
    # path       = paths + "/" + source_schemaname + "/" + source_tablename.replace('$','_') + "_ext/*"
    if data_path == 'SRC2Hive' or data_path == "KFK2Hive":
        # path = rootpath + schemaname + "/" + tablename.replace('$', '_') + "/in_progress/*"
        path = 'hdfs://getnamenode' + rootpath + schemaname + "/" + tablename.replace('$', '_') + "/in_progress/*"
    else:
        # path = rootpath + schemaname + "/" + tablename.replace('$', '_') + "_ext/*"
        path = 'hdfs://getnamenode' + rootpath + schemaname + "/" + tablename.replace('$', '_') + "_ext/*"
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: remove_files] - " + str(data_path) + ": path: " + path)
    try:
        if data_path == "SRC2Hive":
            local_path        = "/apps/hvr_hdfs_staging/" + schemaname + "/in_progress/*-" + target_tablename + ".csv"
            files = glob.glob(local_path)
            for file in files:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: remove_files] - Removing local file :" + file)
                subprocess.call(["rm", "-rf", file])
        elif data_path == "Talend2Hive":
            # talend_hdfs_inprogress_path = rootpath + schemaname + "/" + tablename + "/in_progress/" + target_tablename + ".csv"
            talend_hdfs_inprogress_path = 'hdfs://getnamenode' + rootpath + schemaname + "/" + tablename + "/in_progress/" + target_tablename + ".csv"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: remove_files] - talend_hdfs_inprogress_path: " + talend_hdfs_inprogress_path)
            subprocess.call(["hadoop", "fs", "-rm", "-r", talend_hdfs_inprogress_path])
        elif data_path == "NiFi2Hive":
            # talend_hdfs_inprogress_path = rootpath + schemaname + "/" + tablename + "/in_progress/" + target_tablename + ".csv"
            nifi_hdfs_inprogress_path = 'hdfs://getnamenode' + rootpath + schemaname + "/" + tablename + "_ext_talend/in_progress/"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: remove_files] - nifi_hdfs_inprogress_path: " + nifi_hdfs_inprogress_path)
            subprocess.call(["hadoop", "fs", "-rm", "-r", nifi_hdfs_inprogress_path])
        subprocess.call(["hadoop", "fs", "-rm", "-r", path])
    except OSError as ose:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: remove_files] - IGNORING OSError: ", ose)
    except Exception as e:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: remove_files] - IGNORING Exception: ", e)


def run_cmd(args_list):
    print(logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + '[utils: run_cmd] - Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err

# DO NOT USE - Experimetal code block --Starts - requires SUID bit to be set on python bin file
def run_cmd_asuser(osuser, args_list):
    import pwd
    print(logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + '[utils: run_cmd] - Running system command: {0}'.format(' '.join(args_list)))

    # osuser = getpass.getuser()
    pw_record = pwd.getpwnam(osuser)
    user_uid       = pw_record.pw_uid
    user_gid       = pw_record.pw_gid

    proc = subprocess.Popen(args_list, preexec_fn=os.setuid(user_uid), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err
# DO NOT USE - Experimetal code block --Ends

def move_hdfs_files(source_path, target_path):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: move_hdfs_files] - Moving files from " + source_path + " to " + target_path)
    (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (source_path + "*"), target_path])
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: move_hdfs_files] - Moving files from source to target - STATUS: " + str(ret_mv))
    if ret_mv <> 0:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[utils: move_hdfs_files] - ERROR while moving files: " + str(out_mv))


def cleanup_orphan_staging_hive(schema_name=None, filter_days=1):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: Entered")
    config_list = load_config()
    conn_hivemeta = None
    conn_hive = None

    try:
        hivemeta_sql = "SELECT concat(concat(d.NAME, '.'), t.TBL_NAME) AS full_table_name, \
                                FROM_unixtime(t.CREATE_TIME) as table_create_date, \
                                REPLACE(s.location,'hdfs://getnamenode','') as hdfs_path \
                        FROM TBLS t inner join DBS d on d.DB_ID = t.DB_ID \
                                inner join SDS s on s.SD_ID = t.SD_ID \
                        WHERE 1 = 1 \
                                AND t.TBL_NAME REGEXP '_tmp[0-9]{5,10}$' \
                                AND from_unixtime(t.CREATE_TIME) < CURDATE() - INTERVAL " + str(abs(filter_days)) + " DAY"
        if schema_name is not None:
            hivemeta_sql = hivemeta_sql + " AND d.NAME = '" + schema_name + "'"

        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: hivemeta_sql: " + hivemeta_sql)
        conn_hivemeta = mysql.connector.connect(user=config_list['mysql_dbUser'], password=base64.b64decode(config_list['mysql_dbPwd']),
                                                host=config_list['mysql_dbUrl'], database=config_list['mysql_dbMetastore_dbName'])

        cur_hivemeta = conn_hivemeta.cursor()
        cur_hivemeta.execute(hivemeta_sql)
        hivemeta_results = cur_hivemeta.fetchall()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: hivemeta_result: ", hivemeta_results)

        if len(hivemeta_results) > 0:
            try:
                conn_hive, cur_hive = dbConnectHive(config_list['tgt_db_hive_dbUrl'], config_list['tgt_db_hive_dbPort'], config_list['tgt_db_hive_authMech'])
            except Exception as e:
                try:
                    conn_hive, cur_hive = dbConnectHive(config_list['tgt_db_hive_dbUrl2'], config_list['tgt_db_hive_dbPort'], config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    raise

            for result in hivemeta_results:
                full_tablename, _, hdfs_path = result

                if hdfs_path.find(config_list['misc_hdfsStagingPath']) <> -1:
                    (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', hdfs_path])
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: removing staging tmp dir - STATUS: " + str(ret_rm))

                    drop_hive_temp_ext_table = "DROP TABLE IF EXISTS `" + full_tablename + "`"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: drop_hive_temp_ext_table: " + drop_hive_temp_ext_table)
                    cur_hive.execute(drop_hive_temp_ext_table)

                else:
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: cleanup not allowed for table " + full_tablename + " in hdfs path " + hdfs_path)

    except Exception as e:
        output_msg = traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: cleanup_orphan_staging_hive: ERROR: ", output_msg)
        raise
    finally:
        try:
            if conn_hivemeta is not None:
                conn_hivemeta.close()
        except:
            pass

        try:
            if conn_hive is not None:
                conn_hive.close()
        except:
            pass


def load_config():
    print ("utils: load_config: Entered")
    # config = read_config(['/apps/gp2hdp_sync/environ.properties'])
    # config = read_config(['environ.properties'])
    config_list = {}
    try:
        config = read_config(['/apps/common/environ.properties'])
        if ((config == None) or (len(config.sections()) == 0)):
            print ("utils: load_config: Config file not available in /apps/common/....locating in /data/analytics/common/")
            config = read_config(['/data/analytics/common/environ.properties'])
        # For local testing
        if ((config == None) or (len(config.sections()) == 0)):
            import os
            if os.environ.get('OS','None') == 'Windows_NT':
                print ("utils: load_config: Config file not available in /data/analytics/common/...locating in local")
                config = read_config(['environ.properties'])

        if ((config == None) or (len(config.sections()) == 0)):
            print ("utils: load_config: Config file not available")
        else:
            env = config.get('branch', 'env')
            config_list['env'] = env
            for section in config.sections():
                if str(section).find(env + ".") <> -1:
                    section_part = str(section).split(".")[1]
                    for option in config.options(section):
                        config_list[section_part + "_" + option] = config.get(section, option)
    except Exception as e:
        output_msg = traceback.format_exc()
        print ("utils: load_config: ERROR: ", output_msg)
    return config_list


def fetch_data_rds(sql):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: fetch_data_rds: Entered")
    config_list = {}
    conn_rds = None
    try:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: fetch_data_rds: sql: " + sql)
        config_list = load_config()
        conn_rds, cur_rds = dbConnect(config_list['meta_db_dbName'], config_list['meta_db_dbUser'], config_list['meta_db_dbUrl'], base64.b64decode(config_list['meta_db_dbPwd']))
        results = dbQuery(cur_rds, sql)
        return results
    except Exception as e:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: fetch_data_rds: ERROR: " + str(e.message))
        raise
    finally:
        if conn_rds is not None and not conn_rds.closed:
            conn_rds.close()


def update_data_rds(sql):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: update_data_rds: Entered")
    config_list = {}
    conn_rds = None
    rows_updated = 0
    try:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: update_data_rds: sql: " + sql)
        if sql.lower().startswith('update'):
            config_list = load_config()
            conn_rds, cur_rds = dbConnect(config_list['meta_db_dbName'], config_list['meta_db_dbUser'], config_list['meta_db_dbUrl'], base64.b64decode(config_list['meta_db_dbPwd']))
            cur_rds.execute(sql)
            rows_updated = cur_rds.rowcount
        else:
            rows_updated = -1
    except Exception as e:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: update_data_rds: ERROR: " + str(e.message))
        raise
    finally:
        if conn_rds is not None and not conn_rds.closed:
            conn_rds.close()
    if (rows_updated == -1):
        raise ValueError("ERROR: Only update allowed")
    return rows_updated


def call_method(path, module, method_name, sc, load_id, run_id, *args):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_method: Entered")
    retry_cnt = 10
    wait_time = 20
    step_location = 0
    error = 0
    err_msg = ''
    output = {}

    hive_txn = HiveTxn()
    hive_txn.run_id = run_id

    get_kerberos_token()
    while retry_cnt > 0:
        try:
            sys.path.append(path)
            module_handle = __import__(module, globals(), locals(), [], -1)
            func_handle = getattr(module_handle, method_name)

            read_output = hive_txn.get_objects(path)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_method: read_output: ", read_output)

            if read_output['error'] > 0:
                output = read_output
            else:
                if len(args) == 0:
                    output = func_handle(sc, load_id, run_id, step_location)
                elif len(args) == 1:
                    output = func_handle(sc, load_id, run_id,args[0], step_location)
                elif len(args) == 2:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], step_location)
                elif len(args) == 3:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], step_location)
                elif len(args) == 4:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], step_location)
                elif len(args) == 5:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], args[4], step_location)
                elif len(args) == 6:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], args[4], args[5], step_location)
                elif len(args) == 7:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], args[4], args[5], args[6], step_location)
                elif len(args) == 8:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], step_location)
                elif len(args) == 9:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], step_location)
                elif len(args) == 10:
                    output = func_handle(sc, load_id, run_id, args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], step_location)

            # print ("utils: call_method: output: ", output)
            if output is None:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_method: ERROR: output is None")
                rows_inserted = 0
                rows_updated = 0
                rows_deleted = 0
                num_errors = 1
                error = 1
                err_msg = "ERROR: utils: call_method:  output is None while calling method " + method_name + " inside module " + module + " at " + path
                status = 'Job Error'
                output_msg = "output is None"

                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors, "step_location": step_location}
                retry_cnt = 0

            elif len(output) > 0:
                output_msg = output.get('output_msg', "")
                error = output.get('error', 0)

                step_location = output.get('step_location', 0)

                if output_msg.find("java.io.FileNotFoundException: File does not exist") <> -1:
                    retry_cnt -= 1
                    print ("utils: call_method: retries left: " + str(retry_cnt + 1) + ": waiting for " + str(wait_time) + " secs...")
                    hive_txn.cleanup_hive()
                    time.sleep(wait_time)
                else:
                    retry_cnt = 0

            if retry_cnt == 0 and error == 0:
                write_output = hive_txn.write_hive(sc)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_method: write_output: ", write_output)

                if write_output['error'] > 0:
                    output['error'] = write_output['error']
                    output['err_msg'] = write_output['err_msg']
                    output['status'] = write_output['status']
                    output['output_msg'] = write_output['output_msg']

        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_method: ERROR: ", e)
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 2
            err_msg = "ERROR: utils: call_method:  while calling method " + method_name + " inside module " + module + " at " + path
            status = 'Job Error'
            output_msg = traceback.format_exc()

            if (output is None) or (len(output) == 0):
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors, "step_location": step_location}
            else:
                output["error"] = error
                output["err_msg"] = err_msg
                output["output_msg"] = output_msg
            retry_cnt = 0
        finally:
            if retry_cnt == 0:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: No Retry")
                hive_txn.cleanup_hive()

    if len(output) == 0:
        output = {"error": error, "err_msg": err_msg}
    return output


def call_datasync_method(path, i_instance, method_name, sc, table_name, load_id, run_id, data_path, v_timestamp):
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: Entered")
    retry_cnt = 10
    wait_time = 20
    error = 0
    err_msg = ''
    output = {}

    hive_txn = HiveTxn()
    hive_txn.run_id = run_id

    while retry_cnt > 0:
        try:
            sys.path.append(path)
            func_handle = getattr(i_instance, method_name)

            read_output = hive_txn.get_datasync_objects(data_path, table_name)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: read_output: ", read_output)

            if read_output['error'] > 0:
                output = read_output
            else:
                output = func_handle(sc, table_name, load_id, run_id, data_path, v_timestamp)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: output: ", output)

            if output['error'] > 0:
                output_msg = output["output_msg"]
                if output_msg.find("java.io.FileNotFoundException: File does not exist") <> -1:
                    retry_cnt -= 1
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: retries left: " + str(retry_cnt + 1) + ": waiting for " + str(wait_time) + " secs...")
                    hive_txn.cleanup_hive()
                    time.sleep(wait_time)
                else:
                    retry_cnt = 0
            elif output['error'] < 0:
                output['error'] = 0
                retry_cnt = 0
            else:
                write_output = hive_txn.write_hive(sc, True)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: write_output: ", write_output)

                if write_output['error'] > 0:
                    output['error'] = write_output['error']
                    output['err_msg'] = write_output['err_msg']
                    output['status'] = write_output['status']
                    output['output_msg'] = write_output['output_msg']
                retry_cnt = 0

        except Exception as e:
            rows_inserted = output.get('rows_inserted', 0)
            rows_updated = output.get('rows_updated', 0)
            rows_deleted = output.get('rows_deleted', 0)
            num_errors = output.get('num_errors', 0)
            error = 1
            err_msg = "ERROR: utils: call_datasync_method:  while calling method " + method_name + " of class " + i_instance.__class__.__name__ +" at " + path
            status = 'Job Error'
            output_msg = traceback.format_exc()
            if len(output) == 0:
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
            else:
                output["error"] = error
                output["err_msg"] = err_msg
                output["output_msg"] = output_msg
            retry_cnt = 0
        finally:
            if retry_cnt == 0:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "utils: call_datasync_method: No Retry")
                hive_txn.cleanup_hive()

    if len(output) == 0:
        output = {"error": error, "err_msg": err_msg}
    return output



def call_method_alt(method_name, *args):
    print ("utils: call_method_alt: Entered")
    retry_cnt = 10
    wait_time = 20
    step_location = 0
    output = {}
    while retry_cnt > 0:
        error_flag = 0
        try:
            frame = inspect.stack()[1]
            i_module = inspect.getmodule(frame[0])
            file = i_module.__file__
            file = str(file).replace(".py","")
            li = str(file).split("/")
            py_module = li[len(li)-1]
            li.remove(py_module)
            path = "/".join(li)
            sys.path.append(path)
            caller_handle = __import__(py_module, globals(), locals(), [], -1)
            func_handle = getattr(caller_handle, method_name)

            if len(args) == 1:
                output = func_handle(args[0], step_location)
            elif len(args) == 2:
                output = func_handle(args[0], args[1], step_location)
            elif len(args) == 3:
                output = func_handle(args[0], args[1], args[2], step_location)
            elif len(args) == 4:
                output = func_handle(args[0], args[1], args[2], args[3], step_location)
            elif len(args) == 5:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], step_location)
            elif len(args) == 6:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], step_location)
            elif len(args) == 7:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], step_location)
            elif len(args) == 8:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7],
                                     step_location)
            elif len(args) == 9:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8],
                                     step_location)
            elif len(args) == 10:
                output = func_handle(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8],
                                     args[9], step_location)

            output_msg = output["output_msg"]
            step_location = output["step_location"]
            if output_msg.find("java.io.FileNotFoundException: File does not exist") <> -1:
                retry_cnt -= 1
                error_flag = 1
                print ("utils: call_method_alt: retries left: " + str(retry_cnt + 1) + ": waiting for " + str(
                    wait_time) + " secs...")
                time.sleep(wait_time)
            else:
                retry_cnt = 0
        except Exception as e:
            print ("utils: call_method: ERROR: ", e)
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 1
            err_msg = "ERROR: utils: call_method:  while calling method " + method_name
            status = 'Job Error'
            output_msg = traceback.format_exc()

            if len(output) == 0:
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors, "step_location": step_location}
            else:
                output["output_msg"] = output_msg
            retry_cnt = 0

        if retry_cnt == 0:
            print ("utils: call_method_alt: No Retry")

        return output



class HiveTxn(object):

    LOAD_TYPE_FULL = 'FULL'
    LOAD_TYPE_APPEND_ONLY = 'APPEND_ONLY'
    LOAD_TYPE_INCREMENTAL = 'INCREMENTAL'

    def __init__(self):
        print ("[HiveTxn: __init__]: Entered")
        self.class_name = self.__class__.__name__
        self.config_list = load_config()
        self.data_path = None
        self.run_id = 0
        self.source_table_dlist = []
        self.target_table_dlist = []


    def read_hive(self, source_table_dlist, target_table_dlist):
        method_name = self.class_name + ": " + "read_hive"
        print_hdr = "[" + method_name + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        # To eliminate duplicate append in retry
        self.source_table_dlist = []
        self.target_table_dlist = []

        table_list = []
        if target_table_dlist is not None and len(target_table_dlist) > 0:
            for table_d in target_table_dlist:
                table_list.append(table_d['table_name'])
        if source_table_dlist is not None and len(source_table_dlist) > 0:
            for table_d in source_table_dlist:
                table_list.append(table_d['table_name'])

        print (print_hdr + "table_list: ", table_list)

        conn_hivemeta = None
        conn_hive = None
        # tmp_source_table_dlist = []
        # tmp_target_table_dlist = []
        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        num_errors = 1
        error = 0
        err_msg = ''
        output = {}

        if len(table_list) == 0:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "No tables to read")
            output = {"error": error, "err_msg": err_msg}
            return output

        try:
            table_filter_list = ",".join("'" + l + "'" for l in table_list)

            hivemeta_sql = "SELECT  concat(concat(d.NAME, '.'), t.TBL_NAME) AS FULL_TABLE_NAME, d.NAME AS SCHEMA_NAME, t.TBL_NAME AS TABLE_NAME, \
                                    CASE WHEN t.TBL_TYPE LIKE '%VIEW'THEN 'VIEW' WHEN t.TBL_TYPE LIKE '%TABLE' THEN 'TABLE' ELSE t.TBL_TYPE END AS TABLE_TYPE, \
                                    coalesce(p.PARTITION_COL_CNT,0) AS PARTITION_COL_CNT, p.PARTITION_COLS, t.VIEW_EXPANDED_TEXT AS VIEW_SQL \
                                    FROM TBLS t inner join DBS d \
                                    on d.DB_ID = t.DB_ID \
                                    left outer join (select TBL_ID, count(*) as PARTITION_COL_CNT, group_concat(PKEY_NAME) as PARTITION_COLS from hive.PARTITION_KEYS group by TBL_ID) p \
                                    on p.TBL_ID = t.TBL_ID \
                                WHERE 1 = 1 \
                                    AND concat(concat(d.NAME, '.'), t.TBL_NAME) IN (" + table_filter_list + ")"

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "hivemeta_sql: " + hivemeta_sql)
            conn_hivemeta = mysql.connector.connect(user=self.config_list['mysql_dbUser'], password=base64.b64decode(self.config_list['mysql_dbPwd']),
                                                    host=self.config_list['mysql_dbUrl'], database=self.config_list['mysql_dbMetastore_dbName'])

            cur_hivemeta = conn_hivemeta.cursor()
            cur_hivemeta.execute(hivemeta_sql)
            hivemeta_results = cur_hivemeta.fetchall()
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "hivemeta_result: ", hivemeta_results)

            staging_path = self.config_list['misc_hdfsStagingPath']
            warehouse_path = self.config_list['misc_hiveWarehousePath']
            cur_hive = None
            target_cnt = 0
            if len(hivemeta_results) > 0:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    try:
                        conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                    except Exception as e:
                        raise

                cur_hive.execute("SET hive.exec.dynamic.partition = true")
                cur_hive.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
                # Not in list of params that are allowed to be modified at runtime
                # cur_hive.execute("SET mapred.input.dir.recursive = true")
                # cur_hive.execute("SET hive.mapred.supports.subdirectories = true")

                for result in hivemeta_results:
                    full_tablename, schema_name, table_name, table_type, partition_col_cnt, partition_cols, view_sql = result

                    is_target = False
                    is_source = False
                    is_source_ddl = True
                    incremental_column = 'None'
                    second_last_run_time = None
                    load_type = None

                    for target_dict in target_table_dlist:
                        if full_tablename == target_dict.get('table_name'):
                            target_dict['partition_col_cnt'] = partition_col_cnt
                            target_dict['partition_cols'] = partition_cols
                            # tmp_target_table_dlist.append(target_dict)
                            self.target_table_dlist.append(target_dict)
                            is_target = True
                            target_cnt += 1

                    for source_dict in source_table_dlist:
                        if full_tablename == source_dict.get('table_name'):
                            # tmp_source_table_dlist.append(source_dict)
                            self.source_table_dlist.append(source_dict)
                            is_source_ddl = source_dict.get('table_name', True)
                            incremental_column = source_dict.get('incremental_column', 'None')
                            second_last_run_time = source_dict.get('second_last_run_time', None)
                            load_type = source_dict.get('load_type', None)
                            is_source = True

                    tmp_table = table_name + '_tmp' + str(self.run_id)
                    # tmp_path = staging_path + schema_name + '.db/' + tmp_table + '/'
                    # tmp_path = staging_path + schema_name + '/' + tmp_table + '/'
                    tmp_path = 'hdfs://getnamenode' + staging_path + schema_name + '/' + tmp_table + '/'
                    # whs_tmp_path = warehouse_path + schema_name + '.db/' + tmp_table + '/'
                    whs_tmp_path = 'hdfs://getnamenode' + warehouse_path + schema_name + '.db/' + tmp_table + '/'

                    if is_target or (is_source and is_source_ddl):

                        # (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (tmp_path + '*')])
                        (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (tmp_path)])
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "removing staging tmp dir - STATUS: " + str(ret_rm))

                        drop_hive_temp_ext_table = "DROP TABLE IF EXISTS `" + schema_name + "." + tmp_table + "`"
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "drop_hive_temp_ext_table: " + drop_hive_temp_ext_table)
                        cur_hive.execute(drop_hive_temp_ext_table)

                        create_hive_temp_ext_table = "CREATE EXTERNAL TABLE `" + schema_name + "." + tmp_table + "` LIKE `" + schema_name + "." + table_name + "` \
                                                STORED AS ORC LOCATION '" + tmp_path + "' \
                                                TBLPROPERTIES ('orc.compress'='ZLIB')"

                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "create_hive_temp_ext_table: " + create_hive_temp_ext_table)
                        cur_hive.execute(create_hive_temp_ext_table)

                    if is_source and is_source_ddl:
                        if table_type == 'VIEW':
                            # Change location of tmp table from warehouse path to staging path
                            # alter_location_sql = "ALTER TABLE `" + schema_name + "." + tmp_table + "` SET LOCATION 'hdfs://getnamenode" + tmp_path + "'"
                            alter_location_sql = "ALTER TABLE `" + schema_name + "." + tmp_table + "` SET LOCATION '" + tmp_path + "'"
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "alter_location_sql: " + alter_location_sql)
                            cur_hive.execute(alter_location_sql)

                            # insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + "`" + view_sql
                            insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + "` SELECT * FROM `" + full_tablename + "`"
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "insert_tmp_table_sql: " + insert_tmp_table_sql)
                            cur_hive.execute(insert_tmp_table_sql)

                            # if self.data_path.find("MIR2") <> -1 and load_type == self.LOAD_TYPE_INCREMENTAL:
                            #     if incremental_column <> 'None' and second_last_run_time is not None and second_last_run_time <> 'None' and view_sql.find(incremental_column) <> -1:
                            #         second_last_run_date = second_last_run_time.strftime('%Y-%m-%d')
                            #         incremental_view_sql = view_sql.replace(incremental_column, "'" + second_last_run_date + "'")
                            #         insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + "`" + incremental_view_sql
                            #         print (print_hdr + "insert_tmp_table_sql: " + insert_tmp_table_sql)
                            #         cur_hive.execute(insert_tmp_table_sql)
                            #     else:
                            #         error = 1
                            #         err_msg = "ERROR: HiveTxn: read_hive: Incremental column or second last run time not available for Incremantal load"
                            #         status = 'Job Error'
                            #         output_msg = "Second Last Run Time data not available for Incremantal load"
                            #         output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                            #                   "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
                            # else:
                            #     insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + "` SELECT * FROM `" + full_tablename + "`"
                            #     print (print_hdr + "insert_tmp_table_sql: " + insert_tmp_table_sql)
                            #     cur_hive.execute(insert_tmp_table_sql)

                            # Remove warehouse path created for tmp table from view
                            (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', whs_tmp_path])
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "removing warehouse tmp dir - STATUS: " + str(ret_rm))

                        elif table_type == 'TABLE':
                            insert_tmp_table_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + tmp_table + \
                                                   "` SELECT * FROM `" + schema_name + "." + table_name + "`"

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "insert_tmp_table_sql: " + insert_tmp_table_sql)
                            cur_hive.execute(insert_tmp_table_sql)

            # if (len(hivemeta_results) == 0) or (target_cnt == 0):
            if target_cnt == 0:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "WARNING: TARGET table locking not enabled")
            if len(hivemeta_results) == 0:
                error = 2
                err_msg = "ERROR: HiveTxn: read_hive: Table details not existing in Hive MetaStore"
                status = 'Job Error'
                output_msg = "Table details not existing in Hive MetaStore"
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}

            # self.source_table_dlist = tmp_source_table_dlist
            # self.target_table_dlist = tmp_target_table_dlist

        except Exception as e:
            error = 3
            err_msg = "ERROR: HiveTxn: read_hive: while reading hive transactions"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                      "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
        finally:
            try:
                if conn_hivemeta is not None:
                    conn_hivemeta.close()
            except: pass

            try:
                if conn_hive is not None:
                    conn_hive.close()
            except: pass

        if len(output) == 0:
            output = {"error": error, "err_msg": err_msg}
        return output


    def write_hive(self, sc, is_datasync=False):
        method_name = self.class_name + ": " + "write_hive"
        print_hdr = "[" + method_name + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        conn_hive = None
        error = 0
        err_msg = ''
        output = {}

        if len(self.target_table_dlist) == 0:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "No tables to write")
            output = {"error": error, "err_msg": err_msg}
            return output

        try:
            staging_path = self.config_list['misc_hdfsStagingPath']
            warehouse_path = self.config_list['misc_hiveWarehousePath']
            conn_hive = None
            try:
                conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
            except Exception as e:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    raise

                cur_hive.execute("SET hive.exec.dynamic.partition = true")
                cur_hive.execute("SET hive.exec.dynamic.partition.mode = nonstrict")
                # Not in list of params that are allowed to be modified at runtime
                # cur_hive.execute("SET mapred.input.dir.recursive = true")
                # cur_hive.execute("SET hive.mapred.supports.subdirectories = true")

            print (print_hdr + "target_table_dlist: ", self.target_table_dlist)
            for table_dict in self.target_table_dlist:
                table = table_dict['table_name']
                load_type = table_dict.get('load_type', self.LOAD_TYPE_FULL)
                s3_backed = table_dict.get('s3_backed', False)

                partition_col_cnt = table_dict['partition_col_cnt']
                partition_cols = table_dict['partition_cols']

                schema_name, table_name = table.split('.')
                tmp_table = table_name + '_tmp' + str(self.run_id)
                # tmp_path = staging_path + schema_name + '.db/' + tmp_table + '/'
                tmp_path = staging_path + schema_name + '/' + tmp_table + '/'
                hdfs_tmp_path = 'hdfs://getnamenode' + tmp_path

                # if s3_backed:
                if s3_backed and load_type == self.LOAD_TYPE_APPEND_ONLY:
                    target_path = 's3a://' + self.config_list['s3_bucket_name'] + '/' + schema_name + '/' + table_name + '/'
                else:
                    # target_path = warehouse_path + schema_name + '.db/' + table_name + '/'
                    target_path = 'hdfs://getnamenode' + warehouse_path + schema_name + '.db/' + table_name + '/'

                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "temp_path: " + tmp_path)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "hdfs_tmp_path: " + hdfs_tmp_path)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "target_path: " + target_path)

                ret_rm = 0
                err_rm = ''
                ret_mv = 0
                err_mv = ''

                table_lock_sql = "LOCK TABLE `" + schema_name + "." + table_name + "` EXCLUSIVE"
                table_unlock_sql = "UNLOCK TABLE `" + schema_name + "." + table_name + "`"

                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Load_type: " + load_type + " with partition column count: " + str(partition_col_cnt))
                try:
                    # print (print_hdr + "table_lock_sql: " + table_lock_sql)
                    # cur_hive.execute(table_lock_sql)

                    if (load_type == self.LOAD_TYPE_FULL) or (load_type == self.LOAD_TYPE_INCREMENTAL and partition_col_cnt == 0):
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "table_lock_sql: " + table_lock_sql)
                        cur_hive.execute(table_lock_sql)

                        (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (target_path + "*")])
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + load_type + ": removing files from target - STATUS: " + str(ret_rm))

                        # if s3_backed:
                        #     (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'distcp', '-D', 'fs.s3a.fast.upload=true', (hdfs_tmp_path + "*"), target_path])
                        #     print (print_hdr + load_type + ": distcp files from temp to s3 target - STATUS: " + str(ret_mv))
                        # else:
                        # (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (tmp_path + "*"), target_path])
                        (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (hdfs_tmp_path + "*"), target_path])
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + load_type + ": moving files from temp to target - STATUS: " + str(ret_mv))

                    elif load_type == self.LOAD_TYPE_APPEND_ONLY and partition_col_cnt == 0:
                        if s3_backed:
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'distcp', (hdfs_tmp_path + "*"), target_path])
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + load_type + ": distcp files from temp to s3 target - STATUS: " + str(ret_mv))
                        else:
                            # (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (tmp_path + "*"), target_path])
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'fs', '-mv', (hdfs_tmp_path + "*"), target_path])
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + load_type + ": moving files from temp to target -  STATUS: " + str(ret_mv))

                    elif load_type == self.LOAD_TYPE_APPEND_ONLY and partition_col_cnt > 0:
                        if s3_backed:
                            (ret_mv, out_mv, err_mv) = run_cmd(['hadoop', 'distcp', (hdfs_tmp_path + "*"), target_path])
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + load_type + ": distcp files from temp to s3 target - STATUS: " + str(ret_mv))
                        else:
                            from pyspark.sql import HiveContext

                            sqlContext = HiveContext(sc)
                            sqlContext.setConf("hive.exec.dynamic.partition", "true")
                            sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
                            sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
                            sqlContext.setConf("mapred.input.dir.recursive", "true")
                            sqlContext.setConf("hive.mapred.supports.subdirectories", "true")

                            # Disabled till assertion issue resolved
                            # sqlContext.setConf("spark.sql.orc.enabled", "true")
                            # sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "true")
                            # sqlContext.setConf("spark.sql.orc.char.enabled", "true")

                            sc._jsc.hadoopConfiguration().set('fs.s3a.attempts.maximum', '30')

                            sqlContext.setConf("hive.hadoop.supports.splittable.combineinputformat", "true")
                            sqlContext.setConf("tez.grouping.min-size", "1073741824")
                            sqlContext.setConf("tez.grouping.max-size", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.minsize", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.maxsize", "2147483648")
                            sqlContext.setConf("hive.merge.smallfiles.avgsize", "2147483648")
                            sqlContext.setConf("hive.exec.reducers.bytes.per.reducer", "2147483648")

                            select_tmp_sql = "SELECT * FROM " + schema_name + "." + tmp_table
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "select_tmp_sql: " + select_tmp_sql)
                            final_df = sqlContext.sql(select_tmp_sql)

                            partition_cols_list =  partition_cols.split(",")
                            final_df.write.option("compression", "zlib").mode("append").format("orc").partitionBy(partition_cols_list).save(target_path)

                    elif load_type == self.LOAD_TYPE_INCREMENTAL and partition_col_cnt > 0:
                        if not is_datasync:
                            from pyspark.sql import HiveContext

                            sqlContext = HiveContext(sc)
                            sqlContext.setConf("hive.exec.dynamic.partition", "true")
                            sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
                            sqlContext.setConf("spark.sql.orc.filterPushdown", "true")
                            sqlContext.setConf("mapred.input.dir.recursive", "true")
                            sqlContext.setConf("hive.mapred.supports.subdirectories", "true")

                            # Disabled till assertion issue resolved
                            # sqlContext.setConf("spark.sql.orc.enabled", "true")
                            # sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "true")
                            # sqlContext.setConf("spark.sql.orc.char.enabled", "true")

                            sc._jsc.hadoopConfiguration().set('fs.s3a.attempts.maximum', '30')

                            sqlContext.setConf("hive.hadoop.supports.splittable.combineinputformat", "true")
                            sqlContext.setConf("tez.grouping.min-size", "1073741824")
                            sqlContext.setConf("tez.grouping.max-size", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.minsize", "2147483648")
                            sqlContext.setConf("mapreduce.input.fileinputformat.split.maxsize", "2147483648")
                            sqlContext.setConf("hive.merge.smallfiles.avgsize", "2147483648")
                            sqlContext.setConf("hive.exec.reducers.bytes.per.reducer", "2147483648")

                            select_tmp_sql = "SELECT * FROM " + schema_name + "." + tmp_table
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "select_tmp_sql: " + select_tmp_sql)
                            final_df = sqlContext.sql(select_tmp_sql)

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "table_lock_sql: " + table_lock_sql)
                            cur_hive.execute(table_lock_sql)

                            spark_tmp_table = table_name + '_stg'
                            final_df.createOrReplaceTempView(spark_tmp_table)

                            insert_ext_target_sql = 'INSERT OVERWRITE TABLE ' + schema_name + '.' + table_name + \
                                        ' PARTITION ( ' + partition_cols + ' ) \
                                        SELECT * FROM ' + spark_tmp_table

                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "insert_ext_target_sql: " + insert_ext_target_sql)
                            sqlContext.sql(insert_ext_target_sql)

                    else:
                        error = 1
                        err_msg = "ERROR: HiveTxn: write_hive: Not entered control flow for loading target table"
                        status = 'Job Error'
                        output_msg = "Not entered control flow for loading target table"
                        output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg}

                    if ret_rm <> 0 or ret_mv <> 0:
                        error = ret_rm if ret_mv == 0 else ret_mv
                        err_msg = "ERROR: HiveTxn: write_hive: Error with unix commnds"
                        status = 'Job Error'
                        output_msg = err_rm[0:3997] if ret_mv == 0 else err_mv[0:3997]
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR output_msg: ", output_msg)
                        if output_msg.find("_SUCCESS") == -1:
                            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg}
                            break
                        else:
                            error = 0
                            err_msg = ''
                            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Ignored ERROR for _SUCCESS file exists in the target")

                except Exception as e:
                    raise
                finally:
                    try:
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "table_unlock_sql: " + table_unlock_sql)
                        cur_hive.execute(table_unlock_sql)
                    except:
                        pass

                # if (load_type == self.LOAD_TYPE_INCREMENTAL or load_type == self.LOAD_TYPE_APPEND_ONLY) and partition_col_cnt > 0:
                #
                #     # Not in list of params that are allowed to be modified at runtime
                #     # cur_hive.execute("SET hive.hadoop.supports.splittable.combineinputformat = true")
                #     # cur_hive.execute("SET tez.grouping.min-size = 1073741824")
                #     # cur_hive.execute("SET tez.grouping.max-size = 2147483648")
                #     cur_hive.execute("SET mapreduce.input.fileinputformat.split.minsize = 2147483648")
                #     # cur_hive.execute("SET mapreduce.input.fileinputformat.split.maxsize = 2147483648")
                #     cur_hive.execute("SET hive.merge.smallfiles.avgsize = 2147483648")
                #     cur_hive.execute("SET hive.exec.reducers.bytes.per.reducer = 2147483648")
                #
                #     insert_ext_target_sql = "INSERT"
                #     if load_type == self.LOAD_TYPE_APPEND_ONLY:
                #         insert_ext_target_sql = insert_ext_target_sql + " INTO"
                #     else:
                #         insert_ext_target_sql = insert_ext_target_sql + " OVERWRITE"
                #     insert_ext_target_sql = insert_ext_target_sql + " TABLE `" + schema_name + "." + table_name + "` \
                #                     PARTITION ( " + partition_cols + " ) \
                #                     SELECT * FROM `" + schema_name + "." + tmp_table + "`"
                #
                #     # if load_type == self.LOAD_TYPE_APPEND_ONLY:
                #     #     insert_ext_target_sql = "INSERT INTO TABLE `" + schema_name + "." + table_name + "` \
                #     #                             PARTITION ( " + partition_cols + " ) \
                #     #                             SELECT * FROM `" + schema_name + "." + tmp_table + "`"
                #     #
                #     # else:
                #     #     insert_ext_target_sql = "INSERT OVERWRITE TABLE `" + schema_name + "." + table_name + "` \
                #     #                             PARTITION ( " + partition_cols + " ) \
                #     #                             SELECT * FROM `" + schema_name + "." + tmp_table + "`"
                #
                #     print (print_hdr + "insert_ext_target_sql: " + insert_ext_target_sql)
                #     cur_hive.execute(insert_ext_target_sql)

                if partition_col_cnt > 0:
                    repair_table_sql = 'MSCK REPAIR TABLE `' + schema_name + "." + table_name + "`"
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "repair_table_sql: " + repair_table_sql)
                    cur_hive.execute(repair_table_sql)

                    partition_cols_csv = ",".join(map(lambda s: '`' + s + '`', partition_cols.split(",")))
                    analyze_table_sql = 'ANALYZE TABLE `' + schema_name + "." + table_name + '` partition (' + partition_cols_csv + ') COMPUTE STATISTICS'
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "analyze_table_sql: " + analyze_table_sql)
                    cur_hive.execute(analyze_table_sql)
                else:
                    analyze_table_sql = 'ANALYZE TABLE `' + schema_name + "." + table_name + '` COMPUTE STATISTICS'
                    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "analyze_table_sql: " + analyze_table_sql)
                    cur_hive.execute(analyze_table_sql)

                # refresh_metadata_sql = 'REFRESH TABLE ' + schema_name + "." + table_name
                # print (print_hdr + "refresh_metadata_sql: " + refresh_metadata_sql)
                # cur_hive.execute(refresh_metadata_sql)

        except Exception as e:
            error = 2
            err_msg = "ERROR: HiveTxn: write_hive: while writing hive transactions"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg}
        finally:
            try:
                if conn_hive is not None:
                    conn_hive.close()
            except: pass

        if len(output) == 0:
            output = {"error": error, "err_msg": err_msg}
        return output


    def cleanup_hive(self):
        method_name = self.class_name + ": " + "cleanup_hive"
        print_hdr = "[" + method_name + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        conn_hive = None

        try:
            staging_path = self.config_list['misc_hdfsStagingPath']

            table_dlist = []
            if len(self.source_table_dlist) > 0:
                table_dlist.extend(self.source_table_dlist)
            if len(self.target_table_dlist) > 0:
                table_dlist.extend(self.target_table_dlist)

            if len(table_dlist) == 0:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "No tables to cleanup")
                return

            print (print_hdr + "table_dlist: ", table_dlist)
            if len(table_dlist) > 0:
                try:
                    conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                except Exception as e:
                    try:
                        conn_hive, cur_hive = dbConnectHive(self.config_list['tgt_db_hive_dbUrl2'], self.config_list['tgt_db_hive_dbPort'], self.config_list['tgt_db_hive_authMech'])
                    except Exception as e:
                        raise

                for table_dict in table_dlist:
                    try:
                        table = table_dict['table_name']
                        schema_name, table_name = table.split('.')
                        tmp_table = table_name + '_tmp' + str(self.run_id)
                        # tmp_path = staging_path + schema_name + '.db/' + tmp_table + '/'
                        # tmp_path = staging_path + schema_name + '/' + tmp_table + '/'
                        tmp_path = 'hdfs://getnamenode' + staging_path + schema_name + '/' + tmp_table + '/'

                        drop_hive_temp_ext_table = "DROP TABLE IF EXISTS `" + schema_name + "." + tmp_table + "`"
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "drop_hive_temp_ext_table: " + drop_hive_temp_ext_table)
                        cur_hive.execute(drop_hive_temp_ext_table)

                        # (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (tmp_path + "*")])
                        (ret_rm, out_rm, err_rm) = run_cmd(['hadoop', 'fs', '-rm', '-r', '-f', (tmp_path)])
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "removing files from tmp - STATUS: " + str(ret_rm))
                    except Exception as e:
                        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Ignored ERROR for table " + str(table) + ": ", traceback.format_exc())
        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Ignored ERROR: ", traceback.format_exc())
        finally:
            try:
                if conn_hive is not None:
                    conn_hive.close()
            except: pass


    def get_objects(self, path):
        method_name = self.class_name + ": " + "get_objects"
        print_hdr = "[" + method_name + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        error = 0
        err_msg = ''
        output = {}
        source_table_dlist = []
        target_table_dlist = []

        try:
            config = read_config([path + 'tables.properties'])

            if ((config == None) or (len(config.sections()) == 0)):
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Config file not available")
            else:
                for section in config.sections():
                    if section == 'source_tables':
                        for option in config.options(section):
                            o = config.get(section, option)
                            table, flag = map(lambda s: s.strip(), o.split(","))
                            source_table_dlist.append({'table_name': table, 'is_source_ddl': bool(flag)})

                    if section == 'target_tables':
                        for option in config.options(section):
                            o = config.get(section, option)
                            table, load_type, flag = map(lambda s: s.strip(), o.split(","))
                            target_table_dlist.append(
                                {'table_name': table, 'load_type': load_type, 's3_backed': bool(flag)})

                output = self.read_hive(source_table_dlist, target_table_dlist)

        except Exception as e:
            rows_inserted = 0
            rows_updated = 0
            rows_deleted = 0
            num_errors = 1
            error = 1
            err_msg = "ERROR: HiveTxn: get_objects: while getting objects"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                      "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}

        if len(output) == 0:
            output = {'error': error, 'err_msg': err_msg}
        return output


    def get_datasync_objects(self, data_path, table_name):
        method_name = self.class_name + ": " + "get_datasync_objects"
        self.data_path = data_path
        print_hdr = "[" + method_name + ": " + data_path + "] - "
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

        conn_metadata = None
        error = 0
        err_msg = ''
        output = {}

        rows_inserted = 0
        rows_updated = 0
        rows_deleted = 0
        num_errors = 1

        try:
            control_sql = "SELECT  distinct concat(concat(source_schemaname,'.'),source_tablename) as source_full_tablename, \
                            concat(concat(target_schemaname,'.'),target_tablename) as target_full_tablename, \
                            source_schemaname, source_tablename, target_schemaname, target_tablename, incremental_column, \
                            second_last_run_time, s3_backed, load_type \
                            FROM sync.control_table \
                            where 1 = 1 \
                                and concat(concat(target_schemaname,'.'),target_tablename) = '" + table_name + "' \
                                and status_flag = 'Y' \
                                and data_path = '" + data_path + "'"

            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "control_sql: " + control_sql)

            conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'], self.config_list['meta_db_dbUser'],
                                                    self.config_list['meta_db_dbUrl'], base64.b64decode(self.config_list['meta_db_dbPwd']))
            control_results = dbQuery(cur_metadata, control_sql)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "control_results: ", control_results)

            source_table_dlist = []
            target_table_dlist = []
            if len(control_results) > 0:

                if bool(control_results[0]['s3_backed']) and str(control_results[0]['load_type']) <> 'APPEND_ONLY':
                    error = 1
                    err_msg = "ERROR: : HiveTxn: get_datasync_objects: S3 target storage for source table " + str(control_results[0]['source_full_tablename']) + \
                              " not allowed for load_type " + str(control_results[0]['load_type'])
                    status = 'Job Error'
                    output_msg = "S3 target storage for source table " + str(control_results[0]['source_full_tablename']) + \
                                 " not allowed for load_type " + str(control_results[0]['load_type'])
                    output.update({'status': status, 'rows_inserted': rows_inserted, 'rows_updated': rows_updated,'rows_deleted': rows_deleted,
                                   'error': error, 'err_msg': err_msg, 'output_msg': output_msg})
                else:
                    if data_path.find("HDFS2MIR") <> -1 or data_path.find("SRC2Hive") <> -1 or data_path.find("Talend2Hive") <> -1 or data_path.find("KFK2Hive") <> -1:
                        target_table_dlist.append({'table_name': str(control_results[0]['target_full_tablename']), 'load_type': str(control_results[0]['load_type']), \
                                                   's3_backed': bool(control_results[0]['s3_backed'])})
                    else:
                        source_table_dlist.append({'table_name': str(control_results[0]['source_full_tablename']), 'load_type': str(control_results[0]['load_type']), \
                                                 'incremental_column': str(control_results[0]['incremental_column']), 'second_last_run_time': control_results[0]['second_last_run_time']})
                        target_table_dlist.append({'table_name': str(control_results[0]['target_full_tablename']), 'load_type': str(control_results[0]['load_type']), \
                                                   's3_backed': bool(control_results[0]['s3_backed'])})

                    output = self.read_hive(source_table_dlist, target_table_dlist)
            else:
                error = 2
                err_msg = "ERROR: HiveTxn: get_datasync_objects: No control data available"
                status = 'Job Error'
                output_msg = 'No control data available'
                output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg,"rows_inserted": rows_inserted, \
                          "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}

        except Exception as e:
            error = 3
            err_msg = "ERROR: HiveTxn: get_datasync_objects: while getting datasync objects"
            status = 'Job Error'
            output_msg = traceback.format_exc()
            output = {"error": error, "err_msg": err_msg, "status": status, "output_msg": output_msg, "rows_inserted": rows_inserted, \
                      "rows_updated": rows_updated, "rows_deleted": rows_deleted, "num_errors": num_errors}
        finally:
            if conn_metadata is not None:
                conn_metadata.close()

        if len(output) == 0:
            output = {'error': error, 'err_msg': err_msg}
        return output



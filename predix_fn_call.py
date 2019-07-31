import sys
sys.path.append("/apps/common")
from utils import dbConnect, dbQuery, read_config,txn_dbConnect
from auditLog import audit_logging
import traceback
import base64

def fn_call(load_id,fn_name):
    config              = read_config(['/apps/common/environ.properties'])
    env                 = config.get('branch', 'env')
    metastore_dbName    = config.get(env + '.meta_db', 'dbName')
    dbmeta_Url          = config.get(env + '.meta_db', 'dbUrl')
    dbmeta_User         = config.get(env + '.meta_db', 'dbUser')
    dbmeta_Pwd          = base64.b64decode(config.get(env + '.meta_db', 'dbPwd'))

    dbtgt_Url_predix_wto    = config.get(env + '.tgt_db_predix_wto', 'dbUrl')
    dbtgt_User_predix_wto   = config.get(env + '.tgt_db_predix_wto', 'dbUser')
    dbtgt_dbName_predix_wto = config.get(env + '.tgt_db_predix_wto', 'dbName')
    dbtgt_Pwd_predix_wto    = base64.b64decode(config.get(env + '.tgt_db_predix_wto', 'dbPwd'))
    dbtgt_dbName_port_wto   = config.get(env + '.tgt_db_predix_wto', 'dbPort')

    try:
        conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
        run_id_lists = dbQuery(cur_metadata, run_id_sql)
        run_id_list = run_id_lists[0]
        run_id = run_id_list['nextval']
        status= 'Job Started'
        plant_name = 'GE Transportation'
        system_name = 'WTO Predix'
        job_name = 'WTO Predix - Trigger DB Function'
        tablename = 'WTO Predix'
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename,status, '', 'Python', 0, 0, 0, 0, '',0, 0, '')
    except Exception as e:
        error = 1
        err_msg = "Error: connecting to logging database while making first audit entry"
        print err_msg
        # sendMail(emailSender, emailReceiver, err_msg, tablename, load_id, env, "ERROR","DataIKU Backup", '')
        return error, err_msg

    try:
        conn_target, cur_target = txn_dbConnect(dbtgt_dbName_predix_wto, dbtgt_User_predix_wto, dbtgt_Url_predix_wto,dbtgt_Pwd_predix_wto, dbtgt_dbName_port_wto)

    except Exception as e:
        error = 2
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = "Error while connecting to the Target Predix Database"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg

    try:
        if fn_name.find("(") <> -1 and fn_name.find(")") <> -1:
            fn_result = dbQuery(cur_target, "SELECT * FROM " + fn_name)
        else:
            fn_result = dbQuery(cur_target, "SELECT * FROM " + fn_name + "()")
        # print fn_result
        # print fn_result[0]['proc_wto_wheel_data']
    except Exception as e:
        error = 3
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = "Error while running the Predix Function"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        conn_metadata.close()
        conn_target.close()
        return error, err_msg

    if str(fn_result[0]['proc_wto_wheel_data']) == 'False' or str(fn_result[0]['proc_wto_wheel_data']) == '1':
        print "Function returned False in the Predix Database. Please check the function for more details"
        error = 4
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = "Function returned False in the Predix Database. Please check the function for more details"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        conn_metadata.close()
        conn_target.close()
        return error, err_msg

    status = 'Job Finished'
    err_msg = ''
    audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python', 0,0, 0, 0, '', 0, 0, '')
    conn_metadata.close()
    conn_target.close()

if __name__ == "__main__":
    load_id = sys.argv[1]
    fn_name = sys.argv[2]
    fn_call(load_id,fn_name)


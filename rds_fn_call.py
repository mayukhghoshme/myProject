import sys
sys.path.append("/apps/common")
from utils import dbConnect, dbQuery, read_config, txn_dbConnect
from auditLog import audit_logging
import traceback
import base64

def fn_call(fn_name, load_id=None, run_id=None):
    config              = read_config(['/apps/common/environ.properties'])
    env                 = config.get('branch', 'env')
    metastore_dbName    = config.get(env + '.meta_db', 'dbName')
    dbmeta_Url          = config.get(env + '.meta_db', 'dbUrl')
    dbmeta_User         = config.get(env + '.meta_db', 'dbUser')
    dbmeta_Pwd          = base64.b64decode(config.get(env + '.meta_db', 'dbPwd'))

    dbtgt_Url   = config.get(env + '.tgt_db_i360', 'dbUrl')
    dbtgt_User   = config.get(env + '.tgt_db_i360', 'dbUser')
    dbtgt_dbName = config.get(env + '.tgt_db_i360', 'dbName')
    dbtgt_Pwd    = base64.b64decode(config.get(env + '.tgt_db_i360', 'dbPwd'))

# Making the Job Started entry
    try:
        conn_metadata, cur_metadata = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        # status = 'Job Started'
        plant_name = 'GE Transportation'
        system_name = 'RDS'
        job_name = 'RDS - Trigger DB Function'
        tablename = fn_name
        # audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, '', 0, 0, '')
    except Exception as e:
        output_msg = traceback.format_exc()
        error = 1
        err_msg = "Error: Unable to generate LOAD ID"
        print err_msg, output_msg
        # sendMail(emailSender, emailReceiver, err_msg, tablename, load_id, env, "ERROR","DataIKU Backup", '')
        return error, err_msg

# Generating load id if it was not supplied
    try:
        if load_id is None:
            load_id_sql = "select nextval('sbdt.edl_load_id_seq')"
            load_id_lists = dbQuery(cur_metadata, load_id_sql)
            load_id_list = load_id_lists[0]
            load_id = load_id_list['nextval']
    except Exception as e:
        output_msg = traceback.format_exc()
        error = 1
        status = 'Job Error'
        err_msg = "Error: connecting to logging database while making first audit entry"
        print err_msg, output_msg
        audit_logging(cur_metadata, 0, 0, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg

    try:
        if run_id is None:
            run_id_sql = "select nextval('sbdt.edl_run_id_seq')"
            run_id_lists = dbQuery(cur_metadata, run_id_sql)
            run_id_list = run_id_lists[0]
            run_id = run_id_list['nextval']
    except Exception as e:
        error = 1
        err_msg = "Error: connecting to logging database while making second audit entry"
        print err_msg
        output_msg = traceback.format_exc()
        status = 'Job Error'
        audit_logging(cur_metadata, 0, 0, plant_name, system_name, job_name, tablename, status, '', 'Python', 0, 0, 0,0, err_msg, 0, 0, output_msg)
        return error, err_msg

    try:
        conn_target, cur_target = dbConnect(dbtgt_dbName, dbtgt_User, dbtgt_Url,dbtgt_Pwd)
    except Exception as e:
        error = 2
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = "Error while connecting to the Target  Database"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        return error, err_msg

    try:
        fn_name_list = fn_name.split(',')
        for fn_name in fn_name_list:
            status = 'Job Started'
            tablename = fn_name.split('(')[0]
            audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '','Python', 0, 0, 0, 0, '', 0, 0, '')
            if fn_name.find("(") <> -1 and fn_name.find(")") <> -1:
                fn_result = dbQuery(cur_target, "SELECT * FROM " + fn_name)
                print "Running SQL : SELECT * FROM " + fn_name
            else:
                fn_result = dbQuery(cur_target, "SELECT * FROM " + fn_name + "()")
                print "Running SQL : SELECT * FROM " + fn_name + "()"
            print fn_result
            print fn_result[0][fn_name.split('(')[0].split('.')[1]]
            for notice in conn_target.notices:
                print notice
            if str(fn_result[0][fn_name.split('(')[0].split('.')[1]]) == 'False' or str(fn_result[0][fn_name.split('(')[0].split('.')[1]]) == '1':
                print "Function returned False in the Target Database. Please check the function for more details"
                error = 4
                status = 'Job Error'
                output_msg = traceback.format_exc()
                err_msg = "Function returned False in the Target Database. Please check the function for more details"
                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '','Python', 0, 0, 0, 0, err_msg, 0, 0, output_msg)
                conn_metadata.close()
                conn_target.close()
                return error, err_msg
            else:
                status = 'Job Finished'
                error = 0
                err_msg = 'No Error'
                audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '','Python', 0, 0, 0, 0, '', 0, 0, '')

    except Exception as e:
        error = 3
        status = 'Job Error'
        output_msg = traceback.format_exc()
        err_msg = "Error while running the RDS Function"
        audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
        conn_metadata.close()
        conn_target.close()
        return error, err_msg

    # if str(fn_result[0][fn_name.split('(')[0].split('.')[1]]) == 'False' or str(fn_result[0][fn_name.split('(')[0].split('.')[1]]) == '1':
    #     print "Function returned False in the Target Database. Please check the function for more details"
    #     error = 4
    #     status = 'Job Error'
    #     output_msg = traceback.format_exc()
    #     err_msg = "Function returned False in the Target Database. Please check the function for more details"
    #     audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python',0, 0, 0, 0, err_msg, 0, 0, output_msg)
    #     conn_metadata.close()
    #     conn_target.close()
    #     return error, err_msg

    # status = 'Job Finished'
    # error = 0
    # err_msg = 'No Error'
    # audit_logging(cur_metadata, load_id, run_id, plant_name, system_name, job_name, tablename, status, '', 'Python', 0,0, 0, 0, '', 0, 0, '')
    conn_metadata.close()
    conn_target.close()
    return error, err_msg

if __name__ == "__main__":
    print len(sys.argv)

    if len(sys.argv) == 2:
        fn_name = sys.argv[1]
        err, err_msg = fn_call(fn_name)
    elif len(sys.argv) == 3:
        fn_name = sys.argv[1]
        load_id = sys.argv[2]
        err, err_msg = fn_call(fn_name, load_id)
    elif len(sys.argv) == 4:
        fn_name = sys.argv[1]
        load_id = sys.argv[2]
        run_id = sys.argv[3]
        err, err_msg = fn_call(fn_name, load_id, run_id)
    else:
        err = 10
        print "Incorrect number of arguments supplied. The correct format to execute the script is"
        print "python /apps/datasync/scripts/rds_fn_call.py <function_name> <load_id> <run_id>"
    sys.exit(err)


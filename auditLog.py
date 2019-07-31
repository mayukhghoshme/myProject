
import sys
import psycopg2
import traceback
import base64
sys.path.append('/apps/common/')
sys.path.append("/data/analytics/common/")
from utils import dbConnect, load_config


def get_cursor():

    # config = read_config(['/apps/common/environ.properties'])

    # if(config == None):
    #     return
    # # get the current branch (from local.properties)
    # env             = config.get('branch','env')
    #
    # # proceed to point everything at the 'branched' resources
    # metastore_dbName           = config.get(env+'.meta_db','dbName')
    # dbmeta_Url                 = config.get(env+'.meta_db','dbUrl')
    # dbmeta_User                = config.get(env+'.meta_db','dbUser')
    # dbmeta_Pwd                 = base64.b64decode(config.get(env+'.meta_db','dbPwd'))

    config_list = load_config()
    if ((config_list == None) or (len(config_list) == 0)):
        return
    try:
        # conn_metadata, cur    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
        conn_metadata, cur = dbConnect(config_list['meta_db_dbName'], config_list['meta_db_dbUser'], config_list['meta_db_dbUrl'], base64.b64decode(config_list['meta_db_dbPwd']))
    except Exception as e:
        err_msg = "Error connecting to database while fetching cursor"
        return
    return cur


def audit_logging (cur,load_id, run_id, plant_name, system_name , job_name, tablename, status, data_path, technology, rows_inserted, rows_updated, rows_deleted, number_of_errors, error_message ,source_row_count, target_row_count, error_category):


    if cur == '':
        print ("audiLog: audit_logging: Connection closed - reconnecting...")
        cur = get_cursor()

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


if __name__ == "__main__":
    audit_logging( sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7],sys.argv[8],sys.argv[9],sys.argv[10],sys.argv[11],sys.argv[12],sys.argv[13],sys.argv[14],sys.argv[15],sys.argv[16],sys.argv[17],sys.argv[18])

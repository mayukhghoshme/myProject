import ConfigParser
import sys
import psycopg2
sys.path.append("/apps/common/")
# from readCfg import read_config
from utils import read_config
import base64
#from incrUpdate import dbConnect
#from incrUpdate import dbQuery
from psycopg2.extras import RealDictCursor
sys.path.append("/apps/gp2hdp_sync/")


def dbConnect (db_parm, username_parm, host_parm, pw_parm):
    # Parse in connection information
    credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
    conn = psycopg2.connect(**credentials)
    conn.autocommit = True  # auto-commit each entry to the database
    print "Connected Successfully"
    conn.cursor_factory = RealDictCursor
    cur = conn.cursor()
    return conn, cur

def dbQuery (cur, query):
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def main():
    config = read_config(['/apps/common/environ.properties'])
    env             = config.get('branch','env')
    print env
    print config.get(env+'.meta_db','dbPwd')
    metastore_dbName           = config.get(env+'.meta_db','dbName')
    dbmeta_Url                 = config.get(env+'.meta_db','dbUrl')
    dbmeta_User                = config.get(env+'.meta_db','dbUser')
    dbmeta_Pwd                 = base64.b64decode(config.get(env+'.meta_db','dbPwd'))
    input_source_schema        = sys.argv[1]
    input_source_table         = sys.argv[2]
    input_timestamp            = sys.argv[3]


    conn_metadata, cur_metadata    = dbConnect(metastore_dbName, dbmeta_User, dbmeta_Url, dbmeta_Pwd)
    update_sql                     = "UPDATE sync.control_table SET hvr_last_processed_value = '" + input_timestamp + "' WHERE source_schemaname = '" + input_source_schema + "' AND  source_tablename = '" + input_source_table + "' and data_path = 'GP2HDFS'"
    print update_sql
    try:
        update                         = dbQuery(cur_metadata, update_sql)
    except Exception as e:
            print e

    conn_metadata.close()

if __name__ == "__main__":
    main()

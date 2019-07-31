
from pyspark import SparkConf, SparkContext
import sys

sys.path.append("/apps/common/")
sys.path.append("/data/analytics/common/")
sys.path.append("/apps/datasync/scripts/")
sys.path.append("/data/analytics/datasync/scripts/")

from file2db import File2DBSync
from utils import get_kerberos_token
from datetime import datetime as logdt

if __name__ == "__main__":

    table_name      = str(sys.argv[1])
    load_id         = int(sys.argv[2])
    run_id          = int(sys.argv[3])
    data_path       = str(sys.argv[4])
    v_timestamp     = sys.argv[5]

    APP_NAME        = 'datasync_spark_driver_' + table_name
    conf            = SparkConf().setAppName(APP_NAME)
    conf.set("spark.driver.maxResultSize", "5G")

    sc              = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[datasync_spark_driver: main] - Entered: " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id))

    error = 0
    err_msg = ""

    get_kerberos_token()
    file2db_sync = File2DBSync()
    output = file2db_sync.sync_hive_wrapper(sc, table_name, load_id, run_id, data_path, v_timestamp)

    error = output["error"]
    err_msg = output["err_msg"]
    if error is not None and error > 0:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[datasync_spark_driver: main] - ERROR: " + str(error) + " :: err_msg: " + err_msg)
    else:
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[datasync_spark_driver: main] - SUCCESS: " + data_path + ": " + str(load_id) + ": " + table_name + ": " + str(run_id))

    sys.exit(error)

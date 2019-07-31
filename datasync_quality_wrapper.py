
import sys
sys.path.append("/apps/common/")
from utils import load_config, sendMailHTML
from datetime import datetime as logdt
import traceback

if __name__ == "__main__":

    print_hdr = "[datasync_quality_wrapper: main] - "
    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Entered")

    config_list = load_config()

    emailReceiver              = config_list['email_dataQualityReceivers']

    error = 0

    if len(sys.argv) < 4:
        mail_subject = config_list['env'] + ": ERROR: Datasync Quality"
        output_msg = "ERROR: Mandatory input arguments not passed"
        print print_hdr + output_msg
        sendMailHTML(emailReceiver, mail_subject, output_msg)
        sys.exit(1)

    schema_system_name  = sys.argv[1]   # For Talend2Hive, Talend job will pass system_name from sync.control table
    data_path           = sys.argv[2]
    load_id             = sys.argv[3]

    load_type = 'None'
    if len(sys.argv) > 4:
        load_type = sys.argv[4]
        if load_type not in ['INCREMENTAL', 'FULL', 'None']:
            mail_subject = "ERROR: Datasync Quality: Unsupported Input"
            output_msg = "ERROR: Quality check for load_type: " + load_type + " not supported"
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            sendMailHTML(emailReceiver, mail_subject, output_msg)
            sys.exit(1)


    print_hdr = "[datasync_quality_wrapper: main: " + data_path + ": " + schema_system_name + ": " + str(load_id) + ": " + load_type + "] - "

    if data_path not in ['SRC2Hive','Talend2Hive','KFK2Hive','SQOOP2Hive','NiFi2Hive']:
        mail_subject = "ERROR: Datasync Quality: Unsupported Input"
        output_msg = "ERROR: Quality check for datapath: " + data_path + " not supported"
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(emailReceiver, mail_subject, output_msg)
        sys.exit(1)

    try:
        # import subprocess
        # cmd_args = ['python','/apps/datasync/scripts/datasync_quality.py',schema_name,data_path,load_id,load_type,
        #             '>>','/apps/datasync/logs/alert/datasync_quality.log','2>&1','&']
        # child_process = subprocess.Popen(cmd_args)
        # print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + 'Child Process Id: ' + str(child_process.pid))

        import os
        pid=os.fork()
        if pid==0:
            output_msg = "Inside child process - pid: " + str(pid)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)

            cmd_arg = 'python -u /apps/datasync/scripts/datasync_quality.py ' + schema_system_name + ' ' + data_path + ' ' + str(load_id) + ' ' + load_type + \
                        ' >> /apps/datasync/logs/alert/quality/' + data_path + '/datasync_quality-' + schema_system_name + '-$(date +\%Y-\%m).log 2>&1 &'
            print cmd_arg
            os.system(cmd_arg)
            exit()
        elif pid < 0:
            mail_subject = "ERROR: Datasync Quality: " + schema_system_name + ": " + data_path + ": " + str(load_id)
            output_msg = "ERROR: os.fork in WRAPPER generataed a negative pid: " + str(pid)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
            sendMailHTML(emailReceiver, mail_subject, output_msg)
            sys.exit(0)
        else:
            output_msg = "Inside parent process - pid: " + str(pid)
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)

    except Exception as e:
        mail_subject = "ERROR: Datasync Quality: " + schema_system_name + ": " + data_path + ": " + str(load_id)
        output_msg = "ERROR: Encountered error while running wrapper job" + "\n" + traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + output_msg)
        sendMailHTML(emailReceiver, mail_subject, output_msg)
        sys.exit(0)

    sys.exit(0)

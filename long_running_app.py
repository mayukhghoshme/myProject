import sys
sys.path.append("/apps/common")
from utils import run_cmd, load_config
import pandas as pd
import time
from smtplib import SMTPException
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import traceback
from datetime import datetime as logdt
import json
import requests

class long_running_app(object):
    def __init__(self):
        print "Starting the execution of the Class : ", self.__class__.__name__
        self.info_list = []
        self.config_list = load_config()

    def get_yarn_apps(self):
        try:
            (ret, out, err)     = run_cmd(['yarn', 'application', '-list', '-appStates', 'RUNNING'])
            job_file            = open("/tmp/job_list.txt","w+")
            job_file.write(out)
            job_file.close()
            print "Finished function 1 "
        except Exception as e:
            print e
            output_msg = traceback.format_exc()
            print output_msg

    def prep_job_file(self):
        # Open the file and read all the contents into a variable to remove the first line
        # The first line is extraneous
        try:
            source_file         = open("/tmp/job_list.txt","r")
            lines               = source_file.readlines()
            source_file.close()

            target_file         = open("/tmp/job_list.txt","w")
            target_file.write('\n'.join(lines[1:]))
            target_file.close()
            print "Finished function 2 "
        except Exception as e:
            print e
            output_msg = traceback.format_exc()
            print output_msg

    def get_app_id(self,yarn_host):
        try:
            from datetime import datetime, timedelta
            df = pd.read_csv("/tmp/job_list.txt", sep = '\t')
            for i in range(len(df)):
                ret,out,err             = run_cmd(['yarn','application','-status',df['                Application-Id'][i]])
                parse_out               = out.split('\n')
                start_time_var          = parse_out[7]
                col, val                = start_time_var.split(':')
                start_time              = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(val)/1000))
                start_time_in_datetime  = datetime.strptime(start_time,'%Y-%m-%d %H:%M:%S')
                if (datetime.now() - start_time_in_datetime > timedelta(seconds=1800)) and df['      User'][i].find('hive') == -1:
                    print "APPLICATION RED ALERT : ", df['                Application-Id'][i], df['      User'][i],datetime.now() - start_time_in_datetime
                    response            = requests.get("http://" + yarn_host +":8088/ws/v1/cluster/apps/"+df['                Application-Id'][i] +"")
                    app_details         = json.loads(response.text)
                    q_percentage        = app_details['app']['queueUsagePercentage']
                    tracking_url        = app_details['app']['trackingUrl']
                    dct                 = {"app_id":df['                Application-Id'][i],"app_name":df['    Application-Name'][i],"app_type":df['    Application-Type'][i],"queue":df['     Queue'][i], "queue_percentage_used":str(q_percentage), \
                                           "progress":df['       Progress'][i],"user_id":df['      User'][i],"run_time":str(datetime.now() - start_time_in_datetime),"track_url":tracking_url}
                    self.info_list.append(dct)

            print self.info_list
            return self.info_list
        except Exception as e:
            print e
            output_msg = traceback.format_exc()
            print output_msg

    def send_alert_mail(self, sender_parm, receivers_parm, mail_subject, lock_dlist):
        # print_hdr = "[" + self.class_name + ": send_alert_mail] - "
        sender = sender_parm
        receivers = receivers_parm.split(',')  # sendmail method expects receiver parameter as list

        # print (print_hdr + 'lock_dlist count: ' + str(len(lock_dlist)))
        message = MIMEMultipart("alternative")
        message['Subject'] = mail_subject
        message['From'] = sender_parm
        message['To'] = receivers_parm  # This attribute expects string and not list
        message.add_header('Content-Type', 'text/html')

        html_msg = """\

    <head>
      <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
      <title>Hive Lock Alert</title>
      <style type="text/css" media="screen">
        table{
            background-color: #AAD373;
            empty-cells:hide;
        }
        th{
            border: black 1px solid;
        }    
        td{
            background-color: white;
            border: black 1px solid;
        }
      </style>
    </head>
    <body>
        <table style="border: black 1px solid;">
            <tr>
                <th>Application ID</th>
                <th>Application Name</th>
                <th>Application type</th>
                <th>Queue</th>
                <th>Queue Used(%)</th>
                <th>User ID</th>
                <th>Run Time</th>
                <th>Progress</th> 
                <th>Tracking URL</th>                
            </tr> """


        for lock_dict in lock_dlist:
            html_msg = html_msg + """\
            <tr>
                <td>""" + lock_dict['app_id'] + """</td>
                <td>""" + lock_dict['app_name'] + """</td>
                <td>""" + lock_dict['app_type'] + """</td>
                <td>""" + lock_dict['queue'] + """</td>
                <td>""" + lock_dict['queue_percentage_used'] + """</td>
                <td>""" + lock_dict['user_id'] + """</td>
                <td>""" + lock_dict['run_time'] + """</td>
                <td>""" + lock_dict['progress'] + """</td>
                <td>""" + lock_dict['track_url'] + """</td>
                
            </tr>"""

        html_msg = html_msg + """\
        </table>
    </body>"""

        # print html_msg
        try:
            message.attach(MIMEText(html_msg, 'html'))
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, message.as_string())
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "Successfully sent email")
        except SMTPException:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ')  + "Error: unable to send email")
        except Exception as e:
            print (logdt.now().strftime(
                '[%Y-%m-%d %H:%M:%S] ') + "ERROR details: " + traceback.format_exc())
        finally:
            try:
                del message
                smtpObj.quit()
            except Exception as e:
                print (logdt.now().strftime(
                    '[%Y-%m-%d %H:%M:%S] ') + "ERROR details: " + traceback.format_exc())


if __name__ == "__main__":
    obj1 = long_running_app()
    obj1.get_yarn_apps()
    obj1.prep_job_file()
    application_list = obj1.get_app_id(obj1.config_list["yarn_host"])
    if len(application_list) > 0:
        obj1.send_alert_mail(obj1.config_list["email_sender"],obj1.config_list["email_receivers"],(obj1.config_list["env"].upper() + ": Long Running Applications Above 30 mins "),obj1.info_list)


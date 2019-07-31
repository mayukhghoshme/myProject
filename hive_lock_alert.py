
import sys
sys.path.append("/apps/common/")
from utils import load_config, dbConnectHive, dbConnect, dbQuery
from datetime import datetime, timedelta
import traceback
from smtplib import SMTPException
import smtplib
from datetime import datetime as logdt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import pprint
import base64

class HiveLockAlert(object):

    def __init__(self):
        self.class_name = self.__class__.__name__
        self.config_list = load_config()

    def format_lock_details (self, rows):
        lock_dlist = []

        # rows = [['eservice@gets_intf_lookup_values', 'SHARED'], ['LOCK_QUERYID:hive_20180919143353_44472724-63a0-404e-b690-c469545aac9e', None], ['LOCK_TIME:1537382035204', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:INSERT OVERWRITE TABLE `get_intg.service_sheet_v_tmp23923` SELECT * FROM `get_intg.service_sheet_v`', None], ['eservice@gets_lms_service_sheet', 'SHARED'], ['LOCK_QUERYID:hive_20180919143353_44472724-63a0-404e-b690-c469545aac9e', None], ['LOCK_TIME:1537382035204', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:INSERT OVERWRITE TABLE `get_intg.service_sheet_v_tmp23923` SELECT * FROM `get_intg.service_sheet_v`', None], ['eservice@gets_lms_service_status', 'SHARED'], ['LOCK_QUERYID:hive_20180919143353_44472724-63a0-404e-b690-c469545aac9e', None], ['LOCK_TIME:1537382035204', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:INSERT OVERWRITE TABLE `get_intg.service_sheet_v_tmp23923` SELECT * FROM `get_intg.service_sheet_v`', None], ['get_intg@service_sheet_v', 'SHARED'], ['LOCK_QUERYID:hive_20180919143353_44472724-63a0-404e-b690-c469545aac9e', None], ['LOCK_TIME:1537382035204', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:INSERT OVERWRITE TABLE `get_intg.service_sheet_v_tmp23923` SELECT * FROM `get_intg.service_sheet_v`', None], ['get_intg@service_sheet_v_tmp23923', 'EXCLUSIVE'], ['LOCK_QUERYID:hive_20180919143353_44472724-63a0-404e-b690-c469545aac9e', None], ['LOCK_TIME:1537382035204', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:INSERT OVERWRITE TABLE `get_intg.service_sheet_v_tmp23923` SELECT * FROM `get_intg.service_sheet_v`', None]]
        # rows = [['get_intg@item_fs_cost_glo_v', 'SHARED'], ['LOCK_QUERYID:hive_20180920105539_2372d2c7-fd83-430e-a044-5485eb0c2f6f', None], ['LOCK_TIME:1537455339565', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:SELECT MinMax.*, LastSale.LastUnitSalesPrice ', None], ['FROM', None], ['(SELECT rank () over (partition BY s.product_code,s.geography_code ORDER BY s.product_code,s.geography_code, CAST(YEAR(gl_effective_date) AS INT) DESC) AS rank,', None], ['  CAST(YEAR(gl_effective_date) AS INT)                                                                                                                       AS BIDYear,', None], ['  s.geography_code                                                                                                                                                        AS Region,', None], ['  s.product_code,', None], ['  ROUND(MAX(conv_unit_local_amount),4) AS MAX_conv_unit_local_amount,', None], ['  ROUND(MIN(conv_unit_local_amount),4) AS MIN_conv_unit_local_amount,', None], ['  ROUND(AVG(FC.FS_Cost),4)             AS FS_Cost,', None], ['  CASE', None], ['    WHEN SUM(quantity_invoiced) = 0', None], ['    THEN 0', None], ['    ELSE ROUND((SUM(conv_unit_local_amount*quantity_invoiced) / SUM(quantity_invoiced)),4)', None], ['  END AS AVG_conv_unit_local_amount,', None], ['  AVG(', None], ['  CASE', None], ['    WHEN (quantity_invoiced*conv_unit_local_amount)= 0', None], ['    THEN 0', None], ['    ELSE ROUND(( ((quantity_invoiced*conv_unit_local_amount) - (quantity_invoiced*FC.FS_Cost)) / (quantity_invoiced*conv_unit_local_amount) ),4)', None], ['  END) AS AVG_CM,', None], ['  ROUND(STDDEV(', None], ['  CASE', None], ['    WHEN (quantity_invoiced*conv_unit_local_amount)= 0', None], ['    THEN 0', None], ['    ELSE ( ((quantity_invoiced*conv_unit_local_amount) - (quantity_invoiced*FC.FS_Cost)) / (quantity_invoiced*conv_unit_local_amount) )', None], ['  END),4)                                                AS SD_CM,', None], ['  ROUND(SUM(quantity_invoiced),4)                        AS Total_Quantity_Invoiced,', None], ['  ROUND(SUM(quantity_invoiced*conv_unit_local_amount),4) AS Total_Sales,', None], ['  ROUND(SUM(quantity_invoiced*FC.FS_Cost),4)             AS Total_Actual_Cost', None], ['FROM get_intg.Part_Sales_Data S', None], ['LEFT JOIN', None], ['  (SELECT a.product_number,', None], ['    a.unit_item_cost AS FS_COST', None], ['  FROM get_intg.item_fs_cost_glo_v a ,', None], ['    (SELECT product_number ,', None], ['      MAX(fiscal_year) fiscal_year', None], ['    FROM get_intg.item_fs_cost_glo_v', None], ['    WHERE fiscal_year <= YEAR(current_date)', None], ['    GROUP BY product_number', None], ['    ) b', None], ['  WHERE a.product_number=b.product_number', None], ['  AND a.fiscal_year     =b.fiscal_year', None], ['  ) FC', None], ['ON s.product_code                             = fc.product_number', None], ['WHERE YEAR(s.gl_effective_date) >= (YEAR(current_date) -4)', None], ["AND Account_Code NOT                                                       IN ('4010501000','4010512000')", None], ['AND (S.product_code                          IS NOT NULL', None], ["AND S.product_code                           <> 'UNASSIGNED')", None], ['AND s.conv_unit_local_amount                  > 0', None], ['AND s.Quantity_Invoiced                       >0', None], ['AND fc.FS_Cost                               IS NOT NULL', None], ['GROUP BY CAST(YEAR(gl_effective_date) AS INT),', None], ['  s.product_code,', None], ['  s.geography_code', None], [') MinMax,', None], ['( SELECT DISTINCT PS.product_code                                                                                                                                  AS product_code1 ,', None], ['  PS.geography_code                                                                                                                                                AS Region,', None], ['  PS.conv_unit_local_amount                                                                                                                                        AS LastUnitSalesPrice,', None], ['  rank () over (partition BY PS.product_code,PS.geography_code ORDER BY ', None], ['  PS.product_code,PS.geography_code, PS.gl_effective_date DESC, PS.TRANSACTION_ID DESC) AS Rank,', None], ['  YEAR(PS.gl_effective_date)                                                                                                                          AS GL_YEAR', None], ['FROM get_intg.Part_Sales_Data PS', None], ['WHERE YEAR(PS.gl_effective_date) >= (YEAR (current_date) -4)', None], ["AND PS.Account_Code NOT                                                     IN ('4010501000','4010512000')", None], ['AND (PS.product_code                          IS NOT NULL', None], ["AND PS.product_code                           <> 'UNASSIGNED')", None], ['AND PS.conv_unit_local_amount                  > 0', None], ['AND Ps.Quantity_Invoiced                       >0', None], [') LastSale WHERE MinMax.product_code           = LastSale.product_code1 AND MinMax.Region = LastSale.Region AND MinMax.rank = LastSale.rank AND MinMax.rank =1', None], ['get_intg@part_sales_data', 'SHARED'], ['LOCK_QUERYID:hive_20180920105539_2372d2c7-fd83-430e-a044-5485eb0c2f6f', None], ['LOCK_TIME:1537455339565', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:SELECT MinMax.*, LastSale.LastUnitSalesPrice ', None], ['FROM', None], ['(SELECT rank () over (partition BY s.product_code,s.geography_code ORDER BY s.product_code,s.geography_code, CAST(YEAR(gl_effective_date) AS INT) DESC) AS rank,', None], ['  CAST(YEAR(gl_effective_date) AS INT)                                                                                                                       AS BIDYear,', None], ['  s.geography_code                                                                                                                                                        AS Region,', None], ['  s.product_code,', None], ['  ROUND(MAX(conv_unit_local_amount),4) AS MAX_conv_unit_local_amount,', None], ['  ROUND(MIN(conv_unit_local_amount),4) AS MIN_conv_unit_local_amount,', None], ['  ROUND(AVG(FC.FS_Cost),4)             AS FS_Cost,', None], ['  CASE', None], ['    WHEN SUM(quantity_invoiced) = 0', None], ['    THEN 0', None], ['    ELSE ROUND((SUM(conv_unit_local_amount*quantity_invoiced) / SUM(quantity_invoiced)),4)', None], ['  END AS AVG_conv_unit_local_amount,', None], ['  AVG(', None], ['  CASE', None], ['    WHEN (quantity_invoiced*conv_unit_local_amount)= 0', None], ['    THEN 0', None], ['    ELSE ROUND(( ((quantity_invoiced*conv_unit_local_amount) - (quantity_invoiced*FC.FS_Cost)) / (quantity_invoiced*conv_unit_local_amount) ),4)', None], ['  END) AS AVG_CM,', None], ['  ROUND(STDDEV(', None], ['  CASE', None], ['    WHEN (quantity_invoiced*conv_unit_local_amount)= 0', None], ['    THEN 0', None], ['    ELSE ( ((quantity_invoiced*conv_unit_local_amount) - (quantity_invoiced*FC.FS_Cost)) / (quantity_invoiced*conv_unit_local_amount) )', None], ['  END),4)                                                AS SD_CM,', None], ['  ROUND(SUM(quantity_invoiced),4)                        AS Total_Quantity_Invoiced,', None], ['  ROUND(SUM(quantity_invoiced*conv_unit_local_amount),4) AS Total_Sales,', None], ['  ROUND(SUM(quantity_invoiced*FC.FS_Cost),4)             AS Total_Actual_Cost', None], ['FROM get_intg.Part_Sales_Data S', None], ['LEFT JOIN', None], ['  (SELECT a.product_number,', None], ['    a.unit_item_cost AS FS_COST', None], ['  FROM get_intg.item_fs_cost_glo_v a ,', None], ['    (SELECT product_number ,', None], ['      MAX(fiscal_year) fiscal_year', None], ['    FROM get_intg.item_fs_cost_glo_v', None], ['    WHERE fiscal_year <= YEAR(current_date)', None], ['    GROUP BY product_number', None], ['    ) b', None], ['  WHERE a.product_number=b.product_number', None], ['  AND a.fiscal_year     =b.fiscal_year', None], ['  ) FC', None], ['ON s.product_code                             = fc.product_number', None], ['WHERE YEAR(s.gl_effective_date) >= (YEAR(current_date) -4)', None], ["AND Account_Code NOT                                                       IN ('4010501000','4010512000')", None], ['AND (S.product_code                          IS NOT NULL', None], ["AND S.product_code                           <> 'UNASSIGNED')", None], ['AND s.conv_unit_local_amount                  > 0', None], ['AND s.Quantity_Invoiced                       >0', None], ['AND fc.FS_Cost                               IS NOT NULL', None], ['GROUP BY CAST(YEAR(gl_effective_date) AS INT),', None], ['  s.product_code,', None], ['  s.geography_code', None], [') MinMax,', None], ['( SELECT DISTINCT PS.product_code                                                                                                                                  AS product_code1 ,', None], ['  PS.geography_code                                                                                                                                                AS Region,', None], ['  PS.conv_unit_local_amount                                                                                                                                        AS LastUnitSalesPrice,', None], ['  rank () over (partition BY PS.product_code,PS.geography_code ORDER BY ', None], ['  PS.product_code,PS.geography_code, PS.gl_effective_date DESC, PS.TRANSACTION_ID DESC) AS Rank,', None], ['  YEAR(PS.gl_effective_date)                                                                                                                          AS GL_YEAR', None], ['FROM get_intg.Part_Sales_Data PS', None], ['WHERE YEAR(PS.gl_effective_date) >= (YEAR (current_date) -4)', None], ["AND PS.Account_Code NOT                                                     IN ('4010501000','4010512000')", None], ['AND (PS.product_code                          IS NOT NULL', None], ["AND PS.product_code                           <> 'UNASSIGNED')", None], ['AND PS.conv_unit_local_amount                  > 0', None], ['AND Ps.Quantity_Invoiced                       >0', None], [') LastSale WHERE MinMax.product_code           = LastSale.product_code1 AND MinMax.Region = LastSale.Region AND MinMax.rank = LastSale.rank AND MinMax.rank =1', None], ['odw@item_fs_cost', 'SHARED'], ['LOCK_QUERYID:hive_20180920105539_2372d2c7-fd83-430e-a044-5485eb0c2f6f', None], ['LOCK_TIME:1537455339565', None], ['LOCK_MODE:IMPLICIT', None], ['LOCK_QUERYSTRING:SELECT MinMax.*, LastSale.LastUnitSalesPrice ', None], ['FROM', None], ['(SELECT rank () over (partition BY s.product_code,s.geography_code ORDER BY s.product_code,s.geography_code, CAST(YEAR(gl_effective_date) AS INT) DESC) AS rank,', None], ['  CAST(YEAR(gl_effective_date) AS INT)                                                                                                                       AS BIDYear,', None], ['  s.geography_code                                                                                                                                                        AS Region,', None], ['  s.product_code,', None], ['  ROUND(MAX(conv_unit_local_amount),4) AS MAX_conv_unit_local_amount,', None], ['  ROUND(MIN(conv_unit_local_amount),4) AS MIN_conv_unit_local_amount,', None], ['  ROUND(AVG(FC.FS_Cost),4)             AS FS_Cost,', None], ['  CASE', None], ['    WHEN SUM(quantity_invoiced) = 0', None], ['    THEN 0', None], ['    ELSE ROUND((SUM(conv_unit_local_amount*quantity_invoiced) / SUM(quantity_invoiced)),4)', None], ['  END AS AVG_conv_unit_local_amount,', None], ['  AVG(', None], ['  CASE', None], ['    WHEN (quantity_invoiced*conv_unit_local_amount)= 0', None], ['    THEN 0', None], ['    ELSE ROUND(( ((quantity_invoiced*conv_unit_local_amount) - (quantity_invoiced*FC.FS_Cost)) / (quantity_invoiced*conv_unit_local_amount) ),4)', None], ['  END) AS AVG_CM,', None], ['  ROUND(STDDEV(', None], ['  CASE', None], ['    WHEN (quantity_invoiced*conv_unit_local_amount)= 0', None], ['    THEN 0', None], ['    ELSE ( ((quantity_invoiced*conv_unit_local_amount) - (quantity_invoiced*FC.FS_Cost)) / (quantity_invoiced*conv_unit_local_amount) )', None], ['  END),4)                                                AS SD_CM,', None], ['  ROUND(SUM(quantity_invoiced),4)                        AS Total_Quantity_Invoiced,', None], ['  ROUND(SUM(quantity_invoiced*conv_unit_local_amount),4) AS Total_Sales,', None], ['  ROUND(SUM(quantity_invoiced*FC.FS_Cost),4)             AS Total_Actual_Cost', None], ['FROM get_intg.Part_Sales_Data S', None], ['LEFT JOIN', None], ['  (SELECT a.product_number,', None], ['    a.unit_item_cost AS FS_COST', None], ['  FROM get_intg.item_fs_cost_glo_v a ,', None], ['    (SELECT product_number ,', None], ['      MAX(fiscal_year) fiscal_year', None], ['    FROM get_intg.item_fs_cost_glo_v', None], ['    WHERE fiscal_year <= YEAR(current_date)', None], ['    GROUP BY product_number', None], ['    ) b', None], ['  WHERE a.product_number=b.product_number', None], ['  AND a.fiscal_year     =b.fiscal_year', None], ['  ) FC', None], ['ON s.product_code                             = fc.product_number', None], ['WHERE YEAR(s.gl_effective_date) >= (YEAR(current_date) -4)', None], ["AND Account_Code NOT                                                       IN ('4010501000','4010512000')", None], ['AND (S.product_code                          IS NOT NULL', None], ["AND S.product_code                           <> 'UNASSIGNED')", None], ['AND s.conv_unit_local_amount                  > 0', None], ['AND s.Quantity_Invoiced                       >0', None], ['AND fc.FS_Cost                               IS NOT NULL', None], ['GROUP BY CAST(YEAR(gl_effective_date) AS INT),', None], ['  s.product_code,', None], ['  s.geography_code', None], [') MinMax,', None], ['( SELECT DISTINCT PS.product_code                                                                                                                                  AS product_code1 ,', None], ['  PS.geography_code                                                                                                                                                AS Region,', None], ['  PS.conv_unit_local_amount                                                                                                                                        AS LastUnitSalesPrice,', None], ['  rank () over (partition BY PS.product_code,PS.geography_code ORDER BY ', None], ['  PS.product_code,PS.geography_code, PS.gl_effective_date DESC, PS.TRANSACTION_ID DESC) AS Rank,', None], ['  YEAR(PS.gl_effective_date)                                                                                                                          AS GL_YEAR', None], ['FROM get_intg.Part_Sales_Data PS', None], ['WHERE YEAR(PS.gl_effective_date) >= (YEAR (current_date) -4)', None], ["AND PS.Account_Code NOT                                                     IN ('4010501000','4010512000')", None], ['AND (PS.product_code                          IS NOT NULL', None], ["AND PS.product_code                           <> 'UNASSIGNED')", None], ['AND PS.conv_unit_local_amount                  > 0', None], ['AND Ps.Quantity_Invoiced                       >0', None], [') LastSale WHERE MinMax.product_code           = LastSale.product_code1 AND MinMax.Region = LastSale.Region AND MinMax.rank = LastSale.rank AND MinMax.rank =1', None]]

        # if rows:
        #     for idx, row in enumerate(rows):
        #         # print idx, idx % 5, row
        #         if (idx % 5 == 0):
        #             lock_dict['table_name'] = row[0].replace('@','.')
        #             lock_dict['lock_type'] = row[1]
        #         if (idx % 5 == 1):
        #             lock_dict['lock_query_id'] = row[0].split(':')[1]
        #         if (idx % 5 == 2):
        #             lock_dict['lock_datetime'] = (datetime.fromtimestamp(int(row[0].split(':')[1])/1000)).strftime('%Y-%m-%d %H:%M:%S')
        #             lock_duration = ((datetime.now() - datetime.fromtimestamp(int(row[0].split(':')[1])/1000)).seconds)/60
        #             lock_dict['current_lock_duration'] = str(lock_duration) + ' mins'
        #         if (idx % 5 == 3):
        #             lock_dict['lock_mode'] = row[0].split(':')[1]
        #         if (idx % 5 == 4):
        #             lock_dict['lock_query_string'] = row[0].split(':')[1]
        #             print lock_dict
        #             if lock_duration > LOCK_TIME_THESHOLD_MINS:
        #                 lock_dlist.append(lock_dict)
        #             lock_dict = {}

        if rows:
            lock_dict = {}
            lock_duration = 0
            lock_query_string = None
            lock_query_id = None

            LOCK_TIME_THESHOLD_MINS = int(self.config_list['misc_lockDurationThresholdMins'])

            rows_len = len(rows)
            for idx, row in enumerate(rows):
                if row[1] <> None:
                    if idx > 0:
                        lock_dict['lock_query_string'] = lock_query_string
                        print lock_dict
                        if lock_duration > LOCK_TIME_THESHOLD_MINS and lock_dict['table_name'].find('=') == -1:
                            lock_dict['lock_query_user'] = self.get_hive_query_user(lock_query_id)
                            lock_dict['waiting_job'] = ""
                            lock_dict['waiting_job_start_time'] = ""
                            lock_dlist.append(lock_dict)
                        lock_dict = {}
                        lock_query_id = None

                    lock_dict['table_name'] = row[0].replace('@','.')
                    lock_dict['lock_type'] = row[1]
                    lock_query_string = None
                elif row[0].find('LOCK_QUERYID:') <> -1:
                    lock_query_id = row[0].split(':')[1]
                    lock_dict['lock_query_id'] = lock_query_id
                elif row[0].find('LOCK_TIME:') <> -1:
                    lock_dict['lock_datetime'] = (datetime.fromtimestamp(int(row[0].split(':')[1])/1000)).strftime('%Y-%m-%d %H:%M:%S')
                    lock_duration = ((datetime.now() - datetime.fromtimestamp(int(row[0].split(':')[1])/1000)).seconds)/60
                    lock_dict['current_lock_duration'] = str(lock_duration)
                elif row[0].find('LOCK_MODE:') <> -1:
                    lock_dict['lock_mode'] = row[0].split(':')[1]
                elif row[0].find('LOCK_QUERYSTRING:') <> -1:
                    lock_query_string = row[0].split(':')[1]
                else:
                    lock_query_string = lock_query_string + row[0]

                if idx == rows_len - 1:
                    lock_dict['lock_query_string'] = lock_query_string
                    print lock_dict
                    if lock_duration > LOCK_TIME_THESHOLD_MINS and lock_dict['table_name'].find('=') == -1:
                        lock_dict['lock_query_user'] = self.get_hive_query_user(lock_query_id)
                        lock_dict['waiting_job'] = ""
                        lock_dict['waiting_job_start_time'] = ""
                        lock_dlist.append(lock_dict)

        # print lock_dlist
        return lock_dlist


    def get_hive_query_user(self, hive_query_id):
        print_hdr = "[" + self.class_name + ": get_hive_query_user] - "
        query_user = None
        ambari_api_url = None

        try:
            ambari_api_url = 'http://' + self.config_list['ambari_host'] + '/api/v1/views/TEZ/versions/' + self.config_list['ambari_tezViewVersion'] + '/instances/TEZ_CLUSTER_INSTANCE/resources/atsproxy/ws/v1/timeline/HIVE_QUERY_ID/' + hive_query_id
            auth_values = (self.config_list['ambari_authUser'], base64.b64decode(self.config_list['ambari_authPassword']))

            response = requests.get(ambari_api_url, auth=auth_values)
            if response.status_code == 200:
                response_dict = response.json()
                query_user = ((response_dict['primaryfilters'])['user'])[0]
            else:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  print_hdr + "ERROR - Encountered HTTP Response " +
                       str(response.status_code) + " for " + str(ambari_api_url) + "\n")
                pprint.pprint(response.json())
                print("")
                query_user = 'HTTP ' + str(response.status_code)

        except Exception as e:
            err_msg = traceback.format_exc()
            print ('ambari_api_url: ' + str(ambari_api_url))
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  print_hdr + "ERROR details: " + err_msg)
        return str(query_user)


    def get_waiting_job(self, lock_dlist):
        print_hdr = "[" + self.class_name + ": get_waiting_job] - "
        conn_metadata = None

        try:
            table_list = []
            results = []
            for lock_dict in lock_dlist:
                if lock_dict['table_name'].find('=') == -1:
                    table_list.append(lock_dict['table_name'])
            table_distinct_list = list(set(table_list))

            if table_distinct_list:
                table_filter_clause = ",".join("'" + l + "'" for l in table_distinct_list)

                conn_metadata, cur_metadata = dbConnect(self.config_list['meta_db_dbName'], self.config_list['meta_db_dbUser'],
                                                        self.config_list['meta_db_dbUrl'], base64.b64decode(self.config_list['meta_db_dbPwd']))

                log_sql = "select job_key,table_name,to_char(max_start_time,'YYYY-MM-DD HH24:MI:SS') as start_time \
from ( \
    select plant_name ||' : '|| data_path||' : '||job_name||' : '||load_id||' : '||run_id as job_key,table_name,status,log_time, \
        max(log_time) over (partition by table_name) as max_start_time \
    from sbdt.edl_log \
    where 1 = 1 \
        and log_time > (current_timestamp - INTERVAL '1 day') \
        and plant_name not in ('TRANSPORTATION') \
        and (upper(data_path) not like '%2GP' or upper(data_path) not like '%2RDS' or upper(data_path) not like '%2PREDIX') \
        and table_name in (" + table_filter_clause + ") \
        and table_name is not null and length(trim(table_name)) > 0 and table_name <> 'NA') T1 \
where 1 = 1 \
        and log_time = max_start_time \
        and upper(status) like '%START%'"

                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "log_sql: " + log_sql)

                results = dbQuery(cur_metadata, log_sql)
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "results: ", results)

            for lock_dict in lock_dlist:
                if len(results) > 0:
                    for result in results:
                        if (result['table_name'] == lock_dict['table_name']) and (datetime.strptime(result['start_time'], '%Y-%m-%d %H:%M:%S') >= \
                                (datetime.strptime(lock_dict['lock_datetime'], '%Y-%m-%d %H:%M:%S') - timedelta(minutes=30))):
                            lock_dict['waiting_job'] = result['job_key']
                            lock_dict['waiting_job_start_time'] = result['start_time']

        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR details: " + traceback.format_exc())
        finally:
            if conn_metadata is not None and not conn_metadata.closed:
                conn_metadata.close()
            return lock_dlist


    def send_alert_mail(self, sender_parm, receivers_parm, mail_subject, lock_dlist):
        print_hdr = "[" + self.class_name + ": send_alert_mail] - "
        sender          = sender_parm
        receivers       = receivers_parm.split(',')     # sendmail method expects receiver parameter as list

        print (print_hdr + 'lock_dlist count: ' + str(len(lock_dlist)))
        message = MIMEMultipart("alternative")
        message['Subject'] = mail_subject
        message['From'] = sender_parm
        message['To'] = receivers_parm   # This attribute expects string and not list
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
                <th>Table Name</th>
                <th>Lock Type</th>
                <th>Lock User</th>
                <th>Lock Datetime</th>
                <th>Current Lock Duration</th>
                <th>Lock Mode</th>
                <th>Lock Query Id</th>
                <th>Lock Query</th>
                <th>Waiting Job</th>
                <th>Waiting Job Start Datetime</th>
            </tr> """

        for lock_dict in lock_dlist:
            html_msg = html_msg + """\
            <tr>
                <td>""" + lock_dict['table_name'] + """</td>
                <td>""" + lock_dict['lock_type'] + """</td>
                <td>""" + lock_dict['lock_query_user'] + """</td>
                <td>""" + lock_dict['lock_datetime'] + """</td>
                <td>""" + lock_dict['current_lock_duration'] + """ mins</td>
                <td>""" + lock_dict['lock_mode'] + """</td>
                <td>""" + lock_dict['lock_query_id'] + """</td>
                <td>""" + lock_dict['lock_query_string'] + """</td>
                <td>""" + lock_dict['waiting_job'] + """</td>
                <td>""" + lock_dict['waiting_job_start_time'] + """</td>
            </tr>"""

        html_msg = html_msg + """\
        </table>
    </body>"""

        # print html_msg
        try:
            message.attach(MIMEText(html_msg, 'html'))
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, message.as_string())
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Successfully sent email")
        except SMTPException:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Error: unable to send email")
        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR details: " + traceback.format_exc())
        finally:
            try:
                del message
                smtpObj.quit()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR details: " + traceback.format_exc())


    def send_error_mail(self, sender_parm, receivers_parm, mail_subject, mail_body_header):
        print_hdr = "[" + self.class_name + ": send_error_mail] - "
        sender          = sender_parm
        receivers       = receivers_parm.split(',')     # sendmail method expects receiver parameter as list

        message = MIMEMultipart("alternative")
        message['Subject'] = mail_subject
        message['From'] = sender_parm
        message['To'] = receivers_parm   # This attribute expects string and not list
        message.add_header('Content-Type', 'text/html')

        html_msg = """\
    
    <head></head>
    <body>
    %s  
    =====================================================================================================================================================================
    <br>
    Check log for more details  
    </body>"""%(mail_body_header)

        # print html_msg
        try:
            message.attach(MIMEText(html_msg, 'html'))
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, message.as_string())
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Successfully sent email")
        except SMTPException:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "Error: unable to send email")
        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR details: " + traceback.format_exc())
        finally:
            try:
                del message
                smtpObj.quit()
            except Exception as e:
                print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + print_hdr + "ERROR details: " + traceback.format_exc())


if __name__ == "__main__":

    print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[hive_lock_alert: main] - Entered")
    conn_hive = None
    hiveLockAlert = HiveLockAlert()
    emailSender = hiveLockAlert.config_list['email_sender']
    emailReceiver = hiveLockAlert.config_list['email_receivers']

    rows = []
    try:
        # LOCK_TIME_THESHOLD_MINS = int(config_list['misc_lockDurationThresholdMins'])

        try:
            conn_hive, cur_hive = dbConnectHive(hiveLockAlert.config_list['tgt_db_hive_dbUrl'], hiveLockAlert.config_list['tgt_db_hive_dbPort'],
                                                hiveLockAlert.config_list['tgt_db_hive_authMech'])
        except Exception as e:
            try:
                conn_hive, cur_hive = dbConnectHive(hiveLockAlert.config_list['tgt_db_hive_dbUrl2'], hiveLockAlert.config_list['tgt_db_hive_dbPort'],
                                                    hiveLockAlert.config_list['tgt_db_hive_authMech'])
            except Exception as e:
                raise

        cur_hive.execute("show locks extended")
        rows = cur_hive.fetchall()
        # print rows

        lock_dlist = hiveLockAlert.format_lock_details(rows)

        if len(lock_dlist) > 0:
            lock_dlist_out = hiveLockAlert.get_waiting_job(lock_dlist)
            mail_subject = str(hiveLockAlert.config_list['env']).upper() + ': WARNING: ' + 'HIVE Lock Alert above ' + hiveLockAlert.config_list['misc_lockDurationThresholdMins'] + ' mins'
            hiveLockAlert.send_alert_mail(emailSender, emailReceiver, mail_subject, lock_dlist_out)
        else:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[hive_lock_alert: main] - No Hive Lock available for more than " + hiveLockAlert.config_list['misc_lockDurationThresholdMins'] +  " mins")

    except Exception as e:
        err_msg = traceback.format_exc()
        print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') +  "[hive_lock_alert: main] - ERROR details: " + err_msg)
        print rows
        try:
            mail_subject = str(hiveLockAlert.config_list['env']).upper() + ': ERROR: ' + 'HIVE Lock Alert processing failed'
            mail_body_header = err_msg
            hiveLockAlert.send_error_mail(emailSender, emailReceiver, mail_subject, mail_body_header)
        except Exception as e:
            print (logdt.now().strftime('[%Y-%m-%d %H:%M:%S] ') + "[hive_lock_alert: main] - ERROR in sending error mail: " + traceback.format_exc())
    finally:
        print("\n================================================================================================================================================================\n")
        try:
            if conn_hive is not None:
                conn_hive.close()
        except: pass

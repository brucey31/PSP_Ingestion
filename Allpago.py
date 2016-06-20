__author__ = 'brucepannaman'

import paramiko
import configparser
from datetime import date, timedelta
from subprocess import call, check_output
import os
import psycopg2

config = configparser.ConfigParser()
ini = config.read('conf2.ini')

USERNAME = config.get('Allpago Keys', 'user')
PASSWORD = config.get('Allpago Keys', 'pass')
SERVER = config.get('Allpago Keys', 'server')
PORT = config.get('Allpago Keys', 'port')
AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')

start_date = date(2016, 6, 2)
end_date = date.today() - timedelta(days=1)

countries = ['Brazil', 'Mexico']

host = SERVER
port = PORT
transport = paramiko.Transport(host, port)
password = PASSWORD
username = USERNAME
transport.connect(username=username, password=password)

sftp = paramiko.SFTPClient.from_transport(transport)


while start_date <= end_date:

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/PSP_Reports/Allpago/New_Files/%s" % start_date])

    if len(rs) > 1:
        print "Files Exists for %s \n Moving on ;-)" % start_date

    else:

        for country in countries:

            print "Retrieving data for %s on %s" % (country, start_date)

            sftp.get('allpago/%s_Custom_report_BIP Extract_%s.csv' %(country, start_date), '%s_Custom_report_BIP Extract_%s.csv' %(country, start_date))

            print "Uploading payment for %s on %s to s3" % (country, start_date)
            call(["s3cmd", "put", '%s_Custom_report_BIP Extract_%s.csv' % (country, start_date),'s3://bibusuu/PSP_Reports/Allpago/New_Files/%s/%s_Custom_report_BIP Extract_%s.csv' %(start_date, country, start_date)])

            print "Removing Local File"
            os.remove('%s_Custom_report_BIP Extract_%s.csv' %(country, start_date))

    start_date = start_date + timedelta(days=1)

sftp.close()
transport.close()

conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

print "Creating new table \n Allpago_raw_2"
cursor.execute("create table allpago_raw2(Unique_Id varchar(100),Given_Name varchar(100),Company_Name varchar(100),Customer_Salutation varchar(100),Title varchar(100),Country varchar(100),State varchar(100),City varchar(100),Street varchar(100),ZipCode varchar(100),Channel_Name varchar(100),CPF varchar(100),Website varchar(100),Email varchar(100),Mobile varchar(100),Phone varchar(100),Ip varchar(100),Country_Code varchar(100),Region_Code varchar(100),Region_Name varchar(100),City_Name varchar(100),Memo_Value varchar(100),Memo_Name varchar(100),Memo_CPF varchar(100),Memo_Bank_Name varchar(100),Memo_Account_Number varchar(100),Memo_Agency_Number varchar(100),Merchant_Name varchar(100),Reason_Code_Meaning varchar(100),Return_Code_Meaning varchar(100),Status_Code_Meaning varchar(100),TM_Import_Date varchar(100),TM_File_Ddate varchar(100),TM_Policy_Score varchar(100),TM_Risk_Rating varchar(100),TM_True_Ip varchar(100),TM_True_Ip_City varchar(100),TM_True_Ip_Geo varchar(100),TM_True_Ip_Isp varchar(100),TM_True_Ip_Isp1 varchar(100),TM_Ua_Browser varchar(100),TM_Ua_Mobile varchar(100),TM_Ua_Os varchar(100),TM_Ua_Platform varchar(100),TM_Ua_Agent varchar(100),TM_Browser_Language varchar(100),Original_err_code varchar(100),kind_of_sale varchar(100),sales_cycle varchar(100),ConnectorTxID1 varchar(100),ConnectorTxID2 varchar(100),LR varchar(100),NSU varchar(100),Accel_Fees varchar(100),NetPayOut_Accelerated varchar(100),Accel_PayOutDate varchar(100),Ship_to_State varchar(100),Tax_ID_Type varchar(100),Tax_ID varchar(100),Short_Id varchar(100),Bulk_Id varchar(100),Reference_Id varchar(100),Result varchar(100),Status_Code varchar(100),Reason_Code varchar(100),Return_Code varchar(100),Transaction_Id varchar(100),Transaction_Invoice_Id varchar(100),Account_Last_four_digits varchar(100),Account_holder varchar(100),Brand varchar(100),Account_Year varchar(100),Account_Month varchar(100),Bin_Number varchar(100),Registration_Id varchar(100),Account_Bank varchar(100),Issuing_Bank varchar(100),Card_Type varchar(100),Card_Level varchar(100),Channel varchar(100),Transaction_Date varchar(100),Transaction_Time varchar(100),Product_Description varchar(200),Credit varchar(100),Debit varchar(100),ValuesWOTaxes varchar(100),Fees varchar(100),NetPayOut varchar(100),PayOutWOWht varchar(100),PayOutWithWHT varchar(100),PayOutDate varchar(100),Taxes varchar(100),Merchant_unique_ID varchar(100),Payment_Method varchar(100),Payment_Type varchar(100),Currency	 varchar(100),BOLETO_Due_date	 varchar(100),Num_Installments	 varchar(100),Family_Name varchar(100));")

print "Copying Allpago Data from S3 to  \n  Allpago_raw_2"
cursor.execute("COPY allpago_raw2 FROM 's3://bibusuu/PSP_Reports/Allpago/New_Files'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' removequotes ignoreheader 1 delimiter ';' ;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

print "Copying Allpago Data from S3 to  \n  Allpago_raw_2"
cursor.execute("COPY allpago_raw2 FROM 's3://bibusuu/PSP_Reports/Allpago/Backlog'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' ACCEPTINVCHARS ignoreheader 1 csv ;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

print "Deleting old table Allpago_raw"
cursor.execute("drop table if exists Allpago_raw;")

print 'Aggregating Allpago_raw2 into Allpago_raw'
cursor.execute("create table allpago_raw as select unique_id,  email, ip,given_name, country, country_code, state, city,region_code, channel_name, reason_code_meaning, return_code_meaning, status_code_meaning,concat(concat(concat(concat(substring(transaction_Date,7,4),'-'),substring(transaction_Date,4,2)),'-'),substring(transaction_Date,0,3))::date as transaction_date, credit, debit, currency, payment_method, payment_type, fees, taxes, netpayout, bulk_id, reference_id, status_code, reason_code, return_code, transaction_id, account_last_four_digits, brand, account_year, account_month, bin_number, issuing_bank, card_type, card_level, boleto_due_date, num_installments from allpago_raw2;")

print "Deleting old table Allpago_raw2"
cursor.execute("drop table if exists Allpago_raw2;")

print "Complete\nEnjoy your Allpago Data"

conn.commit()
conn.close()
__author__ = 'brucepannaman'

import csv
import os
import configparser
from datetime import date, timedelta
from subprocess import call, check_output, CalledProcessError
import psycopg2

config = configparser.ConfigParser()
ini = config.read('conf2.ini')

AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')
ADYEN_KEY = config.get('Adyen Keys', 'pass')

merchant_accounts = ["BusuuCOM", "BusuuBRL", "BusuuRUB", "BusuuUSD", "BusuuZuora", "BusuuZuoraRecurring"]

start_date = date.today() - timedelta(days=30)
end_date = date.today()

while start_date < end_date:
    start_date2 = start_date.strftime('%Y_%m_%d')

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/PSP_Reports/Adyen/%s" % start_date2])

    if len(rs) > 1:
        print "File Exists for %s \n Moving on ;-)" % start_date2

    else:
        for merchant in merchant_accounts:

            try:
                check_output(["wget", "--http-user=report%Company.Busuu", "--http-password=%s" % ADYEN_KEY, "https://ca-live.adyen.com/reports/download/MerchantAccount/%s/received_payments_report_%s.csv" % (merchant, start_date2)])
            except CalledProcessError as e:
                continue

            with open('received_payments_report_%s.csv' % start_date2, 'rb') as csvfile:
                reader = csv.reader(csvfile, delimiter=',', quotechar='|')

                iterator = 0
                with open('%s_received_payments_report_%s.csv' % (merchant, start_date2), 'w+b') as outfile:
                    writer = csv.writer(outfile, delimiter=",")

                    print "Fixing Adyen records for %s on %s" % (merchant, start_date)

                    for line in reader:

                        row = []
                        if iterator > 0:
                            merchant2 = line[1]
                            row.append(merchant2)

                            psp_ref = line[2]
                            row.append(psp_ref)

                            merchant_ref = line[3]
                            row.append(merchant_ref)

                            payment_method = line[4]
                            row.append(payment_method)

                            creation_timestamp = line[5]
                            row.append(creation_timestamp)

                            timezone = line[6]
                            row.append(timezone)

                            currency = line[7]
                            row.append(currency)

                            amount = line[8]
                            row.append(amount)

                            risk_score = line[10]
                            row.append(risk_score)

                            shopper_interaction = line[11]
                            row.append(shopper_interaction)

                            name = line[12]
                            row.append(name)

                            shopper_pan = line[13]
                            row.append(shopper_pan)

                            ip = line[14]
                            row.append(ip)

                            country = line[15]
                            row.append(country)

                            issuer_name = line[16]
                            row.append(issuer_name)

                            issuer_id = line[17]
                            row.append(issuer_id)

                            issuer_city = line[18]
                            row.append(issuer_city)

                            issuer_country = line[19]
                            row.append(issuer_country)

                            acquirer_response = line[20]
                            row.append(acquirer_response)

                            shopper_email = line[22]
                            row.append(shopper_email)

                            shopper_reference = line[23]
                            row.append(shopper_reference)

                            cvc2_response = line[26]
                            row.append(cvc2_response)

                            AVS_response = line[27]
                            row.append(AVS_response)

                            billing_street = line[28]
                            row.append(billing_street)

                            acquirer_reference = line[40]
                            row.append(acquirer_response)

                            payment_method = line[41]
                            row.append(payment_method)

                            raw_acquirer_response = line[42]
                            row.append(raw_acquirer_response)
                            # print row

                            writer.writerow(row)

                        iterator = iterator + 1
                    os.remove('received_payments_report_%s.csv' % start_date2)

            print "Uploading payment for %s on %s to s3" % (merchant, start_date2)
            call(["s3cmd", "put", '%s_received_payments_report_%s.csv' % (merchant, start_date2),
                  "s3://bibusuu/PSP_Reports/Adyen/%s/%s_received_payments_report_%s.csv" % (
                  start_date2, merchant, start_date2)])

            print "removing old files"

            os.remove('%s_received_payments_report_%s.csv' % (merchant, start_date2))
    start_date = start_date + timedelta(days=1)

conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Adyen_raw_2"
cursor.execute("drop table if exists adyen_raw_2;")

print "Creating new table \n Adyen_raw_2"
cursor.execute(
    "create table adyen_raw_2 (merchant varchar(25),psp_ref varchar(100),merchant_ref varchar(100),payment_method varchar(250),creation_timestamp timestamp,timezone varchar(5),currency varchar(15),amount float,risk_score int,shopper_interaction varchar(100),name varchar(2000),shopper_pan varchar(50),ip varchar(50),country varchar(50),issuer_name varchar(250),issuer_id varchar(250),issuer_city varchar(50),issuer_country varchar(40),acquirer_response varchar(500),shopper_email varchar(250),shopper_reference varchar(250),cvc2_response varchar(20),AVS_response int,billing_street varchar(50),acquirer_reference varchar(100),payment_card varchar(50),raw_acquirer_response varchar(250));")

print "Copying Adyen Data from S3 to  \n  Adyen_raw_2"
cursor.execute(
    "COPY adyen_raw_2 FROM 's3://bibusuu/PSP_Reports/Adyen/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' csv;" % (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

print "Deleting old table Adyen_raw"
cursor.execute("drop table if exists adyen_raw;")

print 'Renaming Adyen_raw_2 to Adyen_raw'
cursor.execute("alter table adyen_raw_2 rename to adyen_raw;")

print "Complete\nEnjoy your Adyen Data"

conn.commit()
conn.close()
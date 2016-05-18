__author__ = 'brucepannaman'

import requests
import configparser
import psycopg2
import xml.etree.ElementTree as ET

config = configparser.ConfigParser()
ini = config.read('conf2.ini')

RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')


conn_string = "dbname=%s port=%s user=%s password=%s host=%s" % (RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

print "Finding bins that have not been looked up yet"
cursor.execute("select distinct adyen.issuer_id from adyen_raw adyen left join bank_bin_code_list list on list.bin = adyen.issuer_id where left(adyen.issuer_id,4) between 0 and 9999 and len(adyen.issuer_id) = 6 and list.bin is null limit 500;")

search_bins = cursor.fetchall()

iterator = 0

for bins in search_bins:
    bin_to_search = int(str(bins[0]).replace('(', '').replace(',)', ''))

    print "%s - %sth bin of the day being searched" % (bin_to_search, iterator)

    # response = requests.get('https://api.bincodes.com/bin-checker.php?api_key=b3783b61d75789cf743d3acbcf1ae5f2&bin=%s' % bin_to_search)
    try:
        r = requests.get('http://api.bincodes.com/bin-checker.php?api_key=b3783b61d75789cf743d3acbcf1ae5f2&bin=%s' % bin_to_search)
        tree = ET.fromstring(r.text)

        bin = tree.find("bin").text
        bank = tree.find("bank").text
        bank = str(bank).replace("'", "")
        card = tree.find("card").text
        level = tree.find("level").text
        type = tree.find("type").text
        country = tree.find("country").text
        countrycode = tree.find("countrycode").text

        cursor.execute("insert into bank_bin_code_list values ('%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (bin, bank, card, level, type, country, countrycode))
        iterator = iterator + 1

    except:
        print "Found problem with %sth bin of the day" % iterator
        iterator = iterator + 1
        continue


print 'Complete'
conn.commit()
conn.close()





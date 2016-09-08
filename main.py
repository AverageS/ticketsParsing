#!/usr/bin/python
# -*- coding: utf8 -*-
#96 lines of code
import re
import MySQLdb
import os
import fnmatch
import itertools
import time
from docx import Document
import sys
import logging

db = MySQLdb.connect("0.0.0.0", "root", "passw0rd", "netmap", charset="utf8", use_unicode=True, port=3306)
cursor = db.cursor()

class FirstParseErrorException(Exception):
    pass

class SecondParseErrorException(Exception):
    pass

def scan_and_send_new_tickets(time_float, file_path):
    doc_creation_date = os.path.getmtime(file_path)
    if doc_creation_date < time_float:
        return None
    #parse_docx(file_path)

def iterator(path):
    for dirpath, dirnames, files in os.walk(path):
        for f in fnmatch.filter(files, '*.docx'):
            yield '/'.join([dirpath,f])
#format = ['ip', 'string', 'ip', 'string']
FORMAT_TO_REG_MATCH = {
    'ip': r'\d+\.\d+\.\d+\.\d+',
    'string': r'.*(\n)|($)'
}

FORMAT_TO_DATABASE = {
    'ip': "inet_aton('%s')",
    'string': "'%s'",
}

def send_to_database(data_row, format, table_data_list):
    send_str = 'INSERT INTO %s ( ' % table_data_list[-1]
    # list comprehension ???
    for i, el in enumerate(data_row):
        send_str += table_data_list[i] + ', '
    send_str = send_str[:-2] +  " ) VALUES ( "
    for i, el in enumerate(data_row):
        send_str += FORMAT_TO_DATABASE[format[i]] + ","
    send_str = (send_str[:-1] + ')') % tuple(data_row)
    cursor.execute(send_str)
    db.commit()

send_to_database([u'192.168.18.13', u'yoba2', u'some_yoba'], [u'ip', u'string', u'string'], [u'src_ip', u'str1',u'str2', u'netmap2'])

def parse_table(table, format, uniq_columns):
    data_rows = []
    for i, row in enumerate(table.rows[2:]):
        if len(row.cells) < len(format):
            return
        data_row = []
        data = [0] * len(format)
        #Нихуя не работает, блджад.
        for index, el in enumerate(row.cells):
            if index >= len(format):
                break
            if index in uniq_columns:
                data[index] = re.findall(FORMAT_TO_REG_MATCH[format[index]],el.text)
            else:
                data[index] = [el.text]
        t = itertools.permutations(data)
        for l in t:
            print(l)



def parse_docx(f):
    doc = Document(f)
    parse_table(doc.tables[1], ['ip', 'ip', 'string', 'string'], (0,1))

parse_docx('/home/mikhail/Desktop/tickets/Заявка+на+предоставление+сетевой+связности %281%29_353.docx')

'''
def parse_table(table, format, uniq_tables):

    try:
        for i, row in enumerate(table.rows[2:]):
            if len(row.cells) < 3:
                continue
            for srcip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[0].text):
                for dstip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[1].text):
                    for dstport in re.findall(r'\d+', row.cells[2].text):
                        cursor.execute("INSERT INTO netmap (src_ip,dst_ip,dst_port,comment) VALUES (inet_aton(%s),inet_aton(%s),%s,%s)", (srcip,dstip,dstport,row.cells[3].text))
                        db.commit()
    except Exception as e:
        db.rollback()
        raise FirstParseErrorException


def parse_docx(f):
    doc = Document(f)
    try:
        parse_first_table(doc.tables[1])
        parse_second_table(doc.tables[2])
    except SecondParseErrorException:
        logging.debug('somewhat fail with second table in %s, trying 2nd variant' % f)
        try:
            parse_second_table_with_host_string(doc.tables[2])
            logging.info('success %s ' % f)
        except:
            logging.error('epic fail with \t %s ' % f)
    except FirstParseErrorException:
        logging.error('Somewhat failed with first table in %s' % f)
    except:
        logging.error('Somewhat really failed in %s probably there are no tables there' % f)
        print(f)

def main():
    path = sys.argv[1]
    for file_path in iterator(path):
        print(file_path)
        #parse_docx(file_path)
    start_time = time.time()
    while True:
        for file_path in iterator(path):
            scan_and_send_new_tickets(start_time, file_path)
        time.sleep(600)

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR,format='%(asctime)s - %(levelname)s - %(message)s')
    main()

db.close()
'''
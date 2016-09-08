#!/usr/bin/python
# -*- coding: utf8 -*-
import re
import MySQLdb
import os
import fnmatch
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
'''
 create table netmap(
    src_ip BIGINT,
    dst_ip BIGINT,
    dst_port BIGINT,
    comment VARCHAR(1024) CHARACTER SET utf8 COLLATE utf8_unicode_ci);
 create table second_table(
    ip BIGINT,
    host_ip BIGINT,
    system_name VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
    comment VARCHAR(1024) CHARACTER SET utf8 COLLATE utf8_unicode_ci);
 create table third_table(
    ip BIGINT,
    host_name VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
    system_name VARCHAR(256) CHARACTER SET utf8 COLLATE utf8_unicode_ci,
    comment VARCHAR(1024) CHARACTER SET utf8 COLLATE utf8_unicode_ci);
'''
'''
t = time.time()
#t = os.path.getmtime('/'.join([dirpath,f]))
tt = time.gmtime(t)
'''
def scan_and_send_new_tickets(time_float, file_path):
    doc_creation_date = os.path.getmtime(file_path)
    if doc_creation_date < time_float:
        return None
    parse_docx(file_path)

def iterator(path):
    for dirpath, dirnames, files in os.walk(path):
        for f in fnmatch.filter(files, '*.docx'):
            yield '/'.join([dirpath,f])

def parse_first_table(table):
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

def parse_second_table(table):
    try:
        for i, row in enumerate(table.rows[2:]):
            if len(row.cells) < 4:
                continue
            if len( re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[2].text) ) < 1:
                raise SecondParseErrorException
            for srcip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[1].text):
                for host in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[2].text):
                    system = row.cells[0].text
                    comment = row.cells[3].text
                    cursor.execute("INSERT INTO second_table (ip, host_ip, system_name, comment) VALUES (inet_aton(%s), inet_aton(%s),%s,%s)", (srcip,host,system,comment))
                    db.commit()
    except Exception as e:
        db.rollback()
        raise SecondParseErrorException

def parse_second_table_with_host_string(table):
    try:
        for i, row in enumerate(table.rows[2:]):
            if len(row.cells) < 4:
                continue
            for srcip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[1].text):
                host = row.cells[2].text
                system = row.cells[0].text
                comment = row.cells[3].text
                cursor.execute("INSERT INTO third_table (ip, host_name, system_name, comment) VALUES (inet_aton(%s), %s,%s,%s)", (srcip,host,system,comment))
                db.commit()
    except Exception as e:
        db.rollback()
        raise Exception

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
        #print(f)




def main():
    path = sys.argv[1]
    for file_path in iterator(path):
        parse_docx(file_path)
    start_time = time.time()
    while True:
        for file_path in iterator('/home'):
            scan_and_send_new_tickets(start_time, file_path)
        time.sleep(600)

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR,format='%(asctime)s - %(levelname)s - %(message)s')
    main()


db.close()
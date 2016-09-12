#!/usr/bin/python
# -*- coding: utf8 -*-
#96 lines of code
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

def my_insert(arr, index, el):
    ans = list(arr)
    ans.insert(index, el)
    return ans

def scan_and_send_new_tickets(time_float, file_path):
    doc_creation_date = os.path.getmtime(file_path)
    if doc_creation_date < time_float:
        return None
    parse_docx_and_send_to_db(file_path)

def iterator(path):
    for dirpath, dirnames, files in os.walk(path):
        for f in fnmatch.filter(files, '*.docx'):
            yield '/'.join([dirpath,f])


FORMAT_TO_REG_MATCH = {
    'ip': r'\d+\.\d+\.\d+\.\d+',
    'string': r'.*(\n)|($)',
    'port': r'\d+',
}

FORMAT_TO_DATABASE = {
    'ip': "inet_aton('%s')",
    'port': '%s',
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
    try:
        cursor.execute(send_str)
        db.commit()
    except:
        db.rollback()
        logging.error(send_str)
        raise FirstParseErrorException

#send_to_database([u'192.168.18.13', u'yoba2', u'some_yoba'], [u'ip', u'string', u'string'], [u'src_ip', u'str1',u'str2', u'netmap2'])

def parse_table(table, format, uniq_columns):
    answer = []
    for i, row in enumerate(table.rows[1:]):
        data = [[] for x in range(len(format))]
        for index, el in enumerate(row.cells):
            if index >= len(format) or el.text in ['',u'']:
                break
            #TODO Remove uniq_columns, add uniq column check
            if index in uniq_columns:
                found_arr = re.findall(FORMAT_TO_REG_MATCH[format[index]],el.text)
                if found_arr == []:
                    break
                data[index].extend(found_arr)
            else:
                data[index].extend([el.text])
        data_rows = [[]]
        for index, cell_data in enumerate(data):
            if len(cell_data) == 1:
                map(lambda x: x.insert(index, cell_data[0]), data_rows)
            elif len(cell_data) > 1:
                old_data_rows = data_rows[-1]
                data_rows = []
                data_rows.extend(map(lambda x: my_insert(old_data_rows, index, x), cell_data))
        answer.extend(data_rows)
    return [x for x in answer if x != []]


def parse_docx_and_send_to_db(f):
    try:
        doc = Document(f)
        format = ['ip', 'ip', 'port', 'string']
        data_rows = parse_table(doc.tables[1], format, (0,1,2))
        logging.debug('parsed succesfully %s' % f)
    except Exception as e:
        logging.error(str(e) + '\n %s ' % f)
        return
    for row in data_rows:
        if len(row) < len(format):
            break
        try:
            send_to_database(row, format,['src_ip', 'dst_ip', 'dst_port', 'comment', 'netmap'])
        except:
            pass


parse_docx_and_send_to_db('/home/mikhail/Desktop/tickets/Заявка+на+предоставление+сетевой+связности Patrol_415.docx')


def main():
    path = sys.argv[1]
    for file_path in iterator(path):
        parse_docx_and_send_to_db(file_path)
    start_time = time.time()
    while True:
        for file_path in iterator(path):
            pass
            ### maybe  just ignore old folders?
            scan_and_send_new_tickets(start_time, file_path)
        time.sleep(600)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    main()

db.close()

#!/usr/bin/python
# -*- coding: utf8 -*-
#96 lines of code
import re
import MySQLdb
import os
import fnmatch
import time
import itertools
from docx import Document
import sys
import logging

db = MySQLdb.connect("0.0.0.0", "root", "passw0rd", "netmap", charset="utf8", use_unicode=True, port=3306)
cursor = db.cursor()

class ParseErrorException(Exception):
    pass


def my_insert(arr, index, el):
    ans = list(arr)
    ans.insert(index, el)
    return ans


def iterator(path):
    for dirpath, dirnames, files in os.walk(path):
        for f in fnmatch.filter(files, '*.docx'):
            yield '/'.join([dirpath,f])


FORMAT_TO_REG_MATCH = {
    'ip': r'\d+\.\d+\.\d+\.\d+',
    'string': r'.*',
    'port': r'\d+',
}

FORMAT_TO_DATABASE = {
    'ip': "inet_aton('%s')",
    'port': '%s',
    'string': "'%s'",
}

def send_to_database(data_row, format, table_data_list):
    send_str = 'INSERT INTO %s ( ' % table_data_list[-1]
    for i, el in enumerate(data_row):
        send_str += table_data_list[i] + ', '
    send_str = send_str[:-2] +  " ) VALUES ( "
    for i, el in enumerate(data_row):
        send_str += FORMAT_TO_DATABASE[format[i]] + ","
    send_str = (send_str[:-1] + ')') % tuple(data_row)
    try:
        cursor.execute(send_str)
        db.commit()
    except Exception as e:
        db.rollback()
        logging.error(send_str)
        raise e


def parse_table(table, format):
    answer = []
    for i, row in enumerate(table.rows[1:]):
        data = [[] for x in range(len(format))]
        for index, el in enumerate(row.cells):
            if index >= len(format) or el.text in ['',u'']:
                break
            found_arr = re.findall(FORMAT_TO_REG_MATCH[format[index]],el.text)
            if found_arr == []:
                break
            data[index].extend(found_arr)
        data_rows = [[]]
        start_time = time.time()
        prod = [x in itertools.product(data)]
        iter_time = time.time() - start_time
        start_time = time.time()
        for index, cell_data in enumerate(data):
            if len(cell_data) == 1:
                map(lambda x: x.insert(index, cell_data[0]), data_rows)
            elif len(cell_data) > 1:
                old_data_rows = data_rows[-1]
                data_rows = []
                data_rows.extend(map(lambda x: my_insert(old_data_rows, index, x), cell_data))
        lam_time = time.time() - start_time
        answer.extend(data_rows)
    return [x for x in answer if x != []]

def parse_docx_and_send_to_db(f, format, table_number):
    try:
        doc = Document(f)
        data_rows = parse_table(doc.tables[table_number], format)
        logging.debug('parsed succesfully %s' % f)
    except Exception as e:
        logging.error(str(e) + '\n %s ' % f)
        raise ParseErrorException
    for row in data_rows:
        if len(row) < len(format):
            continue
        try:
            ticket_name = f.split('/')[-2]
            row.append(ticket_name)
            send_to_database(row, format,['src_ip', 'dst_ip', 'dst_port', 'comment', 'ticket_name' 'netmap'])
        except:
            raise ParseErrorException

#TODO переписать через декораторы
def scan_and_send_new_tickets(time_float, path, format, table_number):
    success_count = 0
    for index, file_path in enumerate(iterator(path)):
        doc_creation_date = os.path.getmtime(file_path)
        if doc_creation_date < time_float:
            continue
        try:
            parse_docx_and_send_to_db(file_path, format, table_number)
            success_count += 1
        except ParseErrorException:
            pass
    logging.info('ALL %d \nSUCCESSFULL %d', index, success_count)


def main():
    path = sys.argv[1]
    format = ['ip', 'ip', 'port', 'string', 'string'] # !!! last string for ticket_name
    scan_and_send_new_tickets(0,  path, format, table_number=1)
    while True:
        start_time = time.time()
        scan_and_send_new_tickets(start_time, path, format, table_number=1)
        time.sleep(600)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    main()

db.close()

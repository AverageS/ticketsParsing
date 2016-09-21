# -*- coding: utf8 -*-
import re
import os
import fnmatch
from elasticsearch import Elasticsearch
import json
import time
from docx import Document
from mapping_creation import create_mapping
import sys
import logging

#TODO Доделать маппинг к ES, сейчас отправляются голые массивы [ip, ip, string]
#TODO нужно отправлять словари {ip: ip, ip: ip, string: str}

def my_insert(arr, index, el):
    ans = list(arr)
    ans.insert(index, el)
    return ans

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def iterator(path):
    for dirpath, dirnames, files in os.walk(path):
        for f in fnmatch.filter(files, '*.docx'):
            yield '/'.join([dirpath,f])

FORMAT = ['ip', 'ip', 'port', 'string']
FORMAT_NAMES = ['ip_host', 'ip_dest', 'port_dest', 'declaration']

FORMAT_TO_REG_MATCH = {
    'ip': r'\d+\.\d+\.\d+\.\d+',
    'string': r'.*',
    'port': r'\d+',
}


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
        for index, cell_data in enumerate(data):
            if len(cell_data) == 1:
                map(lambda x: x.insert(index, cell_data[0]), data_rows)
            elif len(cell_data) > 1:
                old_data_rows = data_rows[-1]
                data_rows = []
                data_rows.extend(map(lambda x: my_insert(old_data_rows, index, x), cell_data))
        answer.extend(data_rows)
    return [x for x in answer if len(x) == len(format)]


def scan_and_send_new_tickets(time_float, path):
    success_count = 0
    for index, file_path in enumerate(iterator(path)):
        doc_creation_date = os.path.getmtime(file_path)
        if doc_creation_date < time_float:
            continue
        try:
            doc = Document(file_path)
            ticket_name = file_path.split('/')[-2]
            data = parse_table(doc.tables[1], FORMAT) #TODO мб убрать номер таблицы в аргументы?
            if len(data) == 0:
                continue
            data.append(ticket_name)
            success_count += 1
            time_to_add =  int(round(doc_creation_date * 1000))
            my_dicts = []
            [my_dicts.append(dict([tpl for tpl in zip(FORMAT_NAMES, x)])) for x in data]
            [el.update([('dateAdded', time_to_add)]) for el in my_dicts]
            b = unicode(json.dumps(my_dicts[:-1]))
            for el in my_dicts:
                es.index(index='tickets', doc_type='first_table',body=unicode(json.dumps(el)))
        except Exception as e:
            pass
    logging.info('ALL %d \nSUCCESSFULL %d', index, success_count)

def main():
    path = '/home/mikhail'
    create_mapping(es, 'tickets')
    scan_and_send_new_tickets(0,  path)
    while True:
        start_time = time.time()
        scan_and_send_new_tickets(start_time, path)
        time.sleep(600)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
    es.indices.delete(index='tickets')
    main()


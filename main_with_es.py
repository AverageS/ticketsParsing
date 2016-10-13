# -*- coding: utf8 -*-
import re
import os
import fnmatch
from elasticsearch import Elasticsearch
import json
import time
from docx import Document
from mapping_creation import create_mapping
import networksConstants
import netaddr
import sys
import logging

def my_insert(arr, index, el):
    ans = list(arr)
    ans.insert(index, el)
    return ans

networksConstants.main()
es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])

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
            if index >= len(format) or (el.text in ['', u''] and format[index] != 'string'):
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
                #TODO Тут бага
                data_rows.extend(map(lambda x: my_insert(old_data_rows, index, x), cell_data))
        answer.extend(data_rows)
    return [x for x in answer if len(x) == len(format)]


#мб вернуться к супер-решению в parse_table, но наврятли
def parse_first_table(table):
    data_rows = []
    for row in table.rows[1:]:
        for srcip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[0].text):
            for dstip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[1].text):
                for dstport in re.findall(r'\d+', row.cells[2].text):
                    data_rows.append([srcip, dstip,dstport,row.cells[3].text])
    return data_rows


def scan_and_send_new_tickets(time_float, path):
    success_count = 0
    all = 0
    for index, file_path in enumerate(iterator(path)):
        last_folder = file_path.split('/')[-2]
        ticket_type = last_folder[:3]
        ticket_number = last_folder[3:]
        if ticket_type not in ('INC', 'CRQ'):
            ticket_type = 'CRQ'
        doc_creation_date = os.path.getmtime(file_path)
        if doc_creation_date < time_float:
            continue
        all +=1
        try:
            doc = Document(file_path)
            data = parse_first_table(doc.tables[1])
            if len(data) == 0:
                continue
            success_count += 1
            time_to_add =  int(round(doc_creation_date * 1000))
            my_dicts = [(dict([tpl for tpl in zip(FORMAT_NAMES, x)])) for x in data]
            [el.update([('dateAdded', time_to_add)]) for el in my_dicts]
            ports_desc = {22:'ssh', 20:'ftp',21:'ftp',1433:'sql',3389:'RDP'}
            for el in my_dicts:
                try:
                    src_ip = netaddr.IPAddress(el['ip_host'])
                    dst_ip = netaddr.IPAddress(el['ip_dest'])
                except:
                    continue
                el['dst_network'] = u'UNKNOWN'
                el['port_desc'] = u'UNKNOWN PORT'
                el['ticket_type'] = unicode(ticket_type)
                el['ticket_number'] = index
                port = int(el['port_dest'])
                el['ip_port_triple'] = unicode(':'.join(list(map(str, [src_ip, dst_ip, port]))))
                if port in ports_desc.keys():
                    el['port_desc'] = ports_desc[port]
                #TODO переписать на сеты
                for cod_name, cod in networksConstants.MAIN_DICT.iteritems():
                    for network in cod:
                        if dst_ip in network:
                            el['dst_network'] = cod_name
                es.index(index='tickets', doc_type='first_table',body=unicode(json.dumps(el)))
        except Exception as e:
            pass
    logging.info('ALL %d \nSUCCESSFULL %d', all, success_count)

def main():
    path = '/usr/share/tickets'
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


import re
import os
import fnmatch
from elasticsearch import Elasticsearch
import time
from docx import Document
from mapping_creation import create_mapping
import networks
import netaddr
import sys
import logging


FORMAT_NAMES = ['ip_host', 'ip_dest', 'port_dest', 'declaration']

es = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
def parse_first_table(table):
    data_rows = []
    for row in table.rows[1:]:
        for srcip in re.findall(r'\d+\.\d+\.\d+\.\d+/\d{2}|\d+\.\d+\.\d+\.\d+', row.cells[0].text):
            for dstip in re.findall(r'\d+\.\d+\.\d+\.\d+/\d{2}|\d+\.\d+\.\d+\.\d+', row.cells[1].text):
                for dstport in re.findall(r'\d+', row.cells[2].text):
                    data_rows.append([srcip, dstip,dstport,row.cells[3].text])
    return data_rows

def iterator(path):
    for dirpath, dirnames, files in os.walk(path):
        for f in fnmatch.filter(files, '*.docx'):
            yield '/'.join([dirpath,f])

def get_file_info(filename):
    last_folder = filename.split('/')[-2]
    ticket_type = last_folder[:3]
    ticket_number = last_folder[3:]
    doc_creation_date = os.path.getmtime(filename)
    return [ticket_type, ticket_number, doc_creation_date]

def format_table(table, filename):
    dict_table = [dict([tpl for tpl in zip(FORMAT_NAMES, row)]) for row in table]
    ticket_type, ticket_number, doc_creation_date = get_file_info(filename)
    [el.update([('doc_creation_date', int(round(doc_creation_date*1000))),('added_time', int(round(time.time()*1000)))]) for el in dict_table]
    for el in dict_table:
        src_ip_network = netaddr.IPNetwork(el['ip_host'])
        dst_ip_network = netaddr.IPNetwork(el['ip_dest'])
        src_ip, dst_ip = src_ip_network.ip, dst_ip_network.ip
        el['ip_host'], el['ip_dest'] = str(src_ip), str(dst_ip)
        el['host_network'], el['dst_network'] = str(src_ip_network), str(dst_ip_network)
        el['dst_network_description'] = networks.check(dst_ip_network) or 'UNKNOWN'
        el['ticket_type'], el['ticket_number'] = ticket_type, ticket_number
        port = int(el['port_dest'])
        el['ip_port_triple'] = ':'.join(list(map(str, [src_ip, dst_ip, port])))
        el['filename'] = filename
    return dict_table

def error_catching(func):
    def wrapper(filename):
        try:
            func(filename)
            logging.info(filename + '\t sent')
        except Exception as e:
            logging.error(str(e))
    return wrapper


@error_catching
def scan_doc(filename):
    doc = Document(filename)
    table = parse_first_table(doc.tables[1])
    dict_to_send_to_el = format_table(table,filename)
    for el in dict_to_send_to_el:
        es.index(index='tickets', doc_type='first_table', body=el)


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    create_mapping(es, 'tickets')
    path = '/usr/share/tickets'
    names_list = set()
    if '--scan_all' in sys.argv:
        for x in iterator(path):
            scan_doc(x)
            names_list.add(x)
    while True:
        for x in iterator(path):
            if x not in names_list:
                scan_doc(x)
                names_list.add(x)
        time.sleep(600)





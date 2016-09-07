import re
import glob
import MySQLdb
import os
import fnmatch
from docx import Document
import sys

# Open database connection
db = MySQLdb.connect("0.0.0.0","root","passw0rd","netmap", charset = "utf8", use_unicode = True,port=3306)
# prepare a cursor object using cursor() method
cursor = db.cursor()






'''
 create table netmap1(
    -> src_ip INT,
    -> dst_ip INT,
    -> dst_port INT,
    -> comment CHAR(1024) CHARACTER SET utf8 COLLATE utf8_unicode_ci);
'''
def parse_docx(f):
    doc = Document(f)
    try:
        t = doc.tables[1]
    except:
        return
    for i, row in enumerate(t.rows[2:]):
        for srcip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[0].text):
            for dstip in re.findall(r'\d+\.\d+\.\d+\.\d+', row.cells[1].text):
                for dstport in re.findall(r'\d+', row.cells[2].text):
                    try:
                        cursor.execute("INSERT INTO netmap (src_ip,dst_ip,dst_port,comment) VALUES (inet_aton(%s),inet_aton(%s),%s,%s)", (srcip,dstip,dstport,row.cells[3].text))
                        db.commit()
                    except Exception as e:
                        db.rollback()
                        pass


for dirpath, dirnames, files in os.walk('/home'):
    for f in fnmatch.filter(files, '*.docx'):
        print('/'.join([dirpath,f]))
        parse_docx('/'.join([dirpath,f]))

db.close()
import sqlite3
from datetime import date

con = sqlite3.connect('work.sqlite')
cur = con.cursor()

permtransf = cur.execute('select * from aih order by AIHREF,DT_INTER;')

dtint = open('datedelta.csv', 'w')
dtint.writelines('AIHREF,NRECS,DT_INI,DT_FIM,DIAS,COBRANCA\n')

def list2str(list):
    txt = ''
    for i in range(len(list)):
      txt += list[i] + ','
    txt = txt[:-1] + '\n'
    return txt

def todate(datestr):
    thedate = date(int(datestr[:4]),int(datestr[4:6]),int(datestr[6:]))
    return thedate

def datediff(mindate, maxdate):
    delta = todate(maxdate) - todate(mindate)
    diffstr = str(delta).split(' ')[0]
    if diffstr == '0:00:00':
    	diffstr = '0'
    return str(int(diffstr)+1)

aihref = ''
nrecs = 0
for row in permtransf:
    nrecs += 1
    print(nrecs, end = '\r')
    if aihref == '': # first
        aihref = row[8]
        mindate = row[4]
        maxdate = row[5]
        cobranca = row[6]
        nordem = 1
    else:
        if aihref != row[8]:
            dtint.writelines(aihref+','+str(nordem)+
                ','+mindate+','+maxdate+
                ','+datediff(mindate,maxdate)+
                ','+cobranca+'\n')
            mindate = row[4]
            maxdate = row[5]
            cobranca = row[6]
            nordem = 1
        if row[4] < mindate:
            mindate = row[4]
        if row[5] > maxdate:
            maxdate = row[5]
        nordem += 1
        aihref = row[8]
        cobranca = row[6]
else: # for the last sequence
        dtint.writelines(aihref+','+str(nordem)+
            ','+mindate+','+maxdate+
            ','+datediff(mindate,maxdate)+
            ','+cobranca+'\n')

dtint.close()
con.close()

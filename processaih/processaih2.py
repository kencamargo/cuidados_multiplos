import sqlite3
from datetime import date

con = sqlite3.connect('work.sqlite')
cur = con.cursor()

#print('Criando indice, aguarde.')
#cur.execute('create index if not exists refaih on aihpermtransf(AIHREF);')
#cur.execute('create index if not exists refaih on aihperm(AIHREF);')
#permtransf = cur.execute('select * from aihpermtransf order by AIHREF;')
#permtransf = cur.execute('select * from aihperm order by AIHREF;')

permtransf = cur.execute('select * from aih order by AIHREF,DT_INTER;')

cep = open('cep.csv', 'w')
dtint = open('datedelta.csv', 'w')
cep.writelines('CNES,CEP,MUNIC_RES,NASC,DT_INTER,DT_SAIDA,COBRANCA,N_AIH,AIHREF,FLAG,NORDEM\n')
dtint.writelines('AIHREF,DIAS,COBRANCA\n')

def list2str(list):
    txt = ''
    for i in range(len(list)):
      txt += list[i].replace('"','') + ','
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
    return diffstr

aihref = ''
nrecs = 0
for row in permtransf:
    nrecs += 1
    print(nrecs, end = '\r')
    if aihref == '': # first
        aihref = row[8]
        rcep = row[1]
        cobranca = row[6]
        cflag = False
        recs = []
        mindate = row[4].replace('"','')
        maxdate = row[5].replace('"','')
        nordem = 1
    else:
        if aihref != row[8]:
            if cflag:
                for line in recs:
                    cep.writelines(list2str(line))
            dtint.writelines(aihref+','+datediff(mindate,maxdate)+','+cobranca+'\n')
            del recs
            recs = []
            cflag = False
            mindate = row[4].replace('"','')
            maxdate = row[5].replace('"','')
            cobranca = row[6]
            nordem = 1
            rcep = row[1]
        recs.append(row)
        if not cflag and row[1] != rcep:
            cflag = True
        if row[4].replace('"','') < mindate:
            mindate = row[4].replace('"','')
        if row[5].replace('"','') > maxdate:
            maxdate = row[5].replace('"','')
        cobranca = row[6]
        nordem += 1
        aihref = row[8]
else: # for the last sequence
    if cflag:
        for line in recs:
            cep.writelines(list2str(line))
    dtint.writelines(aihref+','+datediff(mindate,maxdate)+','+cobranca+'\n')

cep.close()
dtint.close()
con.close()

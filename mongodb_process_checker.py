#!/usr/bin/python
# Version - 0.2
# Purpose - MongoDB Process Checker
# Author  - Vinoth Kanna RS / Mydbops IT Solutions
# Website - www.mydbops.com   Email - mysqlsupport@mydbops.com

import pymongo, argparse, getpass, urllib, commands, sys, time, re
from prettytable import PrettyTable
from bson.json_util import dumps

parser = argparse.ArgumentParser(description='Mongo Process Checker', formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('-H', '--host', action='store', type=str, dest='mongo_host', default='localhost', help='Input Hostname, Default: localhost')
parser.add_argument('-P', '--port', action='store', type=int, dest='mongo_port', default='27017', help='Input Port, Default: 27017')
parser.add_argument('-u', '--user', action='store', type=str, dest='mongo_user', default='NoAuth', help='Input Username, Default: NoAuth')
parser.add_argument('-p', '--password', action='store', type=str, dest='mongo_password', default='NoAuth', help='Input Password, Default: NoAuth')
parser.add_argument('-a', '--authDB', action='store', type=str, dest='authdb', default='admin', help='Input Auth DB, Default: admin')
parser.add_argument('-i', '--interval', action='store', type=int, dest='refresh_rate', default='4', help='Input Refresh Interval (Sec), Default: 4 Sec')
parser.add_argument('-k', '--kill', action='store', type=float, dest='kill', default='-1', help='Input Value To Kill Queries Exceeding X Sec')
parser.add_argument('--dry-run', action='store_true', dest='mock', help='Just Print Queries Exceeding X Sec')
parser.add_argument('-v', '--verbose', action='store_true', dest='verbose', help='Print OpCounter, Document Stats')
parser.add_argument('-r', '--repl', action='store_true', dest='repl', help='Print Replication Info')

args = parser.parse_args()

w1 = 35
w2 = 27
w3 = 57

if args.mock is True:
    if args.kill <= 0:
        print ('\nInvalid / Missing --Kill Value\n')
        sys.exit(1)

if args.mongo_user != 'NoAuth':
    if args.mongo_password == 'NoAuth':
        args.mongo_password=getpass.getpass('\nEnter the password : ')

os_mongodb_password = args.mongo_password
args.mongo_password = urllib.quote_plus(args.mongo_password)


def read_time(ms):

    ms = int(ms)
    rt = str(ms) + ' ms'
    if ms >= 1000:
        sec = (ms / 1000)
        rt = str(sec) + ' s'
        if sec >= 60:
            m, sec = divmod(sec, 60)
            if sec > 0:
                sec = str(sec) + ' s'
            else:
                sec = ''
            rt = str(m) + ' min ' + sec
            if m >= 60:
                hr, m = divmod(m, 60)
                if m > 0:
                    m = str(m) + ' min '
                else:
                    m = ''
                rt = str(hr) + ' hr ' + m + sec
    return rt


def op_stat():

    global po_i, po_u, po_d, po_q, po_c, po_g

    opc = sstat.get('opcounters', '')
    co_i = opc.get('insert', '')
    co_u = opc.get('update', '')
    co_d = opc.get('delete', '')
    co_q = opc.get('query', '')
    co_c = opc.get('command', '')
    co_g = opc.get('getmore', '')

    if i != 0:
        ins = co_i - po_i
        upd = co_u - po_u
        dtd = co_d - po_d
        qry = co_q - po_q
        com = co_c - po_c
        gem = co_g - po_g
        print ('Op-Counter : Ins: %d, Upd: %d, Del: %d, Qry: %d, Com: %d, GetM: %d\n' % (ins, upd, dtd, qry, com, gem))

    po_i = co_i
    po_u = co_u
    po_d = co_d
    po_q = co_q
    po_c = co_c
    po_g = co_g


def doc_stat():

    global pd_i, pd_u, pd_d, pd_r

    opc = sstat.get('metrics', '').get('document', '')
    cd_i = opc.get('inserted', '')
    cd_u = opc.get('updated', '')
    cd_d = opc.get('deleted', '')
    cd_r = opc.get('returned', '')

    if i != 0:
        ins = cd_i - pd_i
        upd = cd_u - pd_u
        dtd = cd_d - pd_d
        ret = cd_r - pd_r
        print ('Doc-Stats  : Ins: %d, Upd: %d, Del: %d, Ret: %d' % (ins, upd, dtd, ret))

    pd_i = cd_i
    pd_u = cd_u
    pd_d = cd_d
    pd_r = cd_r


def rs_stat():

    global hd

    stat = rstat.get('ismaster', '')
    rs_name = rstat.get('setName', '')
    if rs_name != '':
        if stat is True:
            status = 'Primary'
        else:
            status = 'Secondary'
        hd = '[' + rs_name + ' / ' + status + ']'
    else:
        if stat is True:
            status = 'StandAlone'
        else:
            status = 'Invalid Server'
        hd = '[' + status + ']'


def conn_stat():

    global pnw_i, pnw_o

    nw = sstat.get('network', '')
    conn = sstat.get('connections', '')
    used = conn.get('current', '')
    free = conn.get('available', '')
    cstat = '[Connections - InUse: ' + str(used) + ' / Free: ' + str(free) + ']'
    cnw_i = nw.get('bytesIn', '')
    cnw_o = nw.get('bytesOut', '')
    if i != 0:
        nw_i = (cnw_i - pnw_i) / 1024.00 / 1024.00
        nw_o = (cnw_o - pnw_o) / 1024.00 / 1024.00
        print ('[Net - In: ' + '{:.2f}'.format(nw_i) + ' MB / Out: ' + '{:.2f}'.format(nw_o) + ' MB]' + cstat.rjust(w3))
    pnw_i = cnw_i
    pnw_o = cnw_o


def repl_stat():

    r_stat = get_out('m', 'db.printReplicationInfo()')
    r_stat = "echo '" + r_stat + "' | grep 'log size\|log length' | cut -d ':' -f2 | sed 's/[()]//g' "
    r_stat = r_stat + "| awk '{ if (NF==1) {printf \"[OpLog - Size: \"$1} else {print \", Duration: \"$2\"]\"} }'"
    r_stat = get_out('l', r_stat)
    s_stat = get_out('m', 'db.printSlaveReplicationInfo()')
    hosts = '['
    for sep in s_stat.splitlines():
        if sep.find('source:') != -1:
            hosts = hosts + '(Host: ' + sep.split()[1]
        if sep.find('behind') != -1:
            hosts = hosts + ', Lag: ' + sep.split()[0] + ' s)  '
    hosts = hosts.strip() + ']'
    print (r_stat + ''.rjust(7) + hosts + '\n')


def get_proc():

    out = PrettyTable(['Op-ID', 'Host', 'Collection', 'Op', 'Duration', 'Query'])
    track = 1
    out.align = 'l'

    for proc in fetch.get('inprog', ''):

        opid = proc.get('opid', '')
        host = proc.get('client', '').split(':')
        ns = proc.get('ns', '')
        op = proc.get('op', '')
        ts = proc.get('microsecs_running', 0) / (1000 * 1.00)
        kt = ts / 1000
        pt = read_time(ts)
        qry = proc.get('command', '')
        qry = dumps(qry)
        msg = proc.get('msg', '')
        wfl = proc.get('waitingForLock', '')

        if msg != '':
            qry = qry + ' / ' + msg

        if wfl is True:
            qry = '[LOCKED] ' + str(qry)

        if ns != 'local.oplog.rs' and ns != 'local.$cmd' and qry.find('currentOp') == -1 and host[0] != '':
            track += 1
            out.add_row([opid, host[0], ns, op, pt, str(qry)])

            wq_list = time.strftime('%Y-%m-%d %H:%M:%S') + ' : ' + str(opid) + ', ' + host[0] + ', ' + ns + ', ' + op + ', ' + str(pt) + ', ' + str(qry) + '\n'
            with open(wq_fname, 'a') as wq:
                wq.write(wq_list)

            if args.kill > 0:
                if kt > args.kill:
                    with open(kq_fname, 'a') as kq:
                        kq.write(wq_list)
                    if args.mock is False:
                        admin.command('killOp', op=opid)

    print ('\033c')
    print ('Timestamp : ' + time.strftime('%Y-%m-%d %H:%M:%S') + '[Refresh Int : '.rjust(w1) + str(args.refresh_rate) + ' Sec]' + inf + '\n')
    print ('Connected To : ' + re.sub(args.mongo_password, r'****', uri) + hd.rjust(w2) + '\n')

    if args.repl is True:
        repl_stat()

    if args.verbose is True:
        doc_stat()
        op_stat()
        conn_stat()

    if track > 1:
        print (out.get_string(sortby='Op-ID'))
    else:
        print ('\n[No Queries To Display]\n')

    time.sleep(args.refresh_rate)


def get_out(flag, cmd):
    if flag == 'm':
        out = commands.getoutput("%s --eval '%s' --quiet" % (os_uri, cmd))
        return out
    if flag == 'l':
        out = commands.getoutput(cmd)
        return out


if args.mongo_user == 'NoAuth':
    uri = args.mongo_host + ':' + str(args.mongo_port) + '/' + args.authdb
    os_uri = 'mongo admin --host ' + args.mongo_host + ':' + str(args.mongo_port)
else:
    uri = args.mongo_user + ':' + args.mongo_password + '@' + args.mongo_host + ':' + str(args.mongo_port) + '/' + args.authdb
    os_uri = 'mongo admin --host ' + args.mongo_host + ':' + str(args.mongo_port) + ' -u ' + args.mongo_user + ' -p ' + os_mongodb_password


dbc = pymongo.MongoClient('mongodb://%s' % uri)
admin = dbc.admin


if args.kill > 0:
    inf = '[Kill Beyond'
    if args.mock is True:
        inf = inf + ' [D]'
    inf = inf.rjust(w2) + ' : ' + str(int(args.kill)) + ' Sec]'
else:
    inf = ''


fn = ''
for tf in args.mongo_host.split('.')[-2:]:
    if fn == '':
        fn = tf
    else:
        fn = fn + '-' + tf

kq_fname = fn + '_killed' + time.strftime('_%d-%b_%H-%M.txt')

wq_fname = fn + '_queries' + time.strftime('_%d-%b_%H-%M.txt')

header = 'Timestamp           : OpID      Host       NS          Op       Duration     Query\n'

with open(wq_fname, 'w+') as wq:
    wq.write(header)

if args.kill > 0:
    with open(kq_fname, 'w+') as kq:
        kq.write(header)


rstat = admin.command('isMaster')

for i in xrange(sys.maxint):

    try:

        fetch = admin.command('currentOp')
        sstat = admin.command('serverStatus')
        rs_stat()
        get_proc()

    except pymongo.errors.PyMongoError, e:
        print ('\nConnecting To : ' + uri + '\n')
        print (time.strftime('%Y-%m-%d %H:%M:%S') + ' : MongoDB Exception - %s\n' % e)
        sys.exit(1)

    except KeyboardInterrupt:
        print ('\n\nExit\n')
        sys.exit(1)

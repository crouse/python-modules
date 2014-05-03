import MySQLdb as mdb
import string
import json

host='127.0.0.1'
user='root'
password='123456'
database='test'

class SimDb(object):
    def __init__(self, host, user, passwd, db, charset="utf8"):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset=charset
        self.conn = None
        self.cur = None
    def connect(self):
        if self.conn: return {'code': 0}
        try:
            self.conn = mdb.connect(self.host, self.user, self.passwd, self.db)
            self.conn.set_character_set(self.charset)
            self.cur = self.conn.cursor()
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e}
        return {'code': 0}

    def fetchOne(self, sql):
        ret = self.connect()
        if ret['code']: return ret

        try:
            q = self.cur.execute(sql)
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}

        try:
            row = self.cur.fetchone()
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}

        return {'code': 0, 'row': row, 'sql': sql}

    def fetchOneDict(self, sql):
        ret = self.fetchOne(sql)
        if ret['code']: return ret
        row = ret['row']
        if not row: return {'code': 0, 'dict': {}, 'sql': sql}
        desc = self.cur.description
        dic = {}
        for i in xrange(0, len(row)):
            dic[desc[i][0]] = row[i]
        return {'code': 0, 'dict': dic, 'sql': ret['sql']}

    def simpFetchOneDict(self, tbname, fieldsTup, condDic):
        ret = self.connect()
        if ret['code']: return ret
        sql = self.makeSimpQuerySQL(tbname, fieldsTup, condDic)
        ret = self.fetchOneDict(sql)
        return ret

    def fetchAll(self, sql):
        ret = self.connect()
        if ret['code']: return ret
        try:
            q = self.cur.execute(sql)
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}

        try:
            rows = self.cur.fetchall()
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}
        return {'code': 0, 'rows': rows, 'count': q, 'sql': sql}

    def fetchAllDict(self, sql):
        ret = self.fetchAll(sql)
        if ret['code']: return ret
        desc = self.cur.description
        rows = ret['rows']
        if not rows: return {'code': 0, 'rows': [], 'count': 0, 'sql': sql}
        count = ret['count']
        lst = []
        for row in rows:
            dic = {}
            for i in xrange(0, len(row)):
                dic[desc[i][0]] = row[i]
            lst.append(dic)
        return {'code': 0, 'rows': lst, 'count': count, 'sql': sql}

    def simpFetchAllDict(self, tbname, fieldsTup, condDic):
        ret = self.connect()
        if ret['code']: return ret
        sql = self.makeSimpQuerySQL(tbname, fieldsTup, condDic)
        ret = self.fetchAllDict(sql)
        return ret

    def makeInsertSQL(self, tbname, dic):
        keys = string.join(dic.keys(), "`,`")
        values = string.join(dic.values(), "','")
        sql = """insert into `%s` (`%s`) values ('%s') """ % (tbname, keys, values)
        return sql

    def makeUpdateSQL(self, tbname, dic, condDic):
        setValue = string.join([str(x[0]) + " = '" + str(x[1]) + "'" for x in dic.items()], ",")
        cond = string.join([ str(x[0]) + "='" + str(x[1]) + "'" for x in condDic.items()], " and ")
        sql = """update `%s` set %s where %s """ % (tbname, setValue, cond)
        return sql

    def makeSimpQuerySQL(self, tbname, fieldsTup, condDic):
        cond = string.join([ str(x[0]) + "='" + str(x[1]) + "'" for x in condDic.items()], " and ")
        if type('a') == type(fieldsTup): fields = fieldsTup
        else: fields = ', '.join(fieldsTup)
        sql = """select %s from %s where %s """ % (fields, tbname, cond)
        return sql

    def insertUpdateDict(self, tbname, prim, fieldDic):
        ret = self.connect()
        if ret['code']: return ret
        _fieldDic = {prim: fieldDic[prim]}
        sql = self.makeSimpQuerySQL(tbname, prim, _fieldDic) 
        ret = self.fetchOne(sql)
        if ret['code']: return ret
        row = ret['row']
        if not row: return self.insertDict(tbname, fieldDic)
        primaryDic = {prim: row[0]}
        sql = self.makeUpdateSQL(tbname, fieldDic, primaryDic)
        try:
            q = self.cur.execute(sql)
            self.conn.commit()
        except mdb.Error as e:
            return {'code': 0, 'errmsg': e, 'sql': sql}
        return {'code': 0, 'sql': sql}

    def insertDict(self, tbname, dic):
        ret = self.connect()
        if ret['code']: return ret
        sql = self.makeInsertSQL(tbname, dic)
        try:
            q = self.cur.execute(sql)
            self.conn.commit()
            return {'code': 0, 'sql': sql}
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}

    def countRows(self, sql):
        ret = self.connect()
        if ret['code']: return ret
        try:
            q = self.cur.execute(sql)
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}
        count = self.cur.fetchone()[0]
        return {'code': 0, 'count': count, 'sql': sql}

    def deleteRowDict(self, tbname, condDic):
        ret = self.connect()
        if ret['code']: return ret
        cond = string.join([ str(x[0]) + "='" + str(x[1]) + "'" for x in condDic.items()], " and ")
        sql = "delete from `%s` where %s" % (tbname, cond)
        try:
            q = self.cur.execute(sql)
            self.conn.commit()
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}
        return {'code': 0, 'count': q, 'sql': sql}

    def cleanTable(self, tbname):
        ret = self.connect()
        if ret['code']: return ret
        sql = "delete from `%s`" % tbname
        try:
            q = self.cur.execute(sql)
            self.conn.commit()
        except mdb.Error as e:
            return {'code': 1, 'errmsg': e, 'sql': sql}
        return {'code': 0, 'sql': sql}

    def close(self):
        if self.cur: self.cur.close()
        if self.conn: self.conn.close()
        return {'code': 0}
        
db = SimDb(host=host, user=user, passwd=password, db=database)

import os
import sqlite3 as sqlite

class DBSqlite(object):
    def __init__(self, dbname='sqlite.db'):
        self.errstr = 'sqlite err'
        self.connection = self.Connect(dbname)

    def Connect(self, dbname):
        self.dbname = dbname
        self.con = sqlite.connect(dbname)

    def Error(self, err, sql, bind_arr):
        print(self.errstr, err, sql, bind_arr)

    def Query(self, sql, bind_arr =None):
        self.cur = self.con.cursor()
        try:
            if(bind_arr == None):
                return self.cur.execute(sql)
            else:
                return self.cur.execute(sql, bind_arr)
        except Exception as e:
            self.Error(str(e), sql, bind_arr)

    def Update(self, sql, bind_arr=None, many=False):
        try:
            self.cur = self.con.cursor()
            if(bind_arr == None):
                if(many):
                   self.cur.executemany(sql)
                else:
                   self.cur.execute(sql)
            else:
                if(many):
                   self.cur.executemany(sql, bind_arr)
                else:
                   self.cur.execute(sql, bind_arr)
            self.con.commit()
            self.cur.close()
        except Exception as e:
            self.Error(str(e), sql, bind_arr)

    def List(self):
        sql = "select name from sqlite_master where type = 'table'"
        self.Query(sql)
        for row in self.cur:
            print(row[0])
        self.cur.close()

    def ListTableInfo(self, db_table):
        sql = "pragma table_info('" + db_table + "')"
        self.Query(sql)
        for row in self.cur:
            print(row)
        self.cur.close()


    def CreateOrOpen(self, db_file, db_table):
        self.con.close()
        self.Connect(db_file)
        bind_arr = [db_table]
        sql = "select count(name) from sqlite_master where type='table' AND name = ?"
        self.Query(sql, bind_arr)
        if(self.cur.fetchone()[0] > 0):
            print(db_table, "exists")
        else:
            sql = "create table " + db_table + "("
            sql += """ id integer primary key autoincrement,
                       name test,
                       value float,
                       type int,
                       unique (name) );
                   """
            self.Update(sql)

dbname='dbtest.db'
dbtable='test_table'
obj=DBSqlite(dbname)
obj.CreateOrOpen(dbname, dbtable)
obj.List()
obj.ListTableInfo(dbtable)

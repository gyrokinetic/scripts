import pymysql

class DBMysql(object):
    def __init__(self, hostname, port, uname, passwd, dbname):

        self.hostname = hostname
        self.port = port
        self.uname = uname
        self.passwd = passwd
        self.dbname = dbname
        self.Connect()

    def Connect(self):
        self.con = pymysql.connect(host=self.hostname, user=self.uname, password = self.passwd, database = self.dbname)

    def Error(self, err, sql, bind_arr):
        self.err = err
        print(err, sql, bind_arr)

    def Query(self, sql, bind_arr =None):
        try:
            self.cur = self.con.cursor()
            if(bind_arr == None):
                return self.cur.execute(sql)
            else:
                return self.cur.execute(sql, bind_arr)
            return True
        except Exception as e:
            print("query error")
            self.Error(str(e), sql, bind_arr)
            return False

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
            return True
        except Exception as e:
            self.Error(str(e), sql, bind_arr)
            return False

    def List(self):
        sql = "show tables"
        res = self.Query(sql)
        if(res):
           for row in self.cur:
              print(row[0])
           self.cur.close()

    def CreateOrOpen(self, dbname, db_table):
        sql = "describe " + dbname + "." + db_table
        res = self.Query(sql)
        if(res):
            print(db_table, "exists")
        else:
            sql = "create table " + dbname + "." + db_table + "("
            sql += """ id int auto_increment,
                       name varchar(255),
                       value float,
                       type int,
                       primary key (id),
                       unique key (name) );
                   """
            self.Update(sql)

ip=''
if(len(ip) < 1):
   with open("/etc/resolv.conf", 'r') as fi:
      for aline in fi:
         if('nameserver' in aline):
            ip = aline.replace(r'nameserver', "").strip()

print(ip)
dbname='mdx'
dbtable='test_table'
obj=DBMysql(ip, '', 'stock', 'stock123', 'stockdb')
obj.CreateOrOpen(dbname, dbtable)
obj.List()

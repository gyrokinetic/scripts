import MySQLdb

class DbMysql:

   def __init__(self, hostname='acerus', username='stock', password='stock123', database='stockDB'):
      self.hostname = hostname
      self.username = username
      self.password = password
      self.database = database
      self.db = MySQLdb.connect(hostname, username, password, database)

   def Commit(self):
      self.db.commit()

   def Execute(self, sql, sql_arr = None):
      try:
         self.cursor = self.db.cursor()
         if(sql_arr is None):
            self.cursor.execute(sql)
         elif(isinstance(sql_arr, list)):
            if(isinstance(sql_arr[0], tuple)):
               print ("enter many option")
               self.cursor.executemany(sql, sql_arr)
            else:
               #print "enter single option"
               self.cursor.execute(sql, sql_arr)
         elif(isinstance(sql_arr, tuple)):
            #print "tuple parameter: ", sql_arr
            self.cursor.execute(sql, sql_arr)
         else:
            #print "unknow parameter: ", sql_arr
            self.cursor.execute(sql)
      except MySQLdb.Error as e:
         print ("Query error: ", str(e))
         print ("sql: ", sql)
         print ("sql_arr: ", sql_arr)
         self.cursor.close()
         self.cursor = None
   
   def GetNextRow(self):
      return self.cursor.fetchone()


"""
# INSERT INTO Songs (SongName, SongArtist, SongAlbum, SongGenre, SongLength, SongLocation) VALUES (%s, %s, %s, %s, %s, %s), (var1, var2, var3, var4, var5, var6)

# c.executemany("insert into T (F1,F2) values (%s, %s)", [('a','b'),('c','d')])

db = DbMysql()
sql="drop database if exists stockdb"
db.Execute(sql)
sql="create database if not exists stockdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
db.Execute(sql)
#sql="drop table if exists stockdb.stock_symbol, stockdb.stock_ticker"
#db.Execute(sql)
sql="create table if not exists stockdb.stock_ticker (id int auto_increment, ticker varchar(11), name varchar(55), price double, mktcap double, ADRTSO varchar(25), IPOYear smallint, sector varchar(25), summaryquote varchar(51), stockexchange varchar(11) default 'unknown', created datetime, updated timestamp, primary key (id), unique key(ticker, stockexchange)) engine=Innodb default charset=utf8"
db.Execute(sql)

Symbol  Name    LastSale        MarketCap       ADR_TSO IPOyear Sector  Industry        Summary_Quote   NASDAQ
"""

import requests, json, ntpath, sys, os, base64, glob, time, random
import DbMysql
import urllib3
urllib3.disable_warnings()

uri='https://www.alphavantage.co/query?outputsize=full&function=TIME_SERIES_DAILY&apikey=05GL5RTG94H196TR'

daily_qs = { "outputsize":"full", "function":"TIME_SERIES_DAILY" }


class Stocks:
   def __init__(self, uri='https://www.alphavantage.co/query?', apikey='05GL5RTG94H196TR'):
      self.uri = uri
      self.apikey = apikey
      self.db = DbMysql.DbMysql(hostname='rasp1', username='stock', password='stock123', database='stockdb')
      self.dirname = 'SymbolDirectory'
      self.fileMap = {'nasdaqlisted.txt':"nasdaqlisted.txt", 'otherlisted.txt':"otherlisted.txt"}

   def Download(self):
      ftpUrl = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/"
      for f in ('nasdaqlisted.txt', 'otherlisted.txt'):
         if os.path.exists(f):
            os.remove(f)
         os.system('wget ' + ftpUrl + f)

   def LoadNasdaqSymbol(self, fname='nasdaqlisted.txt'):
      row = header = stats = insertSQL = valueBind = None
      with open(fname, 'r') as fr:
         for aline in fr:
            aline = aline.strip().lower()
            row = aline.split("|")
            if(header == None):
               header = row
               stats = []
               row = None
               for ict in range(len(header)):
                  header[ict] = header[ict].replace(" ", "_")
                  if(valueBind == None):
                     valueBind = '%s'
                  else:
                     valueBind = valueBind + ',%s'
                  stats.append(-1)
               insertSQL = "insert into stock_nasdaq (" + ",".join(header) + ") values (" + valueBind + ")"
            if(isinstance(row, list) and row[3] == 'n'):
               for ict in range(len(header)):
                  stats[ict] = max(stats[ict], len(row[ict]))
               print (row[0], row[6])
               self.db.Execute(insertSQL, row)
      ticker = header[0].replace(" ", "_")
      sz = str(stats[0])
      sql = "create table if not exists stock_nasdaq (id int auto_increment, "
      sql = sql + ticker + " varchar(" + sz + ")"
      for ict in range(1, len(header)):
         sz = str(stats[ict])
         print(ict, header[ict] + "=>" + sz)
         sql = sql + "," + header[ict] + " varchar(" + sz + ")"
      sql = sql + ", num_fail int default 0, num_rec int default 0, error varchar(127) default '', status char(1) default 'a', start_date date default '1800-01-01', end_date date default '1800-01-01', lastupd timestamp, primary key(id), unique key(" + ticker + "));"
      self.db.Execute(sql)
      print(sql)

   def LoadOtherSymbol(self, fname='otherlisted.txt'):
      row = header = stats = insertSQL = valueBind = None
      with open(fname, 'r') as fr:
         for aline in fr:
            aline = aline.strip().lower()
            row = aline.split("|")
            if(header == None):
               header = row
               stats = []
               row = None
               for ict in range(len(header)):
                  header[ict] = header[ict].replace(" ", "_")
                  if(valueBind == None):
                     valueBind = '%s'
                  else:
                     valueBind = valueBind + ',%s'
                  stats.append(-1)
               insertSQL = "insert into stock_other (" + ",".join(header) + ") values (" + valueBind + ")"
            if(isinstance(row, list) and row[6] == 'n'):
               for ict in range(len(header)):
                  stats[ict] = max(stats[ict], len(row[ict]))
               print (row[0], row[1], row[4])
               self.db.Execute(insertSQL, row)
      for ict in range(len(header)):
         print(header[ict] + "=>" + str(stats[ict]))

      ticker = header[0]
      sz = str(stats[0])
      sql = "create table if not exists stock_other (id int auto_increment, "
      sql = sql + ticker + " varchar(" + sz + ")"
      for ict in range(1, len(header)):
         sz = str(stats[ict])
         print(ict, header[ict] + "=>" + sz) 
         sql = sql + "," + header[ict] + " varchar(" + sz + ")"
      sql = sql + ", num_fail int default 0, num_rec int default 0, error varchar(127) default '', status char(1) default 'a', start_date date default '1800-01-01', end_date date default '1800-01-01', lastupd timestamp, primary key(id), unique key(" + ticker + "));"
      self.db.Execute(sql)
      print(sql)

   def GetJson(self, url, ticker='IBM'):
      headers = {'Content-type': 'application/json'}

      try:
         response=requests.get(url+'&symbol='+ticker, headers=headers, verify=False)
      except Exception as e:
         json_obj = dict()
         json_obj['error'] = str(e)
         return json_obj
      return json.loads(response.text)

   def GetURL(self, daily_qs):
      uri = self.uri + "apikey=" + self.apikey
      for k,v in daily_qs.items():
         uri = uri + '&' + k + '=' + v
      return uri
   
   def GetStockDaily(self, daily_qs, ticker='IBM'):
      uri = self.GetURL(daily_qs)
      return self.GetJson(uri, ticker)
   

   def OutputStock(self, dirName, ticker, db_st='1800-01-01', db_en='1800-01-01'):
      json_obj = self.GetStockDaily(daily_qs, ticker)
      if(len(json_obj) < 1):
         print("No stock data for " + ticker)
         return
      if("Time Series (Daily)" in json_obj):
         json_daily=json_obj["Time Series (Daily)"]
      else:
         print("no daily result")
      foutput = dirName + "/" + ticker + ".csv"
      dt_arr = sorted(json_daily.keys())
      with open(foutput, "a") as f:
         num = 0
         st = en = None
         for dt in dt_arr:
            en = str(dt)
            if(st == None):
               st = en
            if(db_st == '1800-01-01' or en > db_en):
               #print("date=" + dt)
               row = []
               row.append(en)
               rec = json_daily[dt]
               #print(rec)
               row.append(rec["1. open"])
               row.append(rec["2. high"])
               row.append(rec["3. low"])
               row.append(rec["4. close"])
               row.append(rec["5. volume"])
               aline = ",".join(row)
               f.write(aline + "\n")
               num += 1
         print(num)
      return st, en, num

   def UpdateStock(self, rc=None):
   
      sqlMap = dict()
      sqlQuery = dict()
      #sql = sql + ", num_fail int default 0, num_rec int default 0, error varchar(127) default '', status char(1) default 'a', start_date date default '1800-01-01', end_date date default '1800-01-01', lastupd timestamp, primary key(id), unique key(" + ticker + "));"
      sqlQuery['query'] = "select act_symbol, id, start_date, end_date, num_rec, num_fail from stockdb.stock_other where (end_date < date_sub(now(), interval 7 day) or status = 'a') and act_symbol not like '%^%' and num_fail < 5 order by rand()"
      sqlQuery['update'] = "update stockdb.stock_other set num_rec = %s, num_fail = 0, start_date = %s, end_date =%s where id = %s"
      sqlQuery['error'] = "update stockdb.stock_other set error = %s, num_fail = %s where id = %s"

      sqlQuery['query'] = "select symbol, id, start_date, end_date, num_rec, num_fail from stockdb.stock_nasdaq where (end_date < date_sub(now(), interval 7 day) or status = 'a') and symbol not like '%^%' and num_fail < 5 order by rand()"
      sqlQuery['update'] = "update stockdb.stock_nasdaq set num_rec = %s, num_fail = 0, start_date = %s, end_date =%s where id = %s"
      sqlQuery['error'] = "update stockdb.stock_nasdaq set error = %s, num_fail = %s where id = %s"

      sqlMap['stock'] = sqlQuery
      for fname, sqlQuery in sqlMap.items():
         sql = sqlQuery['query']
         if(rc != None):
            sql = sql + " limit " + str(rc)
         self.db.Execute(sql)
   
         cnt = 0
         ticker_arr = []
         for t in self.db.cursor:
            ticker_arr.append(t)
            cnt = cnt + 1
      
         for row in ticker_arr:
            v = str(row[0]).replace('$', '-p')
            k = str(row[1])
            db_st = str(row[2])
            db_en = str(row[3])
            num_rec = row[4]
            num_fail = row[5]
            print("stock=", v, " id=", k)
            time.sleep(random.uniform(0, 320))
            bindv = []
            try:
               st, en, num = self.OutputStock("test", v, db_st, db_en)
               bindv.append(str(num+int(num_rec)))
               bindv.append(db_st)
               bindv.append(en)
               bindv.append(k)
               sql = sqlQuery['update']
            except Exception as e:
               err = str(e).replace("'", "")
               print("error="+err)
               bindv.append(err)
               bindv.append(str(1+int(num_fail)))
               bindv.append(str(k))
               sql = sqlQuery['error']
            self.db.Execute(sql, bindv)
            self.db.Commit()
      
         print (cnt)
      return en, num

   def UpdateStockAll(self, rc=None):
   
      sqlMap = dict()
      sqlQuery = dict()
      sqlQuery['query'] = "select ticker, id, date_st, date_en, num_rec, num_fail from stockdb.stock_all where (date_en < date_sub(now(), interval 7 day) or status = 'a') and ticker not like '%^%' and num_fail < 5 order by rand()"
      sqlQuery['update'] = "update stockdb.stock_all set num_rec = %s, num_fail = 0, date_st = %s, date_en =%s where id = %s"
      sqlQuery['error'] = "update stockdb.stock_all set error = %s, num_fail = %s where id = %s"

      sqlMap['stock'] = sqlQuery
      for fname, sqlQuery in sqlMap.items():
         sql = sqlQuery['query']
         if(rc != None):
            sql = sql + " limit " + str(rc)
         self.db.Execute(sql)
   
         cnt = 0
         ticker_arr = []
         for t in self.db.cursor:
            ticker_arr.append(t)
            cnt = cnt + 1
      
         for row in ticker_arr:
            v = str(row[0]).replace('$', '-p')
            k = str(row[1])
            db_st = str(row[2])
            db_en = str(row[3])
            num_rec = row[4]
            num_fail = row[5]
            print("stock=", v, " id=", k)
            time.sleep(random.uniform(0, 320))
            bindv = []
            try:
               st, en, num = self.OutputStock("test", v, db_st, db_en)
               bindv.append(str(num+int(num_rec)))
               bindv.append(db_st)
               bindv.append(en)
               bindv.append(k)
               sql = sqlQuery['update']
            except Exception as e:
               err = str(e).replace("'", "")
               print("error="+err)
               bindv.append(err)
               bindv.append(str(1+int(num_fail)))
               bindv.append(str(k))
               sql = sqlQuery['error']
            self.db.Execute(sql, bindv)
            self.db.Commit()
      
         print (cnt)
      return st, en, num
       
    

ss = Stocks()
#ss.Download()
#ss.LoadOtherSymbol()
#ss.LoadNasdaqSymbol()
#ss.GetNasdaqStats()
st, en, num = ss.UpdateStockAll(498)
#st, en, num = crawler.OutputStock("test", "LEA", '1998-01-02', '2008-01-17')
print(en, num)

sys.exit(0)
# ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt

def GetOtherStats(self, fname='otherlisted.txt'):
      row = header = stats = None
      with open(fname, 'r') as fr:
         for aline in fr:
            aline = aline.strip()
            row = aline.split("|")
            if(header == None):
               header = row
               row = None
               stats = []
               for ict in range(len(header)):
                  stats.append(-1)
               
            if(isinstance(row, list) and row[6] == 'N'):
               for ict in range(len(header)):
                  stats[ict] = max(stats[ict], len(row[ict]))
      for ict in range(len(header)):
         print(header[ict] + "=>" + str(stats[ict]))

def GetNasdaqStats(self, fname='nasdaqlisted.txt'):
      row = header = stats = None
      with open(fname, 'r') as fr:
         for aline in fr:
            aline = aline.strip()
            row = aline.split("|")
            if(header == None):
               header = row
               row = None
               stats = []
               for ict in range(len(header)):
                  stats.append(-1)
               
            if(isinstance(row, list) and row[3] == 'N'):
               for ict in range(len(header)):
                  stats[ict] = max(stats[ict], len(row[ict]))
      for ict in range(len(header)):
         print(header[ict] + "=>" + str(stats[ict]))


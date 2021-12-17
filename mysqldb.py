import sqlite3
import pandas as pd
import decorators as decs


#Create database
#----------------
def connect_to_db():
  db = 'files/db.db'
  conn = sqlite3.connect(db)
  return conn

def create_db():
  conn = connect_to_db()
  cur = conn.cursor()
  create_table_claims(conn, cur)
  create_table_premium(conn, cur)
  create_table_expenses(conn, cur)
  create_table_patterns(conn, cur)
  create_table_ieulrs(conn, cur)
  create_table_lobs(conn, cur)
  create_table_used_tradecodes(conn, cur)
  conn.close()
  return None

def create_table_claims(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS claims (
    claim_id VARCHAR(255),
    uy INT,
    paid DOUBLE,
    case_ DOUBLE,
    reported DOUBLE,
    date_open DATE,
    date_current DATE,
    lob VARCHAR(255),
    tradecode VARCHAR(255),
    dim2 VARCHAR(255),
    dim3 VARCHAR(255),
    claim_type VARCHAR(255),
    dev_q INT,
    primary key (claim_id)
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_premium(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS premium (
    uy INT,
    lob VARCHAR(255),
    tradecode VARCHAR(255),
    dim2 VARCHAR(255),
    dim3 VARCHAR(255),
    gwp DOUBLE,
    policies INT  
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_expenses(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS expenses (
    uy INT,
    lob VARCHAR(255),
    expense DOUBLE 
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_patterns(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS patterns (
    lob VARCHAR(255),
    dev INT,
    FTU DOUBLE
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_ieulrs(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS ieulrs (
    uy INT,
    lob VARCHAR(255),
    ieulr DOUBLE 
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_lobs(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS lobs (
    lob VARCHAR(255)
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_used_tradecodes(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS used_tradecodes (
    lob VARCHAR(255),
    tradecode VARCHAR(255),
    gwp DOUBLE 
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

#sub functions
#-------------
def show_field_names(dbname, tablename):
  sqlstr = f"""SELECT 
  m.name as table_name, 
  p.name as column_name
FROM 
  sqlite_master AS m
JOIN 
  pragma_table_info(m.name) AS p
ORDER BY 
  m.name, 
  p.cid"""
  conn = connect_to_db()
  cur = conn.cursor()
  cur.execute(sqlstr)
  result = cur.fetchall()
  conn.close()
  return result

@decs.show_records
def show_table(tablename, limit= 1000000000):
  # you can't pass table names as parameters
  # concatenate to sqlstr first
  sqlstr = f"""SELECT * 
  FROM {tablename} 
  LIMIT {limit}"""
  return sqlstr

def clean_table(conn, cur, tablename):
  # you can't pass table names as parameters
  # concatenate to sqlstr first
  sqlstr = f"DELETE FROM {tablename}"
  cur.execute(sqlstr)
  conn.commit()
  return None

def delete_table(conn, cur, tablename):
  sqlstr = f"DROP TABLE {tablename}"
  cur.execute(sqlstr)
  conn.commit()
  return None

def get_cursor(conn):
  return conn.cursor()  

def get_query_results(cur):
  return cur.fetchall()  

#inserting records
#-----------------
def insert_record_claims(cur, claim_id, uy, paid, case_, reported, date_open, date_current, lob, tradecode, dim2, dim3, claim_type, dev_q):
  sqlstr = """INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"""
  cur.execute(sqlstr,(claim_id, uy, paid, case_, reported, date_open, date_current, lob, tradecode, dim2, dim3, claim_type, dev_q))
  return None

def insert_record_premium(cur, uy, lob, tradecode, dim2, dim3, gwp,policies):
  sqlstr = "INSERT INTO premium VALUES (?,?,?,?,?,?,?)"
  cur.execute(sqlstr, (uy, lob, tradecode, dim2, dim3, gwp,policies))  
  return None

def insert_record_expenses(cur, uy, lob, expense):
  sqlstr = "INSERT INTO expenses VALUES (?,?,?)"
  cur.execute(sqlstr, (uy, lob, expense))
  return None

def insert_record_patterns(cur, lob, dev, FTU):
  sqlstr = """INSERT INTO patterns VALUES (?,?,?)"""
  cur.execute(sqlstr,(lob, dev, FTU))
  return None

def insert_record_ieulrs(cur, uy, lob, ieulr):
  sqlstr = "INSERT INTO ieulrs VALUES (?,?,?)"
  cur.execute(sqlstr,(uy, lob, ieulr))
  return None

def insert_record_lobs(cur, lob):
  sqlstr = "INSERT INTO lobs VALUES (?)"
  cur.execute(sqlstr,(lob,))
  return None

def insert_record_used_tradecodes(cur, lob, tradecode, gwp):
  sqlstr = "INSERT INTO used_tradecodes VALUES (?,?,?)"
  cur.execute(sqlstr,(lob, tradecode, gwp))
  return None

#uploading csv files
#------------------
#appreciate record by record is a slow way of doing it
#live app will work of Snowflake so this test code will become obsolete
def upload_all_tables():
  upload_claims_csv()
  upload_premium_csv()
  upload_expenses_csv()
  upload_patterns_csv()
  upload_ieulrs_csv()
  upload_lobs_csv()


def upload_claims_csv():
  conn = connect_to_db()
  cur = conn.cursor()
  #clean tablename
  clean_table(conn, cur, "claims")
  df = pd.read_csv("files/claim_table_random.csv")
  for i, row in df.iterrows():
    insert_record_claims(
      cur, 
      row['claim_id'],
      row['uy'],
      row['paid'],
      row['case'],
      row['reported'],
      row['date_open'],
      row['date_current'],
      row['lob'],
      row['tradecode'],
      row['dim2'],
      row['dim3'],
      row['claim_type'],
      (int(row['date_current'][-4:]) - int(row['date_open'][-4:]) + 1)*4
      )
  conn.commit()
  conn.close()
  return None

def upload_premium_csv():
  conn = connect_to_db()
  cur = conn.cursor()
  #clean tablename
  clean_table(conn, cur, "premium")
  df = pd.read_csv("files/premium_table_random.csv")
  for i, row in df.iterrows():
    insert_record_premium(
      cur,
      row['uy'],
      row['lob'],
      row['tradecode'],
      row['dim2'],
      row['dim3'],
      row['gwp'],
      row['policies']
      )
  conn.commit()
  conn.close()
  return None
  
def upload_expenses_csv():
  conn = connect_to_db()
  cur = conn.cursor()
  #clean tablename
  clean_table(conn, cur, "expenses")
  df = pd.read_csv("files/expense_table_random.csv")
  for i, row in df.iterrows():
    insert_record_expenses(
      cur,
      row['uy'],
      row['lob'],
      row['expense']
      )
  conn.commit()
  conn.close()
  return None

def upload_patterns_csv():
  conn = connect_to_db()
  cur = conn.cursor()
  #clean tablename
  clean_table(conn, cur, "patterns")
  df = pd.read_csv("files/patterns_FTU.csv")
  for i, row in df.iterrows():
    insert_record_patterns(
      cur,
      row['lob'],
      row['dev'],
      row['FTU']
      )
  conn.commit()
  conn.close()
  return None

def upload_ieulrs_csv():
  conn = connect_to_db()
  cur = conn.cursor()
  #clean tablename
  clean_table(conn, cur, "ieulrs")
  df = pd.read_csv("files/IEULRs.csv")
  for i, row in df.iterrows():
    insert_record_ieulrs(
      cur,
      row['uy'],
      row['lob'],
      row['ieulr']
      )
  conn.commit()
  conn.close()
  return None

def upload_lobs_csv():
  conn = connect_to_db()
  cur = conn.cursor()
  #clean tablename
  clean_table(conn, cur, "lobs")
  df = pd.read_csv("files/lines_of_business.csv")
  for i, row in df.iterrows():
    insert_record_lobs(
      cur,
      row['lob']
      )
  conn.commit()
  conn.close()
  return None  

def update_premium():
  conn = connect_to_db()
  cur = conn.cursor()
  sqlstr = """UPDATE premium
  SET gwp = gwp*2/100
  """  
  cur.execute(sqlstr)
  conn.commit()
  conn.close()


if __name__ == "__main__":
  pass
  #create_db()
  #upload_all_tables()
  #upload_patterns_csv()
  #upload_ieulrs_csv()
  #conn = connect_to_db()
  #cur = conn.cursor()
  #clean_table(conn, cur, 'patterns')
  #upload_patterns_csv()
  #delete_table(conn, cur, 'premium')
  #create_table_premium(conn, cur)
  #upload_premium_csv()
  #create_table_ieulrs(conn, cur)
  #upload_ieulrs_csv()
  #create_table_patterns(conn, cur)
  #create_table_lobs(conn, cur)
  #upload_lobs_csv()
  #upload_patterns_csv()
  #show_table("claims",limit=50)
  #print("--------")
  #show_table("premium",limit=5)
  #print("--------")
  #show_table("expenses",limit=10)
  #print("--------")
  show_table("patterns")
  #print("--------")
  #show_table("ieulrs",100)
  #show_table('lobs')
  #print(show_field_names('files/db.db','premium'))
  #create_table_used_tradecodes(conn, cur)
  #show_table('patterns', limit=10)
  #update_premium()
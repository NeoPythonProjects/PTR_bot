import sqlite3
import pandas as pd

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
    '3' DOUBLE,
    '6' DOUBLE,
    '9' DOUBLE,
    '12' DOUBLE,
    '15' DOUBLE,
    '18' DOUBLE,
    '21' DOUBLE,
    '24' DOUBLE,
    '27' DOUBLE,
    '30' DOUBLE,
    '33' DOUBLE,
    '36' DOUBLE,
    '39' DOUBLE,
    '42' DOUBLE,
    '45' DOUBLE,
    '48' DOUBLE,
    '51' DOUBLE,
    '54' DOUBLE,
    '57' DOUBLE,
    '60' DOUBLE,
    '63' DOUBLE,
    '66' DOUBLE,
    '69' DOUBLE,
    '72' DOUBLE,
    '75' DOUBLE,
    '78' DOUBLE,
    '81' DOUBLE,
    '84' DOUBLE,
    '87' DOUBLE,
    '90' DOUBLE,
    '93' DOUBLE,
    '96' DOUBLE,
    '99' DOUBLE,
    '102' DOUBLE,
    primary key(lob)
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None

def create_table_ieulrs(conn, cur):
  sqlstr = """CREATE TABLE IF NOT EXISTS ieulrs (
    lob VARCHAR(255),
    uy INT,
    ieulr DOUBLE 
  )"""
  cur.execute(sqlstr)
  conn.commit()
  return None


#sub functions
#-------------
def show_table(tablename, limit):
  # you can't pass table names as parameters
  # concatenate to sqlstr first
  sqlstr = f"SELECT * FROM {tablename} LIMIT {limit}"
  conn = connect_to_db()
  cur = conn.cursor()
  cur.execute(sqlstr)
  result = cur.fetchall()
  for rec in result:
    print(rec)
  return result

def clean_table(conn, cur, tablename):
  # you can't pass table names as parameters
  # concatenate to sqlstr first
  sqlstr = f"DELETE FROM {tablename}"
  cur.execute(sqlstr)
  conn.commit()
  return None


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

def insert_record_patterns(cur, lob, _3, _6, _9, _12, _15, _18, _21, _24, _27, _30, _33, _36, _39, _42, _45, _48, _51, _54, _57,_60, _63, _66, _69, _72, _75, _78, _81, _84, _87, _90,  _93, _96,_99,_102):
  sqlstr = """INSERT INTO patterns VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
  cur.execute(sqlstr,(lob, _3, _6, _9, _12, _15, _18, _21, _24, _27, _30, _33, _36, _39, _42, _45, _48, _51, _54, _57,_60, _63, _66, _69, _72, _75, _78, _81, _84, _87, _90,  _93, _96,_99,_102))
  return None

def insert_record_ieulrs(cur, uy, lob, ieulr):
  sqlstr = "INSERT INTO ieulrs VALUES (?,?,?)"
  cur.execute(sqlstr,(uy, lob, ieulr))
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
      row['dev_q']
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
      row['3'],
      row['6'],
      row['9'],
      row['12'],
      row['15'],
      row['18'],
      row['21'],
      row['24'],
      row['27'],
      row['30'],
      row['33'],
      row['36'],
      row['39'],
      row['42'],
      row['45'],
      row['48'],
      row['51'],
      row['54'],
      row['57'],
      row['60'],
      row['63'],
      row['66'],
      row['69'],
      row['72'],
      row['75'],
      row['78'],
      row['81'],
      row['84'],
      row['87'],
      row['90'],
      row['93'],
      row['96'],
      row['99'],
      row['102']
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

if __name__ == "__main__":
  pass
  #create_db()
  #upload_all_tables()
  #upload_patterns_csv()
  #upload_ieulrs_csv()
  show_table("claims",5)
  print("--------")
  show_table("premium",5)
  print("--------")
  show_table("expenses",5)
  print("--------")
  show_table("patterns",5)
  print("--------")
  show_table("ieulrs",5)
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

@decs.execute_sql('write')
def create_table_claims() -> str:
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
  return sqlstr


@decs.execute_sql('write')
def create_table_premium() -> str:
  sqlstr = """CREATE TABLE IF NOT EXISTS premium (
    uy INT,
    lob VARCHAR(255),
    tradecode VARCHAR(255),
    dim2 VARCHAR(255),
    dim3 VARCHAR(255),
    gwp DOUBLE,
    policies INT  
  )"""
  return sqlstr


@decs.execute_sql('write')
def create_table_expenses() -> str:
  sqlstr = """CREATE TABLE IF NOT EXISTS expenses (
    uy INT,
    lob VARCHAR(255),
    expense DOUBLE 
  )"""
  return sqlstr


@decs.execute_sql('write')
def create_table_patterns() -> str:
  sqlstr = """CREATE TABLE IF NOT EXISTS patterns (
    lob VARCHAR(255),
    dev INT,
    FTU DOUBLE
  )"""
  return sqlstr


@decs.execute_sql('write')
def create_table_ieulrs() -> str:
  sqlstr = """CREATE TABLE IF NOT EXISTS ieulrs (
    uy INT,
    lob VARCHAR(255),
    ieulr DOUBLE 
  )"""
  return sqlstr


@decs.execute_sql('write')
def create_table_lobs() -> str:
  sqlstr = """CREATE TABLE IF NOT EXISTS lobs (
    lob VARCHAR(255)
  )"""
  return sqlstr


@decs.execute_sql('write')
def create_table_used_tradecodes() -> str:
  sqlstr = """CREATE TABLE IF NOT EXISTS used_tradecodes (
    lob VARCHAR(255),
    tradecode VARCHAR(255),
    gwp DOUBLE 
  )"""
  return sqlstr


#sub functions
#-------------
# sqlstr builder for INSERT statements
def build_insert_sqlstr(*args, **kwargs) -> str:
  """sql string builder for INSERT statements
  The sql string builder looks at the args tuple and adds the required number of ? to the sql string.

  The insert statements only differ in terms of (i) number of arguments passed and (ii) tablename to insert into.
  
  The tablename can be passed as a named argument via f-string
  The fields to be inserted can be passsed as positional arguments
  
  """
  # **kwargs holds tablename
  # *args hold the positional arguments
  # example result: INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
  for k,v in kwargs.items():
    if k == 'tablename': tablename = v
  sqlstr= f"INSERT INTO {tablename} VALUES ("
  for el in args:
    sqlstr = sqlstr + "?"
    if args.index(el) < len(args)-1:
      sqlstr = sqlstr + ","
    else:
      sqlstr = sqlstr + ")"  
  return sqlstr


@decs.execute_sql('read')
def show_field_names() -> str:
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
  return sqlstr


@decs.execute_sql('read')
def show_table(*_,tablename, limit=1000000000) -> str:
# the generic decorator uses args in the cur.execute statement
# args is a tuple a positional arguments
# the execute statment expexts the sql statement to be written using ?
# args will then provide the values for the ?
# Unfortuately, tablenames cannot be passed as variables using ?
# therefore, use the f-string approach to feed in table names
# but then the tablename should not appear in args
# therefore, make table name a named argument, so it shows up in kwargs instead of args
# for limit, i want a default value, so that makes it a named argument
# which will not be in args, so pass that as an f-string variable too
# for table, i don't want a default value, so i need to point out that
# this is a named argument. Do that by showing the end of the postional
# arguments as *_. that makes tablename a named argument

# so, both tablename and limit are passed as named arguments:
# tablename because you can't use the ? method with execute for table names
# limit because i want a default value
# there are therefore no positional arguments in args
# this is fine as there are no ? in sql for which cur.execute is expecting an argument
# all arguments are passed to the sql string via f-string, not ?
  sqlstr = f"""SELECT * 
  FROM  {tablename}
  LIMIT {limit}
  """  
  return sqlstr


@decs.execute_sql('write')
def clean_table(*_, tablename):
  # you can't pass table names as parameters
  # concatenate to sqlstr first via f-string
  sqlstr = f"DELETE FROM {tablename}"
  return sqlstr


@decs.execute_sql('write')
def delete_table(*_, tablename):
  sqlstr = f"DROP TABLE {tablename}"
  return sqlstr


def get_cursor(conn):
  return conn.cursor()  


def get_query_results(cur):
  return cur.fetchall()  


#inserting records
#-----------------
@decs.execute_sql('write')
def insert_record(*args, tablename) -> str:
  """function replaces all the individual insert record functions
  
  arguments:
  *args: variable number of positional arguments: the data fields
  tablename: one named argument

  function calls the build_insert_sqlstr() function to dynamically build up the sql string based on the table name and the number of passed postional arguments.

  decorated with execute_sql('write') for database interaction
  """
  return build_insert_sqlstr(*args, tablename=tablename)


# @decs.execute_sql('write')
# def insert_record_claims(claim_id, uy, paid, case_, reported, date_open, date_current, lob, tradecode, dim2, dim3, claim_type, dev_q) -> str:
#   sqlstr = """INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"""
#   return sqlstr


# @decs.execute_sql('write')
# def insert_record_premium(uy, lob, tradecode, dim2, dim3, gwp,policies) -> str:
#   sqlstr = "INSERT INTO premium VALUES (?,?,?,?,?,?,?)"  
#   return sqlstr


# @decs.execute_sql('write')
# def insert_record_expenses(uy, lob, expense) -> str:
#   sqlstr = "INSERT INTO expenses VALUES (?,?,?)"
#   return sqlstr


# @decs.execute_sql('write')
# def insert_record_patterns(lob, dev, FTU) -> str:
#   sqlstr = """INSERT INTO patterns VALUES (?,?,?)"""
#   return sqlstr


# @decs.execute_sql('write')
# def insert_record_ieulrs(uy, lob, ieulr) -> str:
#   sqlstr = "INSERT INTO ieulrs VALUES (?,?,?)"
#   return sqlstr


# @decs.execute_sql('write')
# def insert_record_lobs(lob) -> str:
#   sqlstr = "INSERT INTO lobs VALUES (?)"
#   return sqlstr


# @decs.execute_sql('write')
# def insert_record_used_tradecodes(lob, tradecode, gwp) -> str:
#   sqlstr = "INSERT INTO used_tradecodes VALUES (?,?,?)"
#   return sqlstr


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


def upload_claims_csv() -> None:
  # clean table has been decorated with execute_sql('write')
  clean_table(tablename="claims")
  df = pd.read_csv("files/claim_table_random.csv")
  for i, row in df.iterrows():
    print(f'reading row {i} of {len(df)}')
    # insert_record is decorated with execute_sql('write')
    insert_record(
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
      (int(row['date_current'][-4:]) - int(row['date_open'][-4:]) + 1)*4,
      tablename='claims'
      )
  return None


def upload_premium_csv() -> None:
  #clean tablename has been decorated with execute_sql('write')
  clean_table(tablename="premium")
  df = pd.read_csv("files/premium_table_random.csv")
  for i, row in df.iterrows():
    # has been decorated with execute_sql('write')
    insert_record(
      row['uy'],
      row['lob'],
      row['tradecode'],
      row['dim2'],
      row['dim3'],
      row['gwp'],
      row['policies'],
      tablename='premium'
      )
  return None
  

def upload_expenses_csv() -> None:
  #clean tablename has been decorated with execute_sql('write')
  clean_table(tablename="expenses")
  df = pd.read_csv("files/expense_table_random.csv")
  for i, row in df.iterrows():
    # has been decorated with execute_sql('write')
    insert_record(
      row['uy'],
      row['lob'],
      row['expense'],
      tablename='expenses'
      )
  return None


def upload_patterns_csv() -> None:
  #clean tablename has been decorated with execute_sql('write')
  clean_table(tablename="patterns")
  df = pd.read_csv("files/patterns_FTU.csv")
  for i, row in df.iterrows():
    # has been decorated with execute_sql('write')
    insert_record(
      row['lob'],
      row['dev'],
      row['FTU'],
      tablename='patterns'
      )
  return None


def upload_ieulrs_csv() -> None:
  #clean tablename has been decorated with execute_sql('write')
  clean_table(tablename="ieulrs")
  df = pd.read_csv("files/IEULRs.csv")
  for i, row in df.iterrows():
    # has been decorated with execute_sql('write')
    insert_record(
      row['uy'],
      row['lob'],
      row['ieulr'],
      tablename='ieulrs'
      )
  return None


def upload_lobs_csv() -> None:
  #clean tablename has been decorated with execute_sql('write')
  clean_table(tablename="lobs")
  df = pd.read_csv("files/lines_of_business.csv")
  for i, row in df.iterrows():
    # has been decorated with execute_sql('write')
    insert_record(
      row['lob'],
      tablename='lobs'
      )
  return None  


@decs.execute_sql('write')
def update_premium() -> str:
  sqlstr = """UPDATE premium
  SET gwp = gwp*2/100
  """  
  return sqlstr


if __name__ == "__main__":
  pass
  #create_db()
  #upload_all_tables()
  #upload_patterns_csv()
  #upload_ieulrs_csv()
  #clean_table(tablename = 'patterns')
  #upload_claims_csv()
  #upload_premium_csv()
  #upload_expenses_csv()
  #upload_ieulrs_csv()
  #upload_lobs_csv()
  #upload_patterns_csv()
  show_table(tablename='used_tradecodes', limit=10)
  #print("--------")
  #show_table("premium",limit=5)
  #print("--------")
  #show_table("expenses",limit=10)
  #print("--------")
  #show_table("patterns")
  #print("--------")
  #show_table("ieulrs",100)
  #show_table('lobs')
  #print(show_field_names('files/db.db','premium'))
  #create_table_used_tradecodes(conn, cur)
  #show_table('patterns', limit=10)
  #update_premium()
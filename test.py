import decorators as decs
import pandas as pd


def build_insert_sqlstr(*args, **kwargs) -> str:
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


def upload_csv(*_, tablename):
  df = pd.read_csv("files/patterns_FTU.csv")
  for i, row in df.iterrows():
    insert_record_patterns(
      row['lob'],
      row['dev'],
      row['FTU']
      )
  return None


@decs.execute_sql('write')
def insert_record_patterns(lob, dev, FTU) -> str:
  sqlstr = """INSERT INTO patterns VALUES (?,?,?)"""
  #cur.execute(sqlstr,(lob, dev, FTU))
  return sqlstr


@decs.execute_sql('write')
def delete_all_records(*_,tablename):
  sqlstr = f"""
  DELETE
  FROM {tablename}
  """
  return sqlstr

if __name__ == "__main__":
  #show_table(tablename = "claims", limit = 5)  
  #insert_record_patterns('test', 8, 555)
  #delete_all_records(tablename = 'patterns')
  #upload_csv(tablename = 'patterns')
  #show_table(tablename = 'patterns', limit=20)
  print(build_insert_sqlstr('a', tablename='claims'))

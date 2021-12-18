import decorators as decs
import pandas as pd


@decs.show_records # works but want to try the more generic decorator
#@decs.interact_with_db('read')
def show_table_old(tablename, limit=1000000000) -> str:
  sqlstr = f"""SELECT *
  FROM {tablename}
  LIMIT {limit}
  """  
  return sqlstr


@decs.interact_with_db('read')
def show_table(*_,tablename, limit=1000000000) -> str:
# the generic decorator uses args in teh cur.execute statement
# args is a tuple a positional arguments
# the execute statment expexts the sql statement to be written using ?
# args will then provide the values for the ?
# Unfortuately, tablenames cannot be passed as variables using ?
# therefore, use the f-string approach to feed intable names
# but then the tablename should not appear in args
# therefore, make table name a named argument, so it shows up in kwargs instead of args
# for limit, i want a default value, so that makes it a named argument
# which will not be in args, so pass that as a n f-string variable too
# for table, i don't want a default value, so i need to point out that
# this is a named argument. Do that by showing the ned of the postional
# arguments as *_. that makes tablename a named argument

# so, both tablename and limit are passed as named arguments:
# tablename because you can't use the ? method with execute for table names
# limit becasue i want a default value
# there are therefore no positional arguments in args
# this is fine as there are no ? in sql for which cur.execute is expecting an argument
# all arguments are passed to the sql string via f-string, not ?
  sqlstr = f"""SELECT * 
  FROM  {tablename}
  LIMIT {limit}
  """  
  return sqlstr


def upload_csv(*_, cur, tablename):
  df = pd.read_csv("files/patterns_FTU.csv")
  for i, row in df.iterrows():
    insert_record_patterns(
      cur,
      row['lob'],
      row['dev'],
      row['FTU']
      )
  # ----------------------------
  
  return None

#@decs.insert_record - works, but wanbt to test the more generic decorator
@decs.interact_with_db('write')
def insert_record_patterns(lob, dev, FTU) -> str:
  sqlstr = """INSERT INTO patterns VALUES (?,?,?)"""
  #cur.execute(sqlstr,(lob, dev, FTU))
  return sqlstr

def delete_records(table):
  pass
  #can use @decs.insert_record i think

if __name__ == "__main__":
  show_table(tablename = "claims", limit = 5)  
 #insert_record_patterns('test', 8, 555)
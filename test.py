import decorators as decs
import pandas as pd


@decs.show_records
def show_table(tablename, limit=1000000000) -> str:
  sqlstr = f"""SELECT *
  FROM {tablename}
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

@decs.insert_record
def insert_record_patterns(lob, dev, FTU) -> str:
  sqlstr = """INSERT INTO patterns VALUES (?,?,?)"""
  #cur.execute(sqlstr,(lob, dev, FTU))
  return sqlstr


if __name__ == "__main__":
  #show_table("claims", limit = 5)  
  insert_record_patterns('test', 4, 666)
import pandas as pd
import functions
from mysqldb import connect_to_db, get_cursor, get_query_results, clean_table


def main() -> None:
  pass
  #store_used_tradecodes()




def store_used_tradecodes() -> None:
  pass
  #1. load LOBs
  df_lobs = pd.DataFrame({'lob': functions.load_lobs()})
  #2. store lob-tradecode combinations that
  # (i) exist in the claims table
  # (ii) pass the premium threshold
  # 2.1 clean used_tradecodes table
  conn = connect_to_db()
  cur = get_cursor(conn)
  clean_table(conn, cur, 'used_tradecodes')
  conn.commit()
  conn.close()
  # 2.2 populate used_tradecodes table
  for i, row in df_lobs.iterrows():
    functions.tradecodes_used(row['lob'])
  return None

def loop_through_lob_tradecode_combos() -> None:
  # loop through existing combos
  # append resulting dataframes
  # the output is? build in an option
  # massive table with all combos?
  # exceptions only?
  # trends?

  #if output is all, i would stick in table
  # in sqlite3 that's record by record, but in sqlalchemy
  # i think you can import a whole dataframe into a table in one go
  
  pass


if __name__ == "__main__":
  main()
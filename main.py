import pandas as pd
import functions


def main() -> None:
  pass
  #store_used_tradecodes()
  #loop_through_lob_tradecode_combos()
  #print("end of loop")
  loop_through_user_defined_tradecode_lists()



def store_used_tradecodes() -> None:
  """function loads lobs from lobs table and checks claims and premium tables for existing lob-tradecode combos exceeding the premium threshold, and stores qualifying lob-tradecode combos in table used_tradecodes

  result = updated table used_tradecodes in database
  """
  #1. load LOBs
  df_lobs = pd.DataFrame({'lob': functions.load_lobs()})
  #2. store lob-tradecode combinations that
  # (i) exist in the claims table
  # (ii) pass the premium threshold
  # populate used_tradecodes table
  functions.clean_table(tablename='used_tradecodes')
  for i, row in df_lobs.iterrows():
    functions.tradecodes_used(row['lob'])
  return None


def loop_through_lob_tradecode_combos() -> None:
  """function loops through existing lob-tradecode combos and calculates ultimates bcl and bf for all uys in that combo. Results are stored in the output table
  """
  #clean output table
  functions.clean_table(tablename='output')
  # read existing combos
  df = functions.load_used_tradecodes()
  # loop through existing combos
  for i, row in df.iterrows():
    df_result = functions.ultimates_by_lob_tradecode_uy(
      row['lob'], 
      row['tradecode']
      )  
    #df_result columns = ['lob', 'tradecode', 'uy', 'dev', 'gwp', 'incd', 'ieulr', 'ftu', 'bcl', 'bf', 'exp', 'cor']
    for j, resultrow in df_result.iterrows():
      # insert_record has been decorated with execute_sql('write') to interact with db
      print(f"inserting {resultrow['lob']}, {resultrow['tradecode']}, {j}")
      functions.insert_record(
        resultrow['lob'],
        resultrow['tradecode'],
        resultrow['uy'],
        resultrow['dev'],
        resultrow['gwp'],
        resultrow['incd'],
        resultrow['ieulr'],
        resultrow['ftu'],
        resultrow['bcl'],
        resultrow['bf'],
        resultrow['exp'],
        resultrow['cor'],
        tablename='output'
        )


def loop_through_user_defined_tradecode_lists():
  df = functions.load_tradecodelists()
  #df[1] = "['tr1', 'tr3', 'tr6']"
  #split the string up into elements 
  for i, row in df.iterrows():
    lst = row['tradecodelist'].split(",")
    # remove bracket from first element
    lst[0] = lst[0][1:]
    # remove bracket from last element
    lst[-1] = lst[-1][:-1]
    # for all, remove leading and trailing '
    lst = [x[1:-1] for x in lst]
    # call function to calculate ultimates for this list
    df_result = functions.ultimates_by_lob_multiple_tradecodes_uy(
      row['lob'],
      lst
    )
    for j, resultrow in df_result.iterrows():
      functions.insert_record(
      resultrow['lob'],
      str(lst),
      resultrow['uy'],
      resultrow['dev'],
      resultrow['gwp'],
      resultrow['incd'],
      resultrow['ieulr'],
      resultrow['ftu'],
      resultrow['bcl'],
      resultrow['bf'],
      resultrow['exp'],
      resultrow['cor'],
      tablename='output'
      )




# the output is? build in an option
# massive table with all combos?
# exceptions only?
# trends?

#if output is all, i would stick in table
# in sqlite3 that's record by record, but in sqlalchemy
# i think you can import a whole dataframe into a table in one go
  
  


if __name__ == "__main__":
  main()

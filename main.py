import pandas as pd
import functions
import kpi


def main() -> None:
  store_used_tradecodes()
  loop_through_lob_tradecode_combos()
  loop_through_user_defined_tradecode_lists()
  run_statistics(1, 0.04)


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


def run_statistics(cor_threshold: float, trend_threshold: float) -> pd.DataFrame:
  """ function runs statistics on 
  1. cor: exceeding cor_threshold
  2. ieulr and bf ulr trends for slope exceeding trend_threshold
  """
  functions.clean_table(tablename='cor_kpi')
  functions.clean_table(tablename='trend_kpi')

  #cor test is performed on total output table
  df_cor = kpi.cor_exceeds_threshold(cor_threshold)
  for i, resultrow in df_cor.iterrows():
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
      tablename='cor_kpi'
    )
  # trend tests are performed at lob-tradecode level
  # run through existing lob-tradecode combos
  # retrieve them from output table to automatically include the user-defined tradecode lists
  df = functions.output_table_grouped()
  for i, row in df.iterrows():
    for stat in ['cor','ieulr','ulr']:
      res = kpi.trend_slope(row['lob'], row['tradecode'], stat)
      if (float(res[0]) < -(trend_threshold)) or (float(res[0]) > trend_threshold):
        if float(res[0]) > trend_threshold:
          #upward trend kpi
          trend_kpi = 'up'
        else:
          #downward trend kpi
          trend_kpi = 'down'
        #x_values and y_values arrays stored as text in one field
        functions.insert_record(
          stat,
          row['lob'],
          row['tradecode'],
          trend_kpi,
          res[0],
          str(res[1]),
          str(res[2]),
          tablename='trend_kpi'
        )


if __name__ == "__main__":
  main()

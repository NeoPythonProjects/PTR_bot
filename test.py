import decorators as decs
import pandas as pd
import sys

from sklearn.linear_model import LinearRegression


@decs.execute_sql('runquery')
def get_dataset_cursor(lob, tradecode) -> str:
  #tradecode can be a string representing a tradecodelist
  return """SELECT * FROM output
  WHERE lob = ? AND tradecode = ?
  """


def trend_slope(lob, tradecode, statistic) -> list:
  """function fits linear regression and calculates slope for the selected statistic.

  inputs: lob, tradecode and statistic ('cor', 'ieulr', 'ulr')
  
  returns list with 3 elements:
  0: slope
  1: array of x-values (uy)
  2: array of y-values (cor, ieulr or ulr)
  """
  #todo statistic can be 'cor', 'ieulr', 'ulr'
  result = result_to_output_df(get_dataset_cursor(lob,tradecode))
  x_values = result['uy'].values
  if statistic == 'cor':
    #df = pd.DataFrame({'uy': result['uy'], 'cor': result['cor']})
    y_values = result['cor'].values
  elif statistic == "ieulr":
    y_values = result['ieulr'].values
  elif statistic == "ulr":
    y_values = result['bf'].values/result['gwp']
  else:
    sys.exit(f"option {statistic} not recognised. use 'cor', 'ieulr' or 'ulr'.")
  reg = LinearRegression()
  reg.fit(x_values.reshape(-1,1), y_values)
  return [reg.coef_[0],x_values, y_values]
  

def result_to_output_df(result: list) -> pd.DataFrame:
  columns = ['lob', 'tradecode', 'uy', 'dev', 'gwp', 'incd', 'ieulr', 'ftu', 'bcl', 'bf', 'exp', 'cor']
  df = pd.DataFrame(result)
  df.columns = columns
  return df


if __name__ == "__main__":
  print(trend_slope("Onshore", "tr1", "ulr"))
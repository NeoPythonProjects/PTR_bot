import decorators as decs
import pandas as pd
import sys
from functions import result_to_output_df

from sklearn.linear_model import LinearRegression


#functions for trends
#--------------------
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
  


# functions for exceeding COR
#----------------------------
@decs.execute_sql('runquery')
def cor_exceeds_threshold_cursor(threshold: float) -> str:
  return f""" SELECT * FROM output
  WHERE cor > ?
   """

def cor_exceeds_threshold(threshold: float) -> pd.DataFrame:
  """function returns a DataFrame with output records exciiding the cor threshold
  """
  return result_to_output_df(cor_exceeds_threshold_cursor(threshold)) 
  
  
# Sub functions
#--------------
def print_to_shell(df: pd.DataFrame) -> None:
  for i, row in df.iterrows():
    print(i, row)


if __name__ == "__main__":
  pass
  #print_to_shell(cor_exceeds_threshold(1))
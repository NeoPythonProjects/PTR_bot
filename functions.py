import pandas as pd
from mysqldb import insert_record, clean_table
import decorators as decs


@decs.execute_sql('runquery')
def load_lobs_cursor():
  """function retrieves existing lobs
  
  return: cursor object, not a sqlstr; the undecorated function returns a sqlstr that feeds into the decorator, which, for action option 'runquery' returns a cursor object
  """
  sqlstr = "SELECT lob FROM lobs"
  return sqlstr
  

def load_lobs() -> list:
  results = load_lobs_cursor()
  #results is a list of tuples [('Onshore',),('EA',)]
  #list comprehension to extract element from tuple
  return [x[0] for x in results]


# functions for lists of tradecodes
#----------------------------------
def sql_builder_claims_where_tradecodelist(lob, lst, claims_table='claims') -> str:
  sqlsubstr_1 = f"SELECT c.lob, c.tradecode, c.uy, c.paid, c.case_, c.paid+c.case_ as incd, c.dev_q FROM {claims_table} as c "
  sqlsubstr_3= " GROUP BY c.lob, c.uy"
  sqlsubstr_2 = f"WHERE c.lob='{lob}' AND ("  
  for el in lst:
    sqlsubstr_2 = sqlsubstr_2 + f"c.tradecode='{el}'"
    if lst.index(el) < len(lst) - 1:
      sqlsubstr_2 = sqlsubstr_2 + " OR "
    else:
      sqlsubstr_2 = sqlsubstr_2 + ")"
  return sqlsubstr_1 + sqlsubstr_2 + sqlsubstr_3


def sql_builder_premium_where_tradecodelist(lob, lst,premium_table='premium') -> str:
  sqlsubstr_1 = f"SELECT p.lob, p.tradecode, p.uy, SUM(p.gwp) as gwp_ FROM {premium_table} as p "
  sqlsubstr_3= " GROUP BY p.lob, p.uy"
  sqlsubstr_2 = f"WHERE p.lob='{lob}' AND ("  
  for el in lst:
    sqlsubstr_2 = sqlsubstr_2 + f"p.tradecode='{el}'"
    if lst.index(el) < len(lst) - 1:
      sqlsubstr_2 = sqlsubstr_2 + " OR "
    else:
      sqlsubstr_2 = sqlsubstr_2 + ")"
  return sqlsubstr_1 + sqlsubstr_2 + sqlsubstr_3


@decs.execute_sql('runquery')
def ultimates_by_lob_tradecodelist_uy_cursor(*_, lob, tradecodelist, claims_table='claims', premium_table='premium'):
  #TODO
  # START HERE
  
  # sqlstr will have no ? in it, so args must be empty
  # for the decorator to work
  # -> make all arguments named arguments
  # aggregate claims for lob - tradecodes - uy combos
  sqlsubstr = sql_builder_claims_where_tradecodelist(lob, tradecodelist,claims_table='claims')
  # aggregate premium for lob - tradecodes - uy combos
  sqlsubstrpremium = sql_builder_premium_where_tradecodelist(lob, tradecodelist, premium_table='premium')
  # link aggregated claims with aggregated premium and add ieulrs and ftu
  sqlstr = f"""SELECT p.lob, p.uy, sub.dev_q, p.gwp_, sub.incd, i.ieulr, pat.FTU, sub.incd*pat.ftu as bcl, sub.incd + ((1-(1/pat.ftu)) * i.ieulr * p.gwp_) as bf, ex.expense, (sub.incd + ((1-(1/pat.ftu)) * i.ieulr * p.gwp_))/p.gwp_ + ex.expense as cor 
  FROM ({sqlsubstrpremium}) as p
  INNER JOIN ({sqlsubstr}) as sub ON p.lob=sub.lob AND p.uy=sub.uy
  INNER JOIN ieulrs as i ON sub.uy = i.uy AND sub.lob = i.lob 
  INNER JOIN patterns as pat ON sub.dev_q = pat.dev and sub.lob = pat.lob
  LEFT JOIN expenses as ex ON sub.lob = ex.lob AND sub.uy = ex.uy
  """
  return sqlstr


def ultimates_by_lob_multiple_tradecodes_uy(lob, tradecodelist, claims_table='claims', premium_table='premium') -> pd.DataFrame:
  result = ultimates_by_lob_tradecodelist_uy_cursor(lob=lob, tradecodelist=tradecodelist, claims_table=claims_table, premium_table=premium_table)
  #result is a list of tuples
  columns = ['lob', 'uy', 'dev', 'gwp', 'incd', 'ieulr', 'ftu', 'bcl', 'bf', 'exp', 'cor']
  df_combos = pd.DataFrame(result)
  df_combos.columns = columns
  return df_combos


# functions for lob - tradecode combos
# ------------------------------------
@decs.execute_sql('runquery')
def ultimates_by_lob_tradecode_uy_cursor(*_, lob, tradecode, claims_table='claims', premium_table='premium'):
  # sqlstr will have no ? in it, so args must be empty
  # for the decorator to work
  # -> make all arguments named arguments
  # aggregate claims for lob - tradecode - uy combos
  sqlsubstr = f"""SELECT c.lob, c.tradecode, c.uy, c.paid, c.case_, c.paid+c.case_ as incd, c.dev_q FROM {claims_table} as c 
  WHERE c.lob='{lob}' AND c.tradecode='{tradecode}'  
  GROUP BY c.lob, c.tradecode, c.uy 
  """
  # aggregate premium for lob - tradecode - uy combos
  sqlsubstrpremium = f"""SELECT p.lob, p.tradecode, p.uy, SUM(p.gwp) as gwp_ FROM {premium_table} as p 
  WHERE p.lob='{lob}' AND p.tradecode='{tradecode}'
  GROUP BY p.lob, p.tradecode, p.uy
  """
  # link aggregated claims with aggregated premium and add ieulrs and ftu
  sqlstr = f"""SELECT p.lob, p.tradecode, p.uy, sub.dev_q, p.gwp_, sub.incd, i.ieulr, pat.FTU, sub.incd*pat.ftu as bcl, sub.incd + ((1-(1/pat.ftu)) * i.ieulr * p.gwp_) as bf, ex.expense, (sub.incd + ((1-(1/pat.ftu)) * i.ieulr * p.gwp_))/p.gwp_ + ex.expense as cor 
  FROM ({sqlsubstrpremium}) as p
  INNER JOIN ({sqlsubstr}) as sub ON p.tradecode=sub.tradecode AND p.lob=sub.lob AND p.uy=sub.uy
  INNER JOIN ieulrs as i ON sub.uy = i.uy AND sub.lob = i.lob 
  INNER JOIN patterns as pat ON sub.dev_q = pat.dev and sub.lob = pat.lob
  LEFT JOIN expenses as ex ON sub.lob = ex.lob AND sub.uy = ex.uy
  """
  return sqlstr


def ultimates_by_lob_tradecode_uy(lob, tradecode, claims_table='claims', premium_table='premium') -> pd.DataFrame:
  result = ultimates_by_lob_tradecode_uy_cursor(lob=lob, tradecode=tradecode, claims_table=claims_table, premium_table=premium_table)
  #result is a list of tuples
  columns = ['lob', 'tradecode', 'uy', 'dev', 'gwp', 'incd', 'ieulr', 'ftu', 'bcl', 'bf', 'exp', 'cor']
  df_combos = pd.DataFrame(result)
  df_combos.columns = columns
  return df_combos


@decs.execute_sql('runquery')
def tradecodes_used_cursor(*_, lob, premium_threshold=0, claims_table="claims", premium_table="premium"):
  # pick up all tradecodes from the claim table
  # if the premium threshold is exceeded, add tradecode to output list
  sqlsubstr = f"""SELECT c.lob, c.tradecode FROM {claims_table} as c 
  WHERE c.lob='{lob}'  
  GROUP BY c.tradecode 
  """
  sqlsubstrpremium = f"""SELECT p.lob, p.tradecode, SUM(p.gwp) as gwp_ FROM {premium_table} as p 
  WHERE p.lob='{lob}'
  GROUP BY p.tradecode
  """
  sqlstr = f"""SELECT p.lob, p.tradecode, p.gwp_ FROM ({sqlsubstrpremium}) as p
  INNER JOIN ({sqlsubstr}) as sub ON p.tradecode=sub.tradecode AND p.lob=sub.lob
  WHERE p.gwp_ > {premium_threshold}  
  """
  return sqlstr

# functions for used_tradecodes
#------------------------------
def tradecodes_used(lob, premium_threshold=0, claims_table="claims", premium_table="premium") -> None:
  clean_table(tablename='used_tradecodes')
  result = tradecodes_used_cursor(lob=lob, premium_threshold=premium_threshold, claims_table=claims_table, premium_table=premium_table)
  #result = list of tuples (tradecode, sum(gwp))
  #this is run at the start of the process
  #we don't want this as a subquery that will rerun at every permutation
  for el in result:
    # has been deorated with execute_sql('write')
    insert_record(
      el[0], 
      el[1], 
      el[2],
      tablename='used_tradecodes'
      )
  return None


@decs.execute_sql('runquery')
def get_FTU_cursor(*_, lob, dev):
  sqlstr = f"""SELECT FTU 
  FROM patterns
  WHERE lob = '{lob}' and dev = {dev} 
  """
  return sqlstr


def get_FTU(lob, dev) -> float:
  results = get_FTU_cursor(lob=lob, dev=dev)
  return float(results[0][0])


@decs.execute_sql('runquery')
def get_ieulr_cursor(*_, lob, uy):
  sqlstr = f"""SELECT ieulr 
  FROM ieulrs
  WHERE lob='{lob}' AND uy={uy} 
  """
  return sqlstr


def get_ieulr(lob, uy):
  results=get_ieulr_cursor(lob=lob, uy=uy)
  return float(results[0][0])


if __name__ == "__main__":
  pass
  #print(get_ieulr("Onshore", 2010))
  #print(calc_BCL_for_all("Onshore"))
  #tradecodes_used('Onshore', premium_threshold=2000000)
  #print(ultimates_by_lob_tradecode_uy('Onshore','tr9'))
  #print(sql_builder_claims_where_tradecodelist("Onshore",
  #'tr1',
  #claims_table='claims'))
  #print(ultimates_by_lob_tradecodelist_uy_cursor(lob='Onshore', tradecodelist=['tr1','tr5','tr7'], claims_table='claims', premium_table='premium'))
  print(ultimates_by_lob_multiple_tradecodes_uy('Onshore',['tr1','tr6','tr9','tr5']))
  
  
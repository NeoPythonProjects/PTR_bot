import pandas as pd
from mysqldb import connect_to_db, get_cursor, get_query_results,insert_record_used_tradecodes


def load_lobs() -> list:
  conn = connect_to_db()
  cur = get_cursor(conn)
  cur.execute("SELECT lob FROM lobs")
  results = get_query_results(cur)
  conn.close()
  #results is a list of tuples [('Onshore',),('EA',)]
  #list comprehension to extract element from tuple
  return [x[0] for x in results]


def ultimates_by_lob_tradecode_uy(lob, tradecode, claims_table='claims', premium_table='premium') -> pd.DataFrame:
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
  conn = connect_to_db()
  cur = get_cursor(conn)
  print(sqlstr)
  cur.execute(sqlstr)
  result = get_query_results(cur)
  #result is a list of tuples
  columns = ['lob', 'tradecode', 'uy', 'dev', 'gwp', 'incd', 'ieulr', 'ftu', 'bcl', 'bf', 'exp', 'cor']
  df_combos = pd.DataFrame(result)
  df_combos.columns = columns
  # for each combo, calculate ultimates each uy
  #2. read list of tradecode combos
  return df_combos


def tradecodes_used(lob, premium_threshold=0, claims_table="claims", premium_table="premium") -> None:
  # pick up all tradecodes from the claim table
  # if the premium threshold is exceeded, add tradecode to output list
  #TODO returns a list of tradecodes that are being used in the current dataset
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
  conn = connect_to_db()
  cur = get_cursor(conn)
  cur.execute(sqlstr)
  result = cur.fetchall()
  #result = list of tuples (tradecode, sum(gwp))
  #todo: check premium totals add up
  #this is run at the start of the process
  #we don't want this as a subquery that will rerun at every permutation
  for el in result:
    insert_record_used_tradecodes(cur,el[0], el[1], el[2])
  conn.commit()
  conn.close()
  return None


def get_FTU(lob, dev) -> float:
  sqlstr = f"""SELECT FTU 
  FROM patterns
  WHERE lob = '{lob}' and dev = {dev} 
  """
  conn = connect_to_db()
  cur = get_cursor(conn)
  cur.execute(sqlstr)
  results = get_query_results(cur)
  conn.close()
  return float(results[0][0])


def get_ieulr(lob, uy) -> float:
  sqlstr = f"""SELECT ieulr 
  FROM ieulrs
  WHERE lob='{lob}' AND uy={uy} 
  """
  conn = connect_to_db()
  cur = get_cursor(conn)
  cur.execute(sqlstr)
  results = get_query_results(cur)
  conn.close()
  return float(results[0][0])


if __name__ == "__main__":
  pass
  #print(get_ieulr("Onshore", 2010))
  #print(calc_BCL_for_all("Onshore"))
  #tradecodes_used('Onshore', premium_threshold=2000000)
  print(ultimates_by_lob_tradecode_uy('Onshore','tr9'))
  
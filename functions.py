from mysqldb import connect_to_db, get_cursor, get_query_results

def ultimates_by_tradecode(tradecode):
  pass
  #1. read list of all trde codes
  # for each tradecode, calculate ultimates by LOB for each uy
  #2. read list of tradecode combos

def tradecodes_used(lob, premium_threshold=0, claims_table="claims", premium_table="premium"):
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
  return result


def BCL() -> float:
  pass

def BF() -> float:
  pass  

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



def calc_BCL_for_all(lob):
  sqlstr = f""" SELECT cl.lob, cl.dev_q, p.FTU,cl.paid, cl.case_, cl.paid + cl.case_ as incd, (cl.paid + cl.case_)*p.FTU as BCL, i.ieulr 
  FROM claims as cl
  INNER JOIN patterns as p ON cl.dev_q = p.dev AND cl.lob=p.lob
  INNER JOIN ieulrs as i ON cl.uy=i.uy AND cl.lob=i.lob
  WHERE cl.lob = '{lob}'
  """
  conn = connect_to_db()
  cur = get_cursor(conn)
  cur.execute(sqlstr)
  results = get_query_results(cur)
  conn.close()
  return results
  

if __name__ == "__main__":
  #print(get_ieulr("Onshore", 2010))
  #print(calc_BCL_for_all("Onshore"))
  print(tradecodes_used('Onshore', premium_threshold=2000000))
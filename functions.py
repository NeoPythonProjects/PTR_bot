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

def ultimates_by_lob_tradecode(lob, tradecode, claims_table='claims', premium_table='premium') -> list:
  sqlsubstr = f"""SELECT c.lob, c.tradecode, c.uy, c.paid, c.case_, c.paid+c.case_ as incd, c.dev_q FROM {claims_table} as c 
  WHERE c.lob='{lob}' AND c.tradecode='{tradecode}'  
  GROUP BY c.lob, c.tradecode, c.uy 
  """
  sqlsubstrpremium = f"""SELECT p.lob, p.tradecode, p.uy, SUM(p.gwp) as gwp_ FROM {premium_table} as p 
  WHERE p.lob='{lob}' AND p.tradecode='{tradecode}'
  GROUP BY p.lob, p.tradecode, p.uy
  """
  sqlstr = f"""SELECT p.lob, p.tradecode, p.uy, sub.dev_q, p.gwp_, sub.incd, i.ieulr, pat.FTU FROM ({sqlsubstrpremium}) as p
  INNER JOIN ({sqlsubstr}) as sub ON p.tradecode=sub.tradecode AND p.lob=sub.lob AND p.uy=sub.uy
  INNER JOIN ieulrs as i ON sub.uy = i.uy AND sub.lob = i.lob 
  INNER JOIN patterns as pat ON sub.dev_q = pat.dev and sub.lob = pat.lob
  """
  #todo: there are multiple dev per uy
  # you need to calculate the BCL ultimates at claim level
  # then calculate BF at claim level with uy-level ulr, but claim level dev

  conn = connect_to_db()
  cur = get_cursor(conn)
  print(sqlstr)
  cur.execute(sqlstr)
  result = get_query_results(cur)
  #result is a list of tuples
  
  #convert to list of elements
  #[x[0] for x in results]
  #data = [x for x in result]
  #print(data)
  #columns = ['lob', 'tradecode', 'gwp']
  #df_combos = pd.DataFrame({columns:data})
  # for each combo, calculate ultimates each uy
  #2. read list of tradecode combos
  return result


def ultimates_by_lob_tradecodelist(lob, tradecodelist) -> list:
  pass  

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
  sqlstr = f""" SELECT cl.lob, cl.dev_q, cl.paid, cl.case_, cl.paid + cl.case_ as incd, p.FTU(cl.paid + cl.case_)*p.FTU as BCL, i.ieulr, incd + (1-1/p.FTU)*i.ieulr* 
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
  pass
  #print(get_ieulr("Onshore", 2010))
  #print(calc_BCL_for_all("Onshore"))
  #tradecodes_used('Onshore', premium_threshold=2000000)
  for el in ultimates_by_lob_tradecode('Onshore','tr9'):
    print(el)
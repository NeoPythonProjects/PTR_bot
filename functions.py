from mysqldb import connect_to_db, get_cursor, get_query_results

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
  print(calc_BCL_for_all("Onshore"))
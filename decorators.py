import functools
import sqlite3

def show_records(func):
  """ decorator that connects to db, creates cursor, executes cursor, prints query result line by line to shell and closes db connections

  func: returns a sql string where args are passed as ? as part of execute and kwargs are passed as f-string variables (cur.execute doesn't accept named arguments)
  """
  # keep introspection intact
  @functools.wraps(func)
  def wrapper_interact_with_db(*args, **kwargs):
    #connect to database
    db = 'files/db.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    # execute func()
    value = func(*args, **kwargs)
    result = cur.execute(value)
    # show records in shell
    for el in result:
      print(el)
    conn.close
    # return func()
    return value
  #return wrapper function (don't execute)
  return wrapper_interact_with_db


def insert_record(func):
  """ decorator that connects to db, creates cursor, executes cursor, commits to db and closes db connections

  func: returns a sql string where args are passed as ? as part of execute and kwargs are passed as f-string variables (cur.execute doesn't accept named arguments)
  """  
  @functools.wraps(func)
  def wrapper_insert_record(*args, **kwargs):
    # connect to database
    db = 'files/db.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    # execute function, returns sqlstr
    sqlstr = func(*args, **kwargs)
    # args is a tuple of positional arguments
    # there is no need to unpack as execute needs a tuple as second argument
    # i'd have preferred to use kwargs so that position of arguments
    # in the calling procedure doesn't matter, but execute doesn't
    # accept keyword arguments
    cur.execute(sqlstr, args)
    conn.commit()
    conn.close
    # return func
    return func
  # return wrapper
  return wrapper_insert_record


def interact_with_db(action):
  """ decorator takes 1 'action' argument.
  It connects to db, creates cursor, executes cursor, commits to db if required and closes db connection.

  decorator combines the show_records and insert_record decorators

  argument: action can be 'read' or 'write'
  'read' lists the query results line by line to shell
  'write' writes to database
  
  func: returns a sql string where args are passed as ? as part of execute and kwargs are passed as f-string variables (cur.execute doesn't accept named arguments)
  """ 
  #combining the 2 decorators into one adding an argument to decide on which action to take 
  # decorator takes action as argument
  # needs an extra level that accepts the argument and return a decorator
  #action can be (i) write or (ii) read
  def decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
      #connect to database
      db = 'files/db.db'
      conn = sqlite3.connect(db)
      cur = conn.cursor()
      #execute func
      sqlstr= func(*args, **kwargs)
      result = cur.execute(sqlstr, args)
      if action == "write":
        conn.commit()
      elif action == "read":
        for el in result:
          print(el)    
      conn.close()
      #return frunction object
      return sqlstr
    return wrapper
  return decorator


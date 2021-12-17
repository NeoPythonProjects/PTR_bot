import functools
import sqlite3

def show_records(func):
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
  @functools.wraps(func)
  def wrapper_insert_record(*args, **kwargs):
    #connect to database
    db = 'files/db.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    #execute function, gives sqlstr
    sqlstr = func(*args, **kwargs)
    cur.execute(sqlstr, args)
    conn.commit()
    conn.close
    #return function
    return func
  # return wrapper
  return wrapper_insert_record


def save_to_db(func):
  @functools.wraps(func)
  def wrapper_save_to_db(*args, **kwargs):
    # unpack kwargs and find tablename and cur
    for k,v in kwargs.items():
      if v == 'cur': cur = k
      if v == 'tablename': tablename = k
    db = 'files/db.db'
    conn = sqlite3.connect(db)
    cur = conn.cursor() # can't pass cur to func()
    #clean table
    sqlstr = f"DELETE FROM {tablename}"
    value = func(*args, **kwargs)
    conn.commit()
    conn.close()
    # return func()
    return value
  #return wrapper
  return wrapper_save_to_db


def interact_with_db(action):
  #action can be (i) show_records, upload_csv
  pass
  



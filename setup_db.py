import sqlite3
db = sqlite3.connect('banshi.db')
with open('schema.sql') as f:
    db.executescript(f.read())
db.commit()
db.close()
print('schema 建立完成')

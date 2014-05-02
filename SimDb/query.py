#!/usr/bin/python
from database import db
print db.cleanTable('person')
dic = {'name': 'maybe', 'address': 'Shanxi', 'sex': 'female'}
if db: print db.insertDict('person', dic)

dic = {'name': 'lixue', 'address': 'Shanxi', 'sex': 'female'}
if db: print db.insertDict('person', dic)

dic = {'name': 'quqinglei', 'address': 'Shandong', 'sex': 'male'}

print 'fetchOneDict ', db.fetchOneDict("select name from person where name = 'maybe'")
print 'fetchAllDict ', db.fetchAllDict("select * from person")

print 'fetchOne ', db.fetchOne("select * from person")
print 'fetchAll ', db.fetchAll("select name, address, sex from person")

print 'simpFetchOneDict ', db.simpFetchOneDict('person', ['name', 'address', 'sex'], {'name': 'quqinglei'})
print 'simpFetchOneDict ', db.simpFetchOneDict('person', ['name', 'address', 'sex'], {'sex': 'female'})

print 'simpFetchAllDict ', db.simpFetchAllDict('person', ['name', 'address', 'sex'], {'sex': 'female'})


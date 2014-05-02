#!/usr/bin/python

from database import db
if db: print db.cleanTable('person')
dic = {'name': 'maybe', 'address': 'Shanxi', 'sex': 'female'}
if db: print db.insertDict('person', dic)

dic = {'name': 'lixue', 'address': 'Shanxi', 'sex': 'female'}
if db: print db.insertDict('person', dic)

ret = db.fetchAllDict("select * from person")
if ret['count'] == 2: print "Result: Test ok"
else: print "Result: Test Failure"
db.close()

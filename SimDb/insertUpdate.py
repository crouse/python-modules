#!/usr/bin/python
from database import db

if db: print db.cleanTable('person')

dic = {'name': 'maybe', 'address': 'Shanxi', 'sex': 'female'}
if db: print db.insertDict('person', dic)

dic = {'name': 'lixue', 'address': 'Shanxi', 'sex': 'female'}
if db: print db.insertDict('person', dic)

dic = {'name': 'maybe', 'address': 'ShanDong', 'sex': 'male'}
if db: print 'insertUpdateDict: ', db.insertUpdateDict('person', 'name', dic)

dic = {'name': 'quqinglei', 'address': 'ShanDong', 'sex': 'male'}
if db: print 'insertUpdateDict: ', db.insertUpdateDict('person', 'name', dic)

if db: print 'fetchAllDict', db.fetchAllDict("select * from person")
if db: print 'countRows ', db.countRows('select * from person')

if db: print 'deleteRowDict ', db.deleteRowDict('person', {'name': 'quqinglei'})

if db: db.close()

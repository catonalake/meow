__author__ = 'cathleen'

import random
import sys
import os
import datetime
import MySQLdb

def init_db():
    print ("init_db()")
    db = MySQLdb.connect(host="localhost",  #find out how to use pythonDBconnection
				    user="root",
				    passwd="Success",
				    db="voting_db")
    return db

def create_table(d):
    print ("create_table(d)")
# init vars....
    db = d
    cur = db.cursor()

    sql = """CREATE TABLE IF NOT EXISTS voter_table (
					 voter_info_id  INT PRIMARY KEY AUTO_INCREMENT ,
					 ri_voter_id  VARCHAR(25) NOT NULL,
					 status_code  VARCHAR(50) NULL DEFAULT ' ',
					 last_name  VARCHAR(50) NULL DEFAULT ' ',
					 first_name  VARCHAR(50) NULL DEFAULT ' ',
					 middle_name  VARCHAR(50) NULL DEFAULT ' ',
					 prefix  VARCHAR(50) NULL DEFAULT ' ',
					 suffix  VARCHAR(20) NULL DEFAULT ' ',
					 street_number  VARCHAR(20) NULL DEFAULT ' ',
					 street_name  VARCHAR(50) NULL DEFAULT ' ',
					 street_name_2  VARCHAR(50) NULL DEFAULT ' ',
					 zip_code  VARCHAR(5) NULL DEFAULT ' ',
					 zip4_code  VARCHAR(4) NULL DEFAULT ' ',
					 city  VARCHAR(50) NULL DEFAULT ' ',
					 unit  VARCHAR(10) NULL DEFAULT ' ',
					 suffix_a  VARCHAR(20) NULL DEFAULT ' ',
					 suffix_b  VARCHAR(20) NULL DEFAULT ' ',
					 state  VARCHAR(50) NULL DEFAULT ' ',
					 carrier_code  VARCHAR(50) NULL DEFAULT ' ',
					 postal_city  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_street_number  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_street_name_1  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_street_name_2  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_zip_code  VARCHAR(5) NULL DEFAULT ' ',
					 mailing_city  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_unit  VARCHAR(20) NULL DEFAULT ' ',
					 mailing_suffix_a  VARCHAR(20) NULL DEFAULT ' ',
					 mailing_suffix_b  VARCHAR(20) NULL DEFAULT ' ',
					 mailing_state  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_country  VARCHAR(50) NULL DEFAULT ' ',
					 mailing_carrier_code  VARCHAR(20) NULL DEFAULT ' ',
					 party_code  VARCHAR(20) NULL DEFAULT ' ',
					 special_status_code  VARCHAR(20) NULL DEFAULT ' ',
					 date_effective  DATE NULL,
					 date_of_privilege  DATE NULL ,
					 sex  VARCHAR(5) NULL DEFAULT ' ',
					 date_accepted  DATE,
					 date_of_status_change  DATE,
					 date_of_birth  DATE,
					 off_reason_code  VARCHAR(20) NULL DEFAULT ' ',
					 date_last_active  DATE NULL,
					 congressional_district  VARCHAR(20) NULL DEFAULT ' ',
					 state_senate_district  VARCHAR(20) NULL DEFAULT ' ',
					 state_rep_district  VARCHAR(20) NULL DEFAULT ' ',
					 precinct  VARCHAR(20) NULL DEFAULT ' ',
					 ward_council  VARCHAR(20) NULL DEFAULT ' ',
					 ward_district  VARCHAR(20) NULL DEFAULT ' ',
					 school_committee_district  VARCHAR(20) NULL DEFAULT ' ',
					 special_district  VARCHAR(20) NULL DEFAULT ' ',
					 fire_district  VARCHAR(20) NULL DEFAULT ' ',
					 phone_number  VARCHAR(20) NULL DEFAULT ' ',
					 email  VARCHAR(75) NULL DEFAULT ' '
                 )"""

#    sql = "CREATE TABLE temptable (testname VARCHAR2(50) NOT NULL , testaddr VARCHAR2(300) NOT NULL)"

    print (sql)

    cur.execute(sql)
    return cur

def insert_rows(d, c, f):
    global debugFlag
    global outputErrorFile
    global staticColNames

    def print_fieldValues(l, fv, sc):
        print ('print_fieldvalues (l, fv, sc)')
        global outputErrorFile
        for x in range(0,l):
            print (sc[x] + " = " + fv[x])
            print (sc[x] + " = " + fv[x], file=outputErrorFile)
        return

    if debugFlag:  print ("insert_rows(d, c, f)")
   

    cur = c
    db = d
    fieldValues = f
    numFieldValues = len(fieldValues)
    if debugFlag: 
        print ("length of fieldValues is ", numFieldValues)
        print (fieldValues)

    if numFieldValues < 51: 
        print (fieldValues, file=outputErrorFile)
        return

    sql = """INSERT INTO voter_history(
                ri_voter_id,
                last_name,
                first_name,
                middle_name,
                suffix,
                date_1,
                election_1,
                type_1,
                precinct_1,
                party_1,
                date_2,
                election_2,
                type_2,
                precinct_2,
                party_2,
                date_3,
                election_3,
                type_3,
                precinct_3,
                party_3,
                date_4,
                election_4,
                type_4,
                precinct_4,
                party_4,
                date_5,
                election_5,
                type_5,
                precinct_5,
                party_5,
                date_6,
                election_6,
                type_6,
                precinct_6,
                party_6,
                date_7,
                election_7,
                type_7,
                precinct_7,
                party_7,
                date_8,
                election_8,
                type_8,
                precinct_8,
                party_8,
                current_party,
                dob,
                street_number,
                suffix_a,
                suffix_b,
                street_name,
                street_name_2,
                unit,
                city,
                postal_city,
                state,
                zip_code,
                zip_code_4,
                precinct,
                status)
		    VALUES (    %s, %s , %s , %s , %s , %s , %s , %s , %s , %s , 
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s );"""

    if debugFlag: 
        print (sql)
        print_fieldValues(numFieldValues, fieldValues, staticColNames)

    try:
            cur.execute(sql, fieldValues)
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e, file = outputErrorFile)
        print(e)
        print_fieldValues(len(fieldValues), fieldValues, staticColNames)
        return None

    return

def delete_rows(d, c, v):
# this routine is not ready for prime time...
    print ("delete_rows(d, c, v)")
    cur = c
    db = d
    value = v
    sql = """SELECT testid 
            FROM temptable
            WHERE testname = 'a name')"""

    # check out how to use prepare statement

    sql = """DELETE FROM temptable 
            WHERE testname IN (a
            testaddr)
		    VALUES (
		    'b name',
		    'b address')"""
    print (sql)
    cur.execute(sql)
    return

def insert_many_rows(d, c):
    print ("insert_many_rows(d, c)")
    cur = c
    db = d
    rowVals = []

    rowVals = (('A name', 'A address'),
        ('z name','z address'),
		('y name', 'y address'),
		('x name', 'x address'))
 
    sql = """INSERT INTO temptable (
            testname,
            testaddr)
		    VALUES ( %s, %s )"""

    print (sql)
    cur.executemany(sql, rowVals)
    return


def commit_changes(d):
    print ("commit_changes(d)")
    db = d
    db.commit()
    return

def fetch_rows(c):
    # THIS HAS NOT BEEN TESTED...  
    print('in fetch_rows(c)')
    from pprint import pprint
    cur = c
    sql = """SELECT * 
            FROM voter_history WHERE last_name = 'GRIFFETH'"""

    cur.execute(sql)
    print ('printing results from ', sql)
    pprint(cur.fetchall())
    return
    

def main():
    print ("in main function")

    db = init_db()    
    cur = create_table(db)
    inputFile = open("voterhistory.txt","r")
    lineCount = 0

    for line in inputFile:
        lineCount +=1
        fieldsToInsert = []
        line = line.strip('\n')
        line = line[:-1]
        fieldsToInsert = line.split('|')
        insert_rows(db, cur, fieldsToInsert)
        if (lineCount % 10000) == 0: print ("processing line number ", lineCount)


    commit_changes(db)
    # fetch_rows(cur)
    db.close()
    return

print ("starting program voterfiletesting")

staticColNames = ('ri_voter_id',
                'last_name',
                'first_name',
                'middle_name',
                'suffix',
                'date_1',
                'election_1',
                'type_1',
                'precinct_1',
                'party_1',
                'date_2',
                'election_2',
                'type_2',
                'precinct_2',
                'party_2',
                'date_3',
                'election_3',
                'type_3',
                'precinct_3',
                'party_3',
                'date_4',
                'election_4',
                'type_4',
                'precinct_4',
                'party_4',
                'date_5',
                'election_5',
                'type_5',
                'precinct_5',
                'party_5',
                'date_6',
                'election_6',
                'type_6',
                'precinct_6',
                'party_6',
                'date_7',
                'election_7',
                'type_7',
                'precinct_7',
                'party_7',
                'date_8',
                'election_8',
                'type_8',
                'precinct_8',
                'party_8',
                'current_party',
                'dob',
                'street_number',
                'suffix_a',
                'suffix_b',
                'street_name',
                'street_name_2',
                'unit',
                'city',
                'postal_city',
                'state',
                'zip_code',
                'zip_code_4',
                'precinct',
                'status')

staticValuePlaces = (    '%s', '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , 
                            '%s', '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' ,
                            '%s', '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' ,
                            '%s', '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' ,
                            '%s', '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' ,
                            '%s', '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , '%s' ,)
print ("length of fieldArray is ", len(staticColNames))
print ("length of valuesArray is ", len(staticValuePlaces))


outputErrorFile = open("voter history logfile.txt", "wt")
debugFlag = False


main()

print ("end program")

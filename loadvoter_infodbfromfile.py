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

def print_fieldValues(l, fv, fa):
    print ('print_fieldvalues (l, fv, fa)')
    global outputErrorFile
    for x in range(0,l):
        print (fa[x] + " = " + fv[x])
        print (fa[x] + " = " + fv[x], file=outputErrorFile)
        # end loop

    print('\n\n printing field values outside the loop - : ', fv)
    print('\n\n length of field values =  ' , l)
    return

def insert_rows(d, c, f):
    global debugFlag
    global outputErrorFile
    global fieldArray
    global print_fieldValues

    if debugFlag:  print ("insert_rows(d, c, f)")

    cur = c
    db = d
    fieldValues = f

    if len(fieldValues) < 51: 
        print (fieldValues, file=outputErrorFile)
        return

    sql = """INSERT INTO voter_info(ri_voter_id, status_code,
                last_name,
                first_name,
                middle_name,
                prefix,
                suffix,
                street_number,
                street_name,
                street_name_2,
                zip_code,
                zip4_code,
                city,
                unit,
                suffix_a,
                suffix_b,
                state,
                carrier_code,
                postal_city,
                mailing_street_number,
                mailing_street_name_1,
                mailing_street_name_2,
                mailing_zip_code,
                mailing_city,
                mailing_unit,
                mailing_suffix_a,
                mailing_suffix_b,
                mailing_state,
                mailing_country,
                mailing_carrier_code,
                party_code,
                special_status_code,
                date_effective,
                date_of_privilege,
                sex,
                date_accepted,
                date_of_status_change,
                date_of_birth,
                off_reason_code,
                date_last_active,
                congressional_district,
                state_senate_district,
                state_rep_district,
                precinct,
                ward_council,
                ward_district,
                school_committee_district,
                special_district,
                fire_district,
                phone_number,
                email)
		    VALUES (    %s, %s , %s , %s , %s , %s , %s , %s , %s , %s , 
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s ,
                        %s, %s , %s , %s , %s , %s , %s , %s , %s , %s , %s );"""

    if debugFlag: print (sql)

    try:
            cur.execute(sql, fieldValues)
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print(e, file = outputErrorFile)
        print(e)
        print_fieldValues(len(fieldValues), fieldValues, fieldArray)
        return None

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
            FROM voter_info WHERE last_name = 'GRIFFETH'"""

    cur.execute(sql)
    print ('printing results from ', sql)
    pprint(cur.fetchall())
    return
    

def main():
    print ("in main function")

    db = init_db()    
    cur = create_table(db)
    inputFile = open("voter.txt","r")
    lineCount = 0

    for line in inputFile:
        lineCount +=1
        fieldsToInsert = []
        line = line.strip('\n')
        line = line[:-1]
        fieldsToInsert = line.split('|')
        insert_rows(db, cur, fieldsToInsert)
        if (lineCount % 10000) == 0: print ("processing line number ", lineCount)
        # end for line loop

    commit_changes(db)
    # fetch_rows(cur)
    db.close()    # close for a happy db :)
    return

print ("starting program voterfiletesting")

fieldArray = ('ri_voter_id',
                    'status_code',
                    'last_name',
                    'first_name',
                    'middle_name',
                    'prefix',
                    'suffix',
                    'street_number',
                    'street_name',
                    'street_name_2',
                    'zip_code',
                    'zip4_code',
                    'city',
                    'unit',
                    'suffix_a',
                    'suffix_b',
                    'state',
                    'carrier_code',
                    'postal_city',
                    'mailing_street_number',
                    'mailing_street_name_1',
                    'mailing_street_name_2',
                    'mailing_zip_code',
                    'mailing_city',
                    'mailing_unit',
                    'mailing_suffix_a',
                    'mailing_suffix_b',
                    'mailing_state',
                    'mailing_country',
                    'mailing_carrier_code',
                    'party_code',
                    'special_status_code',
                    'date_effective',
                    'date_of_privilege',
                    'sex',
                    'date_accepted',
                    'date_of_status_change',
                    'date_of_birth',
                    'off_reason_code',
                    'date_last_active',
                    'congressional_district',
                    'state_senate_district',
                    'state_rep_district',
                    'precinct',
                    'ward/council',
                    'ward_district',
                    'school_committee_district',
                    'special_district',
                    'fire_district',
                    'phone_number',
                    'email')

print ("length of fieldArray is ", len(fieldArray))

VIErrorRecords = open("vierrorogfile.txt", "wt")

outputErrorFile = open("logfile.txt", "wt")
debugFlag = False


main()

print ("end program")

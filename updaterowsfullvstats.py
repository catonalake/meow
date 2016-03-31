__author__ = 'cathleen'
# this program will load the voter history table
import sys
import os
import datetime
import MySQLdb
import geocoder

def init_db():
    print ("init_db()")
    db = MySQLdb.connect(host="localhost",  #find out how to use pythonDBconnection
				    user="root",
				    passwd="Success",
				    db="newvoterproject")
    return db

def create_cursor(d):
    print ("create_cursor(d)")

# init vars....
    db = d
    cur = db.cursor()
    global fieldArray 
    global valuesArray
    global len_fieldArray

    sql = """CREATE TABLE IF NOT EXISTS fullvh (
                voter_id VARCHAR(25) PRIMARY KEY NOT NULL DEFAULT ' ',
                last_name VARCHAR(50) NULL DEFAULT ' ',
                first_name VARCHAR(50) NULL DEFAULT ' ',
                middle_name VARCHAR(50) NULL DEFAULT ' ',
                suffix VARCHAR(50) NULL DEFAULT ' ',
                date_1 VARCHAR(50) NULL DEFAULT ' ',
                election_1 VARCHAR(50) NULL DEFAULT ' ',
                type_1 VARCHAR(50) NULL DEFAULT ' ',
                precinct_1 VARCHAR(50) NULL DEFAULT ' ',
                party_1 VARCHAR(50) NULL DEFAULT ' ',
                date_2 VARCHAR(50) NULL DEFAULT ' ',
                election_2 VARCHAR(50) NULL DEFAULT ' ',
                type_2 VARCHAR(50) NULL DEFAULT ' ',
                precinct_2 VARCHAR(50) NULL DEFAULT ' ',
                party_2 VARCHAR(50) NULL DEFAULT ' ',
                date_3 VARCHAR(50) NULL DEFAULT ' ',
                election_3 VARCHAR(50) NULL DEFAULT ' ',
                type_3 VARCHAR(50) NULL DEFAULT ' ',
                precinct_3 VARCHAR(50) NULL DEFAULT ' ',
                party_3 VARCHAR(50) NULL DEFAULT ' ',
                date_4 VARCHAR(50) NULL DEFAULT ' ',
                election_4 VARCHAR(50) NULL DEFAULT ' ',
                type_4 VARCHAR(50) NULL DEFAULT ' ',
                precinct_4 VARCHAR(50) NULL DEFAULT ' ',
                party_4 VARCHAR(50) NULL DEFAULT ' ',
                date_5 VARCHAR(50) NULL DEFAULT ' ',
                election_5 VARCHAR(50) NULL DEFAULT ' ',
                type_5 VARCHAR(50) NULL DEFAULT ' ',
                precinct_5 VARCHAR(50) NULL DEFAULT ' ',
                party_5 VARCHAR(50) NULL DEFAULT ' ',
                date_6 VARCHAR(50) NULL DEFAULT ' ',
                election_6 VARCHAR(50) NULL DEFAULT ' ',
                type_6 VARCHAR(50) NULL DEFAULT ' ',
                precinct_6 VARCHAR(50) NULL DEFAULT ' ',
                party_6 VARCHAR(50) NULL DEFAULT ' ',
                date_7 VARCHAR(50) NULL DEFAULT ' ',
                election_7 VARCHAR(50) NULL DEFAULT ' ',
                type_7 VARCHAR(50) NULL DEFAULT ' ',
                precinct_7 VARCHAR(50) NULL DEFAULT ' ',
                party_7 VARCHAR(50) NULL DEFAULT ' ',
                date_8 VARCHAR(50) NULL DEFAULT ' ',
                election_8 VARCHAR(50) NULL DEFAULT ' ',
                type_8 VARCHAR(50) NULL DEFAULT ' ',
                precinct_8 VARCHAR(50) NULL DEFAULT ' ',
                party_8 VARCHAR(50) NULL DEFAULT ' ',
                current_party VARCHAR(50) NULL DEFAULT ' ',
                dob VARCHAR(50) NULL DEFAULT ' ',
                street_number VARCHAR(50) NULL DEFAULT ' ',
                suffix_a VARCHAR(50) NULL DEFAULT ' ',
                suffix_b VARCHAR(50) NULL DEFAULT ' ',
                street_name VARCHAR(50) NULL DEFAULT ' ',
                street_name_2 VARCHAR(50) NULL DEFAULT ' ',
                unit VARCHAR(50) NULL DEFAULT ' ',
                city VARCHAR(50) NULL DEFAULT ' ',
                postal_city VARCHAR(50) NULL DEFAULT ' ',
                state VARCHAR(50) NULL DEFAULT ' ',
                zip_code VARCHAR(20) NULL DEFAULT ' ',
                zip_code_4 VARCHAR(20) NULL DEFAULT ' ',
                precinct VARCHAR(50) NULL DEFAULT ' ',
                status VARCHAR(50) NULL DEFAULT ' '
            )"""
    #print (sql)
    #cur.execute(sql)
    return cur

def update_rows(d, c, f):
    global debugFlag
    global outputErrorFile
    global len_fieldArray
    global nolatlng 

#    if debugFlag:  print ("update_rows(d, c, f)")
    cur = c
    db = d
    fieldValues = f

    if len(fieldValues) != len_fieldArray: 
        print ("length of fieldValues is ", len(fieldValues))
        print ("len_fieldArray is ", len_fieldArray)
        for i in fieldValues:
            print (i)
            print (i, file=outputErrorFile)
        print ("length of fieldValues is ", len(fieldValues), file=outputErrorFile)
        print (fieldValues, file=outputErrorFile)
        return

    sql = """UPDATE fullvstats vs
                    SET vs.lat=%s,
                        vs.lng=%s  
                    WHERE vs.vstat_id  = %s;"""
#    if debugFlag: print (sql)
    try:
            cur.execute(sql, fieldValues)
    except (MySQLdb.Error, MySQLdb.Warning) as e:
        print (e, end='')
        print(e, file = outputErrorFile)
        print(fieldValues, file = outputErrorFile) # print the fields that in error
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
            FROM fullvstats WHERE street_name = 'ALDRICH RD';"""

    cur.execute(sql)
    print ('printing results from ', sql)
    pprint(cur.fetchall())
    return
    

def init_global_variables():
    global fieldArray, valuesArray, len_fieldArray  
    global sql_stmt


    # use this to set up the cursoer
    # then fetch each row and process 
    # - calling for the lat/lng
    # - setting up the fields for the insert
    sql_stmt = """SELECT vs.vstat_id, 
        vs.street_number,
        vs.street_name,
        vs.city,
        vs.postal_city,
        vs.lat,
        vs.lng
    FROM
        newvoterproject.fullvstats vs
    WHERE 
        (vs.lat  = 0) 
        AND vs.city = 'OLD FERRY RD' 
        AND (vs.street_number = '1' or vs.street_number = '5') ;
    """
    # todo: set these to fullvstats 
    # - remember to alter the table to include all fields above
    # - and remember to add lat and lng

    fieldArray = ('vstat_id',
        'lat',
        'lng')
    valuesArray = ( '%s', '%s', '%s' )
        
    len_fieldArray = len(fieldArray)
    return

def main():
    print ("in main function")
    g = ''

    init_global_variables()
    db_select = init_db()    
    db_update = init_db()    
    cur_select = create_cursor(db_select)
    cur_insert = create_cursor(db_update)

    #todo: set the cursor here?
    #inputFile = open('C:\\Users\\cathleen\\2015 Voter File\\voterhistory.txt',"r")

    # stuff to finish
    cur_select.execute(sql_stmt)
    row = cur_select.fetchone()
    lineCount = 0
    nolatlng = 0
    while row is not None:
        lineCount +=1
        fieldsToUpdate = []
        gaddr = ''
        gaddr = row[1] + " " + row[2] + " "  + row[4] + " RI" 
        g = geocoder.bing(gaddr, key='AhZjbRIwW3C8eSJ0j05OlZMqWQjhsnpC3X-q9eUY4TqNjIR4RhxgG2oXL7qA3LXu')
        #g = geocoder.google(gaddr)
        # only set the fields for the vstat id, the lat and the lng
        # after processing each column and each c is set up for the insert 
        # - push (append)  g.latlng[0] as lat to fieldsToUpdate
        # - push (append)  g.latlng[1] as lng to fieldsToUpdate
        if (len(g.latlng) == 0):
            # what is the latlng for the center of ri??
            fieldsToUpdate.append(0.0) 
            fieldsToUpdate.append(0.0)
            nolatlng +=1
            e = 'had an error with ' + gaddr
            print(e, file = outputErrorFile)
            print(e)
        else:
            fieldsToUpdate = [g.latlng[0], g.latlng[1], row[0] ]
            update_rows(db_update, cur_insert, fieldsToUpdate)
        # print(row)
        # and get the next row
        row = cur_select.fetchone()
        if ((lineCount % 100) == 0): 
            print ("processing line number ", lineCount)
            commit_changes(db_update)
            #think about...???? do a commit after each insert? instsead of once at the end....

    commit_changes(db_update)
    #fetch_rows(cur)
    print('number of addresses with no lat lng = ', nolatlng , file = outputErrorFile)
    print('number of addresses with no lat lng = ', nolatlng)
    print('record count = ', lineCount , file = outputErrorFile)
    print('record count = ', lineCount)

    db_update.close()
    return

print ("starting program updaterowsfullvstats.py")

outputErrorFile = open("updaterowsfullvstats.py.errorlog.txt", "wt")
debugFlag = False

main()

print ("end program")




# Showing you, Ways to fetch Data from the DashDB
import ibm_db

connection = ibm_db.connect('DATABASE=dbname;uid=;pwd=password;hostname=hostname;port=50000;')

fetch_query = 'SELECT * FROM USERTABLE'

statement = ibm_db.exec_immediate(connection,fetch_query)

dictionary = ibm_db.fetch_both(statement)

while dictionary != False:
	print "The EMAILID is :",dictionary["EMAILID"]
	print "The USERNAME is :",dictionary["USERNAME"]
	dictionary = ibm_db.fetch_both(statement)


Tuple = ibm_db.fetch_tuple(statement)
while Tuple != False:
	
	print "The EMAILID is :",Tuple[0]
	print "The USERNAME is :",Tuple[1]
	dictionary = ibm_db.fetch_tuple(statement)


dictionary = ibm_db.fetch_assoc(statement)
while dictionary != False:
	print "The EMAILID is :",dictionary["EMAILID"]
	print "The USERNAME is :",dictionary["USERNAME"]
	dictionary = ibm_db.fetch_assoc(statement)

while ibm_db.fetch_row(statement)!= False:
	print "The EMAILID is :",ibm_db.result(statement,0)
	print "The USERNAME is :",ibm_db.result(statement,"USERNAME")
	dictionary = ibm_db.fetch_assoc(statement)



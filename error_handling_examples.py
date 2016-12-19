# Sample code snippet for the error handling function 
# connection error, When you are trying to connect in awrong way this exception will be raised
import ibm_db
try:
	connection = ibm_db.connect('DATABASE=dbname;uid=;pwd=password;hostname=hostname;port=50000;')
except:
	print "Not connected : ",ibm_db.conn_errormsg()

# statement error,When you have given a wrong query format this exception will be raised
delete_query = "DELETE FROM T1"
try:
	statement = ibm_db.exec_immediate(connection,delete_query)
except:
	print "Invalid query : ",ibm_db.stmt_errormsg()

		


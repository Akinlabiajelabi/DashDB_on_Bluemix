# importing required modules
import os
import sys
# importing ibm Dashdb packages
import ibm_db
from ibm_db import connect
from ibm_db import active 

# importing logging module
# To log messages
import logging

LOG_FILENAME = "dashDB_DemoLogs.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s, %(levelname)s, %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


class DashDB:
	def __init__(self):
		self.connection=None
		self.userid = ""
		self.password = ""
		self.hostname = ""

	def dashDBInit(self):
		self.databaseConnectionInfo = {"Database name":"BLUDB","User ID":self.userid,"Password":self.password,"Host name":self.hostname,"Port number":"50000"}
		self.DatabaseSchema = 'DASH'+self.userid[4:]
		
		dbtry = 0
		while(dbtry <3):
			try:
				if 'VCAP_SERVICES' in os.environ:
				    hasVcap = True
				    import json
				    vcap_services = json.loads(os.environ['VCAP_SERVICES'])
				    if 'dashDB' in vcap_services:
				        hasdashDB = True
				        service = vcap_services['dashDB'][0]
				        credentials = service["credentials"]
				        url = 'DATABASE=%s;uid=%s;pwd=%s;hostname=%s;port=%s;' % ( credentials["db"],credentials["username"],credentials["password"],credentials["host"],credentials["port"])
				    else:
				        hasdashDB = False
				  
				else:
				    hasVcap = False
				    self.url = 'DATABASE=%s;uid=%s;pwd=%s;hostname=%s;port=%s;' % (self.databaseConnectionInfo["Database name"],self.databaseConnectionInfo["User ID"],self.databaseConnectionInfo["Password"],self.databaseConnectionInfo["Host name"],self.databaseConnectionInfo["Port number"])
	   
				self.connection = ibm_db.connect(self.url, '', '')
				if (active(self.connection)):
					print self.connection
					return self.connection
			except Exception as dberror:
				logging.error("dberror Exception %s"%(ibm_db.conn_errormsg()))
				logging.error("dberror Exception %s"%dberror)
				dbtry+=1
		return False
		
	def connectioncheck_handler(self):
		
		logging.info("connection is"+str(active(self.connection)))
		dbretry = 0
		if (active(self.connection) == False):
			while (dbretry<3):
				self.connection = ibm_db.connect(self.url ,'' ,'')
				if active(self.connection) == True:
					dbretry = 3
				else:
					if dbretry == 2:
						raise Exception("db retry Error")
					else:
						dbretry+=1
				
			logging.info("restarted connection is"+str(active(self.connection)))
		

	def dbCreate(self,col1,col2,col3,col4):
		self.connectioncheck_handler()


		create_query = "CREATE TABLE \""+tablename+"\"(\""+col1+"\" INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO MAXVALUE NO CYCLE NO CACHE ORDER),\""+col2+"\" VARCHAR(30) NOT NULL,\""+col3+"\" VARCHAR(30) NOT NULL,\""+col4+"\" VARCHAR(30) NOT NULL,\""+col5+"\" INT NOT NULL,\""+col6+"\" TIMESTAMP NOT NULL)ORGANIZE BY ROW;"
	
		statement = ibm_db.exec_immediate(self.connection, create_query)
		ibm_db.free_stmt(statement)	
			





	def dbInsert(self,tablename,emailid,password,username,dateofcreation):
		self.connectioncheck_handler()
		try:
			insert_query = "INSERT INTO "+self.DatabaseSchema+".\'"+str(tablename)+"\' VALUES (DEFAULT,\'"+str(emailid)+"\',\'"+str(password)+"\',\'"+str(username)+"\',\'"+str(dateofcreation)+"\')"
			statement = ibm_db.exec_immediate(self.connection, insert_query)
			ibm_db.free_stmt(statement)
			return True	
	
		except:
			logging.error("The dbInsert operation error is %s"%(ibm_db.stmt_errormsg()))

		
	def dbUpdate(self,tablename,columnName,updatevalue,conditionColumnName,conditionColumnValue):
		self.connectioncheck_handler()

		try:
			update_query = "UPDATE "+tablename+" SET "+columnName+" = \'"+str(updatevalue)+"\'  WHERE "+conditionColumnName+" = \'"+str(conditionColumnValue)+"\'"
		
			statement = ibm_db.exec_immediate(self.connection, update_query)
			ibm_db.free_stmt(statement)	
			return True
		except:
			logging.error("The dbUpdate operation error is %s"%(ibm_db.stmt_errormsg()))	
		
	def dbDelete(self,tablename,conditionColumnName,conditionColumnValue):
		self.connectioncheck_handler()
		try:
			delete_query = "DELETE * FROM "+self.DatabaseSchema+".\'"+str(tablename)+"\'WHERE "+conditionColumnName+" = \'"+str(conditionColumnValue)+"\'"
			statement = ibm_db.exec_immediate(self.connection, delete_query)
			ibm_db.free_stmt(statement)
			return True
		except:
			logging.error("The dbDelete operation error is %s"%(ibm_db.stmt_errormsg()))	
	def dbFetch(self,tablename):
		self.connectioncheck_handler()
		try:	
			fetch_query = "SELECT * FROM "+self.DatabaseSchema+".\'"+str(tablename)+"\'"						
			statement = ibm_db.exec_immediate(self.connection, fetch_query)
			dictionary = ibm_db.fetch_assoc(stmt)
			data = []
			while(dictionary!=False):
				data.append(dictionary)
				dictionary = ibm_db.fetch_assoc(stmt)
			ibm_db.free_stmt(statement)
			return data
		except:
			logging.error("The dbFetch operation error is %s"%(ibm_db.stmt_errormsg()))	

if __name__ == '__main__':
	db = Dashdb()

	while True:
		inpt = raw_input("Enter the value 1-Insert Data 2-Update Data 3-Fetch Data 4-Delete Data")

		if inpt == "1":
			tablename = "USERTABLE"
			emailid = raw_input("Enter the emailid:") 
			password = raw_input("Enter the password:")
			username = raw_input("Enter the username:")
			dateofcreation = str(datetime.datetime.now())
			db.dbInsert(tablename,emailid,password,username,dateofcreation)

		if inpt == "2":
			tablename = "USERTABLE"
			columnName = raw_input("Enter the columnname:")
			updatevalue = raw_input("Enter the updatevalue:")
			conditionColumnName = raw_input("Enter the condtioncolumnname:")
			conditionColumnValue = raw_input("Enter the condtioncolumnvalue:")
			db.dbUpdate(tablename,columnName,updatevalue,conditionColumnName,conditionColumnValue)
		if inpt == "3":
			tablename = "USERTABLE"
			data = db.dbFetch(tablename)
			print data

		if inpt == "4":
			tablename = "USERTABLE"
			conditionColumnName = raw_input("Enter the condtioncolumnname:")
			conditionColumnValue = raw_input("Enter the condtioncolumnvalue:")
			db.dbDelete(tablename,conditionColumnName,conditionColumnValue)
			
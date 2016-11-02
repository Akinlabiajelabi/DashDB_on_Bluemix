# importing required modules
import os
import sys
# importing ibm Dashdb packages
import ibm_db
from ibm_db import connect
from ibm_db import active 

import datetime

# importing logging module
# To log messages
import logging


LOG_FILENAME = "dashDB_DemoLogs.log"
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format='%(asctime)s, %(levelname)s, %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


class DashDB:
	def __init__(self):
		self.connection=None
		self.userid = "dash100629"
		self.password = "tQd0KuLmwUeC"
		self.hostname = "dashdb-entry-yp-dal09-09.services.dal.bluemix.net"

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
	   			print self.url
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
		try:
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
		except Exception as e:
			logging.error("The connectioncheck_handler error is %s"%(e))		

	def dbCreate(self,tablename,col1,col2,col3,col4,col5):
		self.connectioncheck_handler()

		try:	
			create_query = "CREATE TABLE "+tablename+" ("+col1+" INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO MAXVALUE NO CYCLE NO CACHE ORDER),"+col2+" VARCHAR(30) NOT NULL,"+col3+" VARCHAR(30) NOT NULL,"+col4+" VARCHAR(30) NOT NULL,"+col5+" TIMESTAMP NOT NULL, PRIMARY KEY("+col2+","+col3+"))ORGANIZE BY ROW;"
			statement = ibm_db.exec_immediate(self.connection, create_query)
			ibm_db.free_stmt(statement)	
			
		except Exception as e:
			logging.error("The dbCreate operation error is %s"%(e))
			return False
		except:
			logging.error("The dbCreate operation error is %s"%(ibm_db.stmt_errormsg()))
			return False
		return True
		

	def dbInsert(self,tablename,emailid,password,username,dateofcreation):
		self.connectioncheck_handler()
		try:
			insert_query = "INSERT INTO "+self.DatabaseSchema+"."+tablename+" VALUES (DEFAULT,\'"+emailid+"\',\'"+password+"\',\'"+username+"\',\'"+str(dateofcreation)+"\')"
			statement = ibm_db.exec_immediate(self.connection, insert_query)
			ibm_db.free_stmt(statement)
				
		except Exception as e:
			logging.error("The dbInsert operation error is %s"%(e))
			return False
		except:
			logging.error("The dbInsert operation error is %s"%(ibm_db.stmt_errormsg()))
			return False	
		return True
		
	def dbUpdate(self,tablename,columnName,updatevalue,conditionColumnName1,conditionColumnValue1,conditionColumnName2,conditionColumnValue2):
		self.connectioncheck_handler()

		try:
			update_query = "UPDATE "+tablename+" SET "+columnName+" = \'"+str(updatevalue)+"\'  WHERE "+conditionColumnName1+" = \'"+str(conditionColumnValue1)+"\' AND "+conditionColumnName2+" = \'"+str(conditionColumnValue2)+"\'"
		
			statement = ibm_db.exec_immediate(self.connection, update_query)
			ibm_db.free_stmt(statement)	
			
		except Exception as e:
			logging.error("The dbUpdate operation error is %s"%(e))
			return False
		except:
			logging.error("The dbUpdate operation error is %s"%(ibm_db.stmt_errormsg()))	
			return False
		return True	

	def dbDelete(self,tablename,conditionColumnName1,conditionColumnValue1,conditionColumnName2,conditionColumnValue2):
		self.connectioncheck_handler()
		try:
			delete_query = "DELETE FROM "+self.DatabaseSchema+"."+tablename+" WHERE "+conditionColumnName1+" = \'"+conditionColumnValue1+"\' AND "+conditionColumnName2+" = \'"+conditionColumnValue2+"\' "
			statement = ibm_db.exec_immediate(self.connection, delete_query)
			ibm_db.free_stmt(statement)

		except Exception as e:
			logging.error("The dbDelete operation error is %s"%(e))
			return False
		except:	
			logging.error("The dbDelete operation error is %s"%(ibm_db.stmt_errormsg()))	
			return False
		return True

	def dbFetch(self,tablename):
		self.connectioncheck_handler()
		try:	
			fetch_query = "SELECT * FROM "+self.DatabaseSchema+"."+tablename+""						
			statement = ibm_db.exec_immediate(self.connection, fetch_query)
			dictionary = ibm_db.fetch_assoc(statement)
			data = []
			while(dictionary!=False):
				data.append(dictionary)
				dictionary = ibm_db.fetch_assoc(statement)
			ibm_db.free_stmt(statement)
			
		except Exception as e:
			logging.error("The dbFetch operation error is %s"%(e))
			return False
		except:		
			logging.error("The dbFetch operation error is %s"%(ibm_db.stmt_errormsg()))	
			return False
		return data	
if __name__ == '__main__':
	db = DashDB()
	db.dashDBInit()

	while True:
		try:
			inpt = raw_input("\tEnter the value \t\n0-Create table \t\n1-Insert Data \t\n2-Update Data \t\n3-Fetch Data \t\n4-Delete Data\n\t\t")

			if inpt == "0":
				tablename = "USERTABLE"
				col1 = "ID"
				col2 = "EMAILID"
				col3 = "PASSWORD"
				col4 = "USERNAME"
				col5 = "DATEOFCREATION"
				create_retrn = db.dbCreate(tablename,col1,col2,col3,col4,col5)
				if create_retrn ==True:
					print "\t\t TABLE CREATED SUCCESSFULLY"
				else:
					print "\t\t TABLE CREATE FAILED"	
			if inpt == "1":
				tablename = "USERTABLE"
				emailid = raw_input("Enter the emailid:") 
				password = raw_input("Enter the password:")
				username = raw_input("Enter the username:")
				dateofcreation = str(datetime.datetime.now())
				insert_return = db.dbInsert(tablename,emailid,password,username,dateofcreation)
				if insert_return == True:
					print "\t\tDATA INSERTED SUCCESSFULLY"
				else:
					print "\t\tDATA INSERT FAILED"

			if inpt == "2":
				tablename = "USERTABLE"
				print "\t\t YOU HAVE SELECTED TO UPDATE THE DATA IN THE DASHDB TABLE\n \t\t SELECT THE ANY ONE OF THE COLUMNS TO UPDTE FROM FOLLOWING"
				print "\t\t\t PASSWORD, USERNAME "
				
				columnName = raw_input("Enter the columnname to be updated :")
				updatevalue = raw_input("Enter the updatevalue :")
				conditionColumnName1 = "EMAILID"
				conditionColumnValue1 = raw_input("Enter the EMAILID:")
				conditionColumnName2 = "PASSWORD"
				conditionColumnValue2 = raw_input("Enter the PASSWORD:")

				update_return = db.dbUpdate(tablename,columnName,updatevalue,conditionColumnName1,conditionColumnValue1,conditionColumnName2,conditionColumnValue2)
				if update_return == True:
					print "\t\tDATA UPDATED SUCCESSFULLY"
				else:	
					print "\t\tDATA UPDATE FAILED"

			if inpt == "3":
				print "\n\t\t\t DATA FETCH OPERATION"
				tablename = "USERTABLE"
				data = db.dbFetch(tablename)
				if data != None:
					print "\t\t DATA FETCHED SUCCESSFULLY"
					print data
				else:
					print "\t\t DATA FETCH FAILED"	
			if inpt == "4":
				print "\n\t\t\t DATA DELETION OPERATION"
				
				tablename = "USERTABLE"
				conditionColumnName1 = "EMAILID"
				conditionColumnValue1 = raw_input("Enter the EMAILID:")
				conditionColumnName2 = "PASSWORD"
				conditionColumnValue2 = raw_input("Enter the PASSWORD:")
				

				del_return = db.dbDelete(tablename,conditionColumnName1,conditionColumnValue1,conditionColumnName2,conditionColumnValue2)
				print del_return
				if del_return == True:
					print "\t\t DATA DELETED SUCCESSFULLY"
				else:
					print "\t\t DATA DELETE FAILED"	
		except KeyboardInterrupt:
			print "\n\t\t\t oops!!! PRESSED ctr+c"
			sys.exit()			
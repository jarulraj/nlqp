
from __future__ import print_function

import cmd
import os
import psycopg2
import getpass
import sys
import traceback

## ============================================================================================
# # Command Interpreter
## ============================================================================================

# DB Connection
db_conn = None;    

dbs = [];
db_tables = [];


class query(cmd.Cmd):
    intro = """English to SQL translator.\nType \"help\" for help.\n"""
    
    ruler = '-'
    prompt = 'query=# '
                
    # Add shell support
    def do_shell(self, line):
        "Run a shell command"
        print ("Executing shell command :", line)
        output = os.popen(line).read()
        print (output)
        self.last_output = output
    
    # Terminate on quit or exit
    def do_q(self, line):
        "Quit console"
        return True
    
    def do_exit(self, line):
        "Quit console"
        return True
    
    def do_db(self, line):
        "Pick the database to analyze"
        print ("Looking up database : ", line)

        try:
            db = line
            c_user = raw_input('Enter username: ')
            c_password = getpass.getpass(prompt="Enter password : ")
            c_host = raw_input("Enter hostname : ") 
            c_port = raw_input("Enter port : ")
    
            if not c_host:
                c_host = 'localhost'
            if not c_port:
                c_port = '5432'
                
            c_user = "parallels";
            c_password = "parallels"
            
            global db_conn;            
            db_conn = psycopg2.connect(database=db, user=c_user, password=c_password, 
                                    host=c_host, port=c_port)
            
            print(db_conn)
            
        except Exception:
            print (traceback.format_exc())
            print ("Unable to perform operation. Please try again.")
            
                                
    def do_list_tables(self, line):
        "List the tables in the database"

        global db_conn;
        if db_conn == None:
            print ("Pick database first")        
        else:
            cursor = db_conn.cursor()            
            
            try:            
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

                global db_tables;
                rows = cursor.fetchall();                
                
                for row in rows:
                    db_tables.append(row[0])

                print (db_tables)
                
            except Exception:
                print (traceback.format_exc())
                print ("Unable to perform operation. Please try again.")
                db_conn.rollback()

            finally:
                if cursor is not None:
                    cursor.close()
            
    def do_list_columns(self, line):
        "List the columns in the table"        

        table = line;
        
        global db_conn;
        if db_conn == None:
            print ("Pick database first")        
        else:
            cursor = db_conn.cursor()            

            try:                        
                cursor.execute("SELECT * FROM " + table +" LIMIT 0")
                column_names = [desc[0] for desc in cursor.description]
                print (column_names)
    
            except Exception:
                print (traceback.format_exc())
                print ("Unable to perform operation. Please try again.")
                db_conn.rollback()

            finally:
                if cursor is not None:
                    cursor.close()

    def complete_list_columns(self, text, line, start_index, end_index):        
        "Auto complete list column command"
        global db_tables;

        if text:
            return [
                db_table for db_table in db_tables
                if db_table.startswith(text)
            ]
        else:
            return db_tables
            
        
if __name__ == '__main__':
    query_cmd = query()
    query_cmd.cmdloop()

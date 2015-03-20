
from __future__ import print_function

import cmd
import os
import psycopg2
import getpass
import sys
import traceback

from numbers import Number

## ============================================================================================
## Command Interpreter
## ============================================================================================

# DB Connection
db_conn = None;    

tables = set();
keywords = set();

column_to_table = {};
table_to_column = {};

class query(cmd.Cmd):
    intro = """English to SQL translator.\nType \"help\" for help.\n"""
    
    ruler = '-'
    prompt = 'query=# '
                

## ============================================================================================
## Basic commands
## ============================================================================================
                
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

## ============================================================================================
## Pick database
## ============================================================================================
    
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
            cursor = None
            
            db_conn = psycopg2.connect(database=db, user=c_user, password=c_password, 
                                    host=c_host, port=c_port)

                        
            ## POPULATE TABLES
            
            cursor = db_conn.cursor()                        
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

            global tables;
            rows = cursor.fetchall();                
            
            for row in rows:
                table = row[0]
                tables.add(table)
                keywords.add(table)

            ## POPULATE COLUMNS

            global table_to_column;
            global column_to_table;

            for table in tables:
                cursor.execute("SELECT * FROM " + table +" LIMIT 0")
                columns = [desc[0] for desc in cursor.description]
                
                table_to_column[table] = columns;
                
                for column in columns:
                    column_to_table[column] = table;
                    keywords.add(column)
                                                            
        except Exception:
            print (traceback.format_exc())
            print ("Unable to perform operation. Please try again.")
            if db_conn is not None:
                db_conn.rollback()

        finally:
            if cursor is not None:
                cursor.close()


## ============================================================================================
## List tables
## ============================================================================================
                                
    def do_list_tables(self, line):
        "List the tables in the database"

        if db_conn == None:
            print ("Pick database first")        
        else:
            print (tables)

## ============================================================================================
## List columns
## ============================================================================================
            
    def do_list_columns(self, line):
        "List the columns in the table"        
        
        if db_conn == None:
            print ("Pick database first")        
        else:
            table = line;
            try:
                print (table_to_column[table])
            except Exception:
                print (traceback.format_exc())
                print ("Unable to perform operation. Please try again.")
 

    def complete_list_columns(self, text, line, start_index, end_index):        
        "Auto complete list column command"

        if text:
            return [
                table for table in tables
                if table.startswith(text)
            ]
        else:
            return tables

## ============================================================================================
## Parse
## ============================================================================================
        
    def do_parse(self,line):
        "Parse a query"

        def isfloat(value):
            try:
              float(value)
              return True
            except ValueError:
              return False        
                  
        def isquoted(value):
            if value.startswith("'") and value.endswith("'"):          
                return True;
            else:
                if value.startswith("\"") and value.endswith("\""):          
                    return True;            
            return False;
                    
        # Tokenize
        tokens = line.split(' ')
        
        print("Tokens :: " + str(tokens))

        filtered_tokens = set()
            
        for token in tokens:
            if token in keywords or isfloat(token) or isquoted(token):                
                if(isquoted(token)):
                    token = token[1:-1]
                                
                filtered_tokens.add(token)        
        
        print("Filtered Tokens :: " + str(filtered_tokens))
        
        
if __name__ == '__main__':
    query_cmd = query()
    query_cmd.cmdloop()

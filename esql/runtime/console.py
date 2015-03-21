#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import getpass
import re
import glob
import colorama

from cmd2 import Cmd
from termcolor import colored

import drivers
import runtime.executor

# Driver
driver = None

keywords = set(['limit', "only", "just",
                'count', "many", "number",
                'is' , "equal", 'equals', 
                'greater', 'less', 'than', 'to'
                'between', 'like', 'in',
                'and', 'or', 'not',
                'larger', 'smaller', 'higher', 'lower'])


limit = False
limit_cnt = 0

count = False

## ============================================================================================
## SEQ SCAN
## ============================================================================================

def isfloat(value):
    "Check if value is a float"

    try:
      float(value)
      return True
    except ValueError:
      return False        
          
def isquoted(value):
    "Check if value is quoted"

    if value.startswith("'") and value.endswith("'"):          
        return True;
    elif value.startswith("\"") and value.endswith("\""):          
        return True;            

    return False;


def get_limit_string():
    "Get LIMIT string"
    
    if limit is True:
        limit_string = " LIMIT " + str(limit_cnt)
    else:
        limit_string = " "

    return limit_string

def get_double_quoted(token):
    "Get token with double quotes in front and back"
    
    return " \"" +  token + "\" "

def get_single_quoted(token):
    "Get token with single quotes in front and back"
    
    return " '" +  token + "' "

def get_operator(tokens):
    "Get next operator in tokens"
    
    print ("Get Operator :: " + str(tokens))
    greater = False
    lesser = False
    not_seen = False
    or_seen = False
    
    for token in tokens:
        if token == "equal" or token == "equals" or token == "is":
            if not_seen is False:
                return " = "
            else:
                return " <> "
        elif token == "not":
            not_seen = True;
        elif token == "greater" or token == "great" or token == "higher" or token == "larger":
            greater = True
        elif token == "lesser" or token == "less" or token == "lower" or token == "smaller":
            lesser = True
        elif token == "or":
            or_seen = True
            if greater is True:               
                return " >= "
            else:
                return " <= "
        elif token == "between":
            return " BETWEEN "
        elif token == "like":
            return " LIKE "
        elif token == "in":
            return "IN"

        elif token in column_to_table.keys():
            break;
        elif token in tables:
            break;

        else:
            continue;
        
    if greater:
        return " > "
    elif lesser:
        return " < "
        
    return " unk "
        
def run_seq_scan(tokens):
    "Execute seq scan"

    for token in tokens:
        if token in tables:
                
            if count:
                execute_query("SELECT COUNT(*) FROM " + token + get_limit_string())
            else:                
                execute_query("SELECT * FROM " + token + get_limit_string())
            break

def get_connective(token):
    "Check and return connective"
    
    if token == "and":
        return " AND ";
    elif token == "or":
        return " OR "

    return ""

def run_index_scan(tokens):
    "Execute index scan"

    lookup_string = ' WHERE '
    
    # Figure out columns
    for token in tokens:
        if token in column_to_table.keys():

            # Attr offset
            offset = tokens.index(token)

            # Value offset
            value = None                    
            for value in tokens[offset:]:
                if value.isdigit() or isquoted(token) or isfloat(token):
                    break;

            # Get operator
            operator = None
            operator = get_operator(tokens[offset+1:])
            
            print ("Operator : " + operator)

            if value is not None and operator is not None:
                if value.isdigit() or isfloat(token):
                    lookup_string = lookup_string  + get_double_quoted(token) + operator + value 
                else:
                    lookup_string = lookup_string  + get_double_quoted(token) + operator + get_single_quoted(value)
                
        lookup_string = lookup_string + get_connective(token)

    # Figure out table
    for token in tokens:
        if token in tables:
            table = token
            break
        
    if count:
        execute_query("SELECT COUNT(*) FROM " + table + lookup_string + get_limit_string())
    else:                
        execute_query("SELECT * FROM " + table + lookup_string + get_limit_string())


## ============================================================================================
## Command Interpreter
## ============================================================================================

class console(Cmd):
    intro = """English to SQL translator.\nType \"help\" for help.\n"""
    
    prompt = colored('query=# ', 'green')

    ## Basic commands

    def do_clear(self, line):
        "Clear the shell"        
        os.system('clear')    

    def do_ls(self, line):
        "List the contents of current dir"
        os.system('ls')    

    def do_pwd(self, line):
        "List the current dir"
        os.system('pwd')    

    ## Execute SQL query directly
                                
    def do_sql(self, query):
        "Execute SQL query directly"
        driver.executeQuery(query);
        
    ## Select database
    
    def do_db(self, db_name):
        "Select the database to analyze"
                
        print ("Looking up database : ", db_name)
        driver.selectDatabase(db_name)

    ## List tables
                                
    def do_list_tables(self, line):
        "List the tables in the database"        
        tables = driver.listTables()
        
        if tables is not None:
            print (tables)

    ## List columns
            
    def do_list_columns(self, table_name):
        "List the columns in the table"        
        columns = driver.listTableColumns(table_name)
        
        if columns is not None:
            print(columns)

    def complete_list_columns(self, text, line, start_index, end_index):        
        "Auto complete list column"

        if text:
            return [
                table for table in driver.listTables()
                if table.startswith(text)
            ]
        else:
            return tables

    ## Parse an english query
        
    def do_p(self, query):
        "Parse an english query"
                            
        # Tokenize
        tokens = query.split(' ')
        
        filtered_tokens = []
          
        table_ref = 0
        column_ref = 0
        point_lookup = 0
        range_lookup = 0
            
        for token in tokens:
            if token in keywords or isfloat(token) or isquoted(token):                
                if(isquoted(token)):
                    token = token[1:-1]
                                
                filtered_tokens.append(token)        
        
                if token in tables:
                    table_ref = table_ref + 1
                    
                if token in column_to_table.keys():
                    column_ref = column_ref + 1                    
                
        print("Filtered Tokens :: " + str(filtered_tokens))

        # Set LIMIT
        global limit;
        global limit_cnt;
        
        limit = False
        if "limit" in tokens or "only" in tokens or "just" in tokens:
            limit = True

            try:
                if "limit" in tokens:
                    offset = tokens.index("limit")
                elif "only" in tokens:
                    offset = tokens.index("only")
                else:
                    offset = tokens.index("just")

                limit_cnt = None
                for token in tokens[offset+1:]:
                    if token.isdigit():
                        limit_cnt = token;    
                    
                limit = limit_cnt.isdigit()
                
            except ValueError:
                limit = False;

        # Set COUNT
        global count;
        
        count = False
        if "count" in tokens or "many" in tokens or "number" in tokens:
            count = True
            
        # Set sequential scan
        if table_ref == 1 and column_ref == 0:
            run_seq_scan(filtered_tokens);

        # Set index scan
        if table_ref == 1 and column_ref != 0:
            run_index_scan(filtered_tokens);
            

## ==============================================
## loadDriverClass
## ==============================================

def loadDriverClass(name):
    "Load the appropriate driver"
    
    full_name = "%sDriver" % name.title()

    driver_name = "%s_driver" % name.title().lower()
    mod = __import__('drivers.%s' % driver_name, globals(), locals(), [full_name])

    print (colored("Loaded :: " + full_name, 'red'))    
    klass = getattr(mod, full_name)
    return klass

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    "Get driver list"
    
    drivers = []
    for f in map(lambda x: os.path.basename(x).replace("_driver.py", ""), glob.glob("./drivers/*_driver.py")):
        if f != "abstract": 
            drivers.append(f)

    return (drivers)

## ==============================================
## Entry point for console
## ==============================================        
def startConsole():
    "Start Console"
        
    system = 'postgres'
    
    ## Create a handle to the target client driver
    global driver
    
    driver_class = loadDriverClass(system)
    assert driver_class != None, "Failed to find '%s' class" % system
    driver = driver_class()
    
    # Start console    
    c = console()        
    c.cmdloop()

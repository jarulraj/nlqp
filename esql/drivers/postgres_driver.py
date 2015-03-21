#!/usr/bin/env python
# -*- coding: utf-8 -*-

from abstract_driver import *

import psycopg2
import traceback
import colorama

from tabulate import tabulate
from termcolor import colored

## ==============================================
## PostgresDriver
## ==============================================
class PostgresDriver(AbstractDriver):
    
    DEFAULT_CONFIG = {                      
        "username" : 'parallels' ,
        "password" : 'parallels' ,
        "host"     : 'localhost' ,
        "port"     : '5432',
        "dbname"   : 'parallels'
    }

    def __init__(self):
        super(PostgresDriver, self).__init__("postgres")
        self.conn = None
        self.cursor = None        
        
        self.config = self.makeDefaultConfig()
                
    def makeDefaultConfig(self):
        return PostgresDriver.DEFAULT_CONFIG
    
    def loadConfig(self, config):
        for key in PostgresDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)

        try:                
            self.conn = psycopg2.connect(database=config['dbname'], user=config['username'], 
                                     password= config['password'], host= config['host'],
                                     port = config['port'])
        
            self.cursor = self.conn.cursor()                                
        
        except Exception:
            print (traceback.format_exc())
            print ("Unable to perform operation. Please try again.")
            if self.conn is not None:
                self.conn.rollback()
        
    def selectDatabase(self, db_name):     
        PostgresDriver.DEFAULT_CONFIG["dbname"] = db_name;
        self.loadConfig(self.config)
        
        try:
            ## POPULATE TABLES
            self.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    
            rows = self.cursor.fetchall();                
            
            for row in rows:
                table = row[0]
                self.tables.add(table)
                
            ## POPULATE COLUMNS
            for table in self.tables:
                self.cursor.execute("SELECT * FROM " + table +" LIMIT 0")

                columns = [desc[0] for desc in self.cursor.description]                

                self.table_to_column[table] = columns;                
                for column in columns:
                    self.column_to_table[column] = table;
                                                            
        except Exception:
            print (traceback.format_exc())
            print ("Unable to perform operation. Please try again.")
            if self.conn is not None:
                self.conn.rollback()

    def listTables(self):        
        if self.conn == None:
            print ("Select the database first")        
        else:
            return self.tables
        
    def listTableColumns(self, table_name):
        
        if self.conn == None:
            print ("Select the database first")        
        elif table_name in self.table_to_column:
            return self.table_to_column[table_name]
            
        return None
                
    def executeQuery(self, query):

        if self.conn is None:
            print ("Select the database first")        
            return
            
        print(colored("SQL Query :: " + query, 'blue'))
        
        try:
            self.cursor.execute(query)    
            columns = [desc[0] for desc in self.cursor.description]                            

            rows = self.cursor.fetchall();               
            if rows is not None:    
                try:               
                    print (colored(tabulate(rows, headers=columns, tablefmt="psql"), 'red'))
                except UnicodeDecodeError:
                    print(colored(rows, 'red'))                
    
        except Exception:
            print (traceback.format_exc())
            print ("Unable to perform operation. Please try again.")
            if self.conn is not None:
                self.conn.rollback()


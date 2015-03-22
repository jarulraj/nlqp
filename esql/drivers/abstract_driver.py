#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime

## ==============================================
## AbstractDriver
## ==============================================
class AbstractDriver(object):

    def __init__(self, name):
        self.name = name
        self.driver_name = "%s Driver" % self.name.title()
        self.config = None
        self.keywords = set()
        
        # Schema information
        self.tables = set()
        self.columns = set()
        self.column_to_table = {}
        self.table_to_column = {}
        
    def __str__(self):
        return self.driver_name
    
    def makeDefaultConfig(self):
        """List the items that need to be in your implementation's configuration file.
        Each item in the list is a doublet containing: ( <PARAMETER NAME>, <DEFAULT VALUE> )
        """        
        raise NotImplementedError("%s does not implement makeDefaultConfig" % (self.driver_name))
    
    def loadConfig(self, config):
        """Initialize the driver using the given configuration dict"""
        raise NotImplementedError("%s does not implement loadConfig" % (self.driver_name))
        
    def formatConfig(self, config):
        """Return a formatted version of the config dict that can be used with the --config command line argument"""
        
        ret =  "# %s Configuration File\n" % (self.driver_name)
        ret += "# Created %s\n" % (datetime.now())
        ret += "[%s]" % self.name
        
        for name in config.keys():
            desc, default = config[name]
            if default == None: 
                default = ""
            ret += "\n\n# %s\n%-20s = %s" % (desc, name, default)             

        return (ret)
        
    def selectDatabase(self, db_name):
        """Establish connection and initialize information about the database"""
        raise NotImplementedError("%s does not implement selectDatabase" % (self.driver_name))

    def listTables(self):
        """Return the list of tables in the database"""
        raise NotImplementedError("%s does not implement listTables" % (self.driver_name))

    def listTableColumns(self, table_name):
        """Return the list of columns in the table"""
        raise NotImplementedError("%s does not implement listTableColumns" % (self.driver_name))
                    
    def executeQuery(self, query):
        """Execute a query"""        
        raise NotImplementedError("%s does not implement executeQuery" % (self.driver_name))

    def parseQuery(self, query):
        """Parse and execute an English query"""        
        raise NotImplementedError("%s does not implement parseQuery" % (self.driver_name))
        
## CLASS
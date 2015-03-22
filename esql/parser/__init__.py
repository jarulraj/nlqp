# -*- coding: utf-8 -*-

import runtime.executor

keywords = set(['limit', "only", "just",
                'count', "many", "number",
                'is' , "equal", 'equals', 
                'greater', 'less', 'than', 'to'
                'between', 'like', 'in',
                'and', 'or', 'not',
                'larger', 'smaller', 'higher', 'lower'])


limit = False
limit_cnt = 0

table_ref = 0
column_ref = 0
point_lookup = 0
range_lookup = 0


count = False

def isFloat(value):
    "Check if value is a float"

    try:
      float(value)
      return True
    except ValueError:
      return False        
          
def isQuoted(value):
    "Check if value is quoted"

    if value.startswith("'") and value.endswith("'"):          
        return True;
    elif value.startswith("\"") and value.endswith("\""):          
        return True;            

    return False;

def getConnective(token):
    "Check and return connective"
    
    if token == "and":
        return " AND ";
    elif token == "or":
        return " OR "

    return ""


def getLimitString():
    "Get LIMIT string"
    
    if limit is True:
        limit_string = " LIMIT " + str(limit_cnt)
    else:
        limit_string = " "

    return limit_string

def getDoubleQuoted(token):
    "Get token with double quotes in front and back"
    
    return " \"" +  token + "\" "

def getSingleQuoted(token):
    "Get token with single quotes in front and back"
    
    return " '" +  token + "' "

def getOperator(driver, tokens):
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

        elif token in driver.column_to_table.keys():
            break;
        elif token in driver.tables:
            break;

        else:
            continue;
        
    if greater:
        return " > "
    elif lesser:
        return " < "
        
    return " unk "

## ==============================================
## Parser
## ==============================================
def parse(driver, query):
        "Parse an english query"
                            
        # Tokenize
        tokens = query.split(' ')
        
        filtered_tokens = []

        global table_ref
        global column_ref
        global point_lookup
        global range_lookup
                  
        table_ref = 0
        column_ref = 0
        point_lookup = 0
        range_lookup = 0
            
        for token in tokens:
            if token in driver.keywords or isFloat(token) or isQuoted(token):                
                if(isQuoted(token)):
                    token = token[1:-1]
                                
                filtered_tokens.append(token)        
        
                if token in driver.tables:
                    table_ref = table_ref + 1
                    
                if token in driver.column_to_table.keys():
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
                return None

        # Set COUNT
        global count;
        
        count = False
        if "count" in tokens or "many" in tokens or "number" in tokens:
            count = True

        return filtered_tokens
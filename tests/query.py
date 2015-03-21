import requests

# Read File and Query

filename = 'queries.txt'
output = 'output.txt'

out = open(output, 'w');

with open(filename) as f:
    content = f.readlines()

for line in content:
    out.write("EH Query  ::  " + line.strip().replace("+"," ") +"\n")

    query_url = "http://english2sql.com/cgi-bin/eng2sql.cgi?question=&question1=" + line + "&output=html"
    r = requests.get(query_url)

    out.write("SQL Query :: " + r.text.split("<TR>")[1].replace("<TD>"," ") + "\n")

# Reference: http://boto3.readthedocs.io/en/latest/guide/configuration.html
# https://stackoverflow.com/questions/41832134/reading-csv-from-s3-and-inserting-into-a-mysql-table-with-aws-lambda
# https://github.com/PyMySQL/PyMySQL
# http://www.programcreek.com/python/example/957/hashlib.sha256
# https://stackoverflow.com/questions/14127529/mysql-import-data-from-csv-using-load-data-infile
# https://www.devart.com/dotconnect/mysql/docs/Devart.Data.MySql~Devart.Data.MySql.MySqlConnection~Commit.html


import boto3,os,pymysql,time,memcache,hashlib
from botocore.client import Config
from flask import Flask, request
from flask import render_template

ACCESS_KEY_ID = ''
ACCESS_SECRET_KEY = '3'
BUCKET_NAME = 'saipriya'

# Connect to S3
Connect_S3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY,
    config=Config(signature_version='s4')
)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))

# connecting to memcached
memcache = memcache.Client([''], debug=0)
print 'connected to memcached'

# credentials to connect to the database
hostname = ''
username = 's'
password = ''
database = ''
myConnection = pymysql.connect(host=hostname, user=username, passwd=password, db=database,charset='utf8mb4', local_infile=True)
print "Connected to SQL"

query = 'select * from quakes limit '
hash = hashlib.sha256(query.encode('utf-8')).hexdigest()
cur = myConnection.cursor()

app = Flask(__name__)

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/login', methods=['POST'])
def login():
    for object in Connect_S3.Bucket('saipriya').objects.all():
        print object.key
        if 'signin.txt' == object.key:
            read = object.get()['Body'].read()
            user_name = request.form['uname']
            user_pass = request.form['password']
            username, password = read.split(":")
            if username == user_name.encode() and password == user_pass.encode():
                print("User credentials successful")
                return render_template('filehandle.html')
            else:
                return "You entered a wrong password! Please try again"

@app.route('/upload', methods=['POST'])
def upload():
    requestfile = request.files['file']
    file_name = requestfile.filename
    data = requestfile.read()
    Connect_S3.Bucket('saipriya').put_object(Key=file_name, Body=data)
    return "File uploaded succesfully!"

@app.route('/csvupload', methods=['POST'])
def csvupload():
    file_name = request.form['csvfile']
    splitfile = file_name.split('.')[0]
    for object in Connect_S3.Bucket('saipriya').objects.all():
        print(object.key)
        if 'boat.csv' == object.key:
            body = object.get()['Body'].read()
            mystr = []
            str = body.split('\n')[0]
            print(str)
            mystr = str.split(',')
            cur = myConnection.cursor()
            droptable="DROP TABLE IF EXISTS %s"%splitfile
            print droptable
            cur.execute(droptable)
            print "Table dropped successfully"
            print(mystr[0], len(mystr))
            executequery1="create table quakes (time text,latitude double,longitude double,depth double,mag double,magType text,nst text,gap text,dmin text,rms double,net text,id text,updated text,place text,type text,horizontalError double,depthError double,magError text,magNst text,status text,locationSource text,magSource text)"
            cur.execute(executequery1)
            executequery2 = 'load data local infile \'C:/Users/Saipriya/PycharmProjects/Assignment4/quakes.csv\' into table quakes fields terminated by \',\' optionally enclosed by \'"\' lines terminated by \'\n\' ignore 1 lines;'
            cur.execute(executequery2)
            count="select count(*) from quakes"
            cur.execute(count)
            result = cur.fetchall()
            c = 0
            str1 = " "
            for res in result:
                c = c + 1
                print str(c) + ':' + str(res)
                str1 += str(c) + ':' + str(res) + '<br><br>'
            myConnection.commit()
            return render_template('filehandle.html', rdscount=result)

@app.route('/sqlexecute', methods=['POST'])
def sqlexecute():
    limit = request.form['limit']
    starttime = time.time()
    print(starttime)
    cur.execute(query + limit)
    endtime = time.time()
    print('endtime')
    totalsqltime = endtime - starttime
    print(totalsqltime)
    return render_template('filehandle.html', rdstime1=totalsqltime)

@app.route('/cleanexecute',methods=['POST'])
def cleanexecute():
    save="savepoint s1"
    cur.execute(save)
    print "save point created"
    safeupdate="SET SQL_SAFE_UPDATES = 0"
    cur.execute(safeupdate)
    cleanquery="update quakes set depth=3.6 where mag=2.8"
    cur.execute(cleanquery)
    print "executed query"
    s="select * from quakes where depth=3.6"
    cur.execute(s)
    result = cur.fetchall()
    c = 0
    str1 = " "
    for row in result:
        c = c + 1
        print str(c) + ':' + str(row)
        str1 += str(c) + ':' + str(row) + '<br><br>'
    myConnection.commit()
    return 'Executed'

@app.route('/query1', methods=['POST'])
def query1():
    q1="select * from quakes where mag between (select min(mag) from quakes) and (select max(mag) from quakes) having place like '%Alaska'";
    cur.execute(q1)
    result = cur.fetchall()
    c = 0
    str1 = " "
    for row in result:
        c = c + 1
        print str(c) + ':' + str(row)
        str1 += str(c) + ':' + str(row) + '<br><br>'
    return str(str1)

@app.route('/query2', methods=['POST'])
def query2():
    r1=request.form['val1']
    r2=request.form['val2']
    q2="select * from quakes where place like '%"+r1+"' or place like '%"+r2+"'"
    cur.execute(q2)
    result = cur.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print str(c) + ':' + str(res)
        str1 += str(c) + ':' + str(res) + '<br><br>'
    return str(str1)

@app.route('/query3', methods=['POST'])
def query3():
    r1 = request.form['val1']
    print r1
    r2 = request.form['val2']
    q2="select * from quakes where DAY(time) between day('%s') and day('%s')"%(r1,r2)
    cur.execute(q2)
    result = cur.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print str(c) + ':' + str(res)
        str1 += str(c) + ':' + str(res) + '<br><br>'
    return str1

@app.route('/query4', methods=['POST'])
def query4():
    r1 = request.form['val1']
    print r1
    r2 = request.form['val2']
    r3 = request.form['val3']
    # q2 = "select * from quakes where mag between %s and %s"%(r1,r2)
    q2 = "select * from quakes where DAY(time) between day('%s') and day('%s')" % (r1, r2)
    cur.execute(q2)
    result = cur.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print str(c) + ':' + str(res)
        str1 += str(c) + ':' + str(res) + '<br><br>'
    return str1

@app.route('/memexecute', methods=['POST'])
def memexecute():
    limit = request.form['limit']
    cur.execute(query + limit)
    result = cur.fetchall()
    memcache.set(hash, result)
    c = 0
    for res in result:
        c = c + 1
        print(str(c) + ':' + str(res))
    starttime = time.time()
    memresult = memcache.get(hash)
    endtime = time.time()
    total = endtime - starttime
    print('Time taken by memcache ', total)
    return render_template('filehandle.html', rdstime2=total)

if __name__ == '__main__':
    app.run(port=5001, debug=True)

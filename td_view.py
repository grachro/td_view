from bottle import route, run, static_file, request, response
import tdclient
import myenv



apikey=myenv.apikey
endpoint=myenv.endpoint
static_root=myenv.static_root

def get_db_names():
    db_names = []
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        for db in client.databases():
            db_names.append(db.name)
    db_names.sort()
    return db_names


def get_tables(db_name, filter):
    tables = {}
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        for db in client.databases():
            if db.name == db_name:
                for table in db.tables():
                    if filter in table.table_name:
                        tables[table.table_name] = table.count
    return tables

def get_table_schema(db_name, table_name):
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        table = client.table(db_name, table_name)
        return table.schema

def get_table_cols(db_name, table_name):
    schema = get_table_schema(db_name, table_name)
    cols = []
    for col in schema:
        cols.append(col[0])
    return cols

def html(body):
    return """
<html>
<head>
<title class="table">td_view</title>


<link rel="stylesheet" href="/static/bootstrap-3.3.7/css/bootstrap.min.css">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"></script>
<script src="/static/bootstrap-3.3.7/js/bootstrap.min.js"></script>

</head>
<a href="/">go top</a>
{body}
</html>
    """.format(body = body)



def edit_filter_html(db_name, filter):
   
    body = """
<div>
<form method="get" action="/table_list/{db_name}">
filter <input type="text" name="filter" value="{filter}"></input><input type="submit"></input>
</form>
</div>
""".format(db_name = db_name, filter = filter,)

    return body

@route('/static/<filename:path>')
def send_static(filename):
    return static_file(filename, root=static_root)
    
@route('/')
def index():
    body = """
<h1>db list</h1>
<ul>
"""  
    for db_name in get_db_names():
        body += "<li><a href='/table_list/{db_name}'>{db_name}</a></li>".format(db_name = db_name)
    
    body += "</ul>"
    
    return html(body)




@route('/table_list/<db_name>')
def table_list(db_name):
    
    filter = ""
    try:
        filter= request.query['filter']    
    except:
        pass  
    filter_html = edit_filter_html(db_name, filter)
   
    
    tables = get_tables(db_name, filter)
    body = """
<h1>db {db_name}</h1>
{filter_html}

<table class="table table-bordered">
<tr><th>table name</th><th>record count</th></tr>
""".format(db_name = db_name, filter_html = filter_html,)

    for tbl_name,tbl_count in sorted(tables.items()):
        list = """
<tr>
  <td>{tbl_name}</td>
  <td>{tbl_count}</td>
  <td><a href="/table/{db_name}/{tbl_name}?filter={filter}">show</a>
</tr>
""".format(
  db_name = db_name,
  tbl_name  = tbl_name,
  tbl_count =tbl_count,
  filter=filter,
)
        body += list

    body += "</table>"
    return html(body)




@route('/table/<db_name>/<table_name>')
def show_table(db_name, table_name):
    
    filter = ""
    try:
        filter= request.query['filter']    
    except:
        pass  
    filter_html = edit_filter_html(db_name, filter)
    
    body = """
<h1>db {db_name}</h1>
{filter_html}

<h2>table {table_name}</h2>
<table class="table table-bordered">
<tr><th>col name</th><th>col type</th></tr>
""".format(
    db_name = db_name,
    table_name = table_name,
    filter_html=filter_html,
)

    for col in get_table_schema(db_name, table_name):
        tr = """
<tr>
  <td>{col_name}</td>
  <td>{col_type}</td>
</tr>
""".format(
    col_name = col[0],
    col_type = col[1],
)
        body += tr            
        

    body += "</table>"

    body += """


<h3>download tsv</h3>
<form action="/download_tsv/{db_name}">
order by <input type="text" name="order_by">
<input type="submit" value="tsv">
<input type="hidden" name="table_name" value="{base_table_name}">
</form>

<h3>copy</h3>
<form action="/copy_schema_and_insert_all/{db_name}">
new table name <input type="text" name="new_table_name">
<input type="submit" value="copy schema and data">
<input type="hidden" name="base_table_name" value="{base_table_name}">
</form>

<form action="/copy_schema/{db_name}">
new table name <input type="text" name="new_table_name">
<input type="submit" value="copy schema only">
<input type="hidden" name="base_table_name" value="{base_table_name}">
</form>


<form action="/insert_all/{db_name}">
target table name <input type="text" name="target_table_name">
<input type="submit" value="copy data only">
<input type="hidden" name="base_table_name" value="{base_table_name}">
</form>


""".format(
    db_name = db_name,
    base_table_name = table_name,
    filter_html=filter_html,
)
 
    return html(body)


@route('/copy_schema/<db_name>')
def copy_schema(db_name):
    base_table_name = request.query['base_table_name']
    new_table_name  = request.query['new_table_name']

    #get base tabel schema
    schema = get_table_schema(db_name, base_table_name)

    #copy schema
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        client.create_log_table(db_name, new_table_name)
        client.update_schema(db_name, new_table_name, schema)


    return show_table(db_name, new_table_name)

def insert(db_name, base_table_name, target_table_name, schema):
    cols = []
    for col in schema:
        cols.append(col[0])

    query = """
insert into table {target_table_name} select {cols} from {base_table_name}
""".format(
  target_table_name = target_table_name,
  base_table_name = base_table_name,
  cols = ",".join(cols),
)
    #copy schema
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        job = client.query(db_name, query, type="hive")
        job.wait()

@route('/insert_all/<db_name>')
def insert_all(db_name):
    base_table_name = request.query['base_table_name']
    target_table_name  = request.query['target_table_name']

    #get base tabel schema
    schema = get_table_schema(db_name, base_table_name)

    insert(db_name, base_table_name, target_table_name, schema)

    return show_table(db_name, target_table_name)



@route('/copy_schema_and_insert_all/<db_name>')
def copy_schema_and_insert_all(db_name):
    base_table_name = request.query['base_table_name']
    new_table_name  = request.query['new_table_name']

    #get base tabel schema
    schema = get_table_schema(db_name, base_table_name)

    #copy schema
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        client.create_log_table(db_name, new_table_name)
        client.update_schema(db_name, new_table_name, schema)

    insert(db_name, base_table_name, new_table_name, schema)


    return show_table(db_name, new_table_name)


@route('/download_tsv/<db_name>')
def download_tsv(db_name):
    table_name = request.query['table_name']
    order_by = request.query['order_by']
    download_file_name = "{table_name}.tsv".format(table_name=table_name)

    cols = get_table_cols(db_name, table_name)

    query = """
select {cols} from {table_name} order by {order_by}
""".format(
  table_name = table_name,
  order_by = order_by,
  cols = ",".join(cols),
)


    content = "\t".join(cols) + "\n"

    #copy schema
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        job = client.query(db_name, query, type="presto")
        job.wait()
        for row in job.result():
            line = "\t".join(row) + "\n"
            content += line


    response.content_type = 'ext/tsv'
    response.set_header('Content-Length', str(len(content)))
    response.set_header('Content-Disposition', 'attachment; filename="%s"' % download_file_name)
    return content


run(host=myenv.host, port=myenv.port, debug=True, reloader=True)




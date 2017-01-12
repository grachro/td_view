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
filter <input type="text" name="filter" value="{filter}"></input><input type="submit" value="find"></input>
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
<h1>td view</h1>

<h2>db list</h2>
<ul>
"""  
    for db_name in get_db_names():
        body += "<li><a href='/table_list/{db_name}'>{db_name}</a></li>".format(db_name = db_name)
    
    body += "</ul>"
    return html(body)




@route('/table_list/<db_name>')
def table_list(db_name):
    
    
    filter= request.query.filter or ""   
    filter_html = edit_filter_html(db_name, filter)
   

    if filter == "":    
        tables = {}
    else:
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
    
    filter= request.query.filter or ""   
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
  <div class="form-group">
    <label>order by</label>
    <input class="form-control" type="text" name="order_by">
  </div>

  <div class="form-check">
    <label class="form-check-label">
      <input class="form-check-input" type="checkbox" name="with_time" checked="checked">
      time
    </label>
  </div>
  <button type="submit" class="btn btn-primary">download tsv</button>
  <input type="hidden" name="table_name" value="{from_table_name}">
</form>

<h3>copy</h3>
<a href="/copy_form?from_db_name={db_name}&from_table_name={from_table_name}&to_db_name={db_name}">go copy form</a>
""".format(
    db_name = db_name,
    from_table_name = table_name,
    filter_html=filter_html,
)
 
    return html(body)


def copy_schema(to_db_name, to_table_name, schema):
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        client.create_log_table(to_db_name, to_table_name)
        client.update_schema(to_db_name, to_table_name, schema)


def copy_data(from_db_name, from_table_name, to_db_name, to_table_name, cols):


    query = """
insert into table {to_db_name}.{to_table_name} select {cols} from {from_table_name}
""".format(
  from_table_name = from_table_name,
  to_db_name = to_db_name,
  to_table_name = to_table_name,
  cols = ",".join(cols),
)
    with tdclient.Client(apikey,endpoint=endpoint) as client:
        job = client.query(from_db_name, query, type="hive")
        job.wait()


@route('/copy_table')
def copy_table():
    from_db_name = request.query['from_db_name']
    from_table_name = request.query['from_table_name']
    to_db_name  = request.query['to_db_name']
    to_table_name  = request.query['to_table_name']
    copy_mode = request.query.copy_mode
    with_time = request.query.with_time or ""

    #get base tabel schema
    schema = get_table_schema(from_db_name, from_table_name)

    cols = []
    for col in schema:
        cols.append(col[0])
    if with_time == "on":
        cols.append("time")


    if copy_mode == "all":
        copy_schema(to_db_name, to_table_name, schema)
        copy_data(from_db_name, from_table_name, to_db_name, to_table_name, cols)
    elif copy_mode == "schema":
        copy_schema(to_db_name, to_table_name, schema)
    elif copy_mode == "data":
        copy_data(from_db_name, from_table_name, to_db_name, to_table_name, cols)

    return show_table(to_db_name, to_table_name)


@route('/download_tsv/<db_name>')
def download_tsv(db_name):
    table_name = request.query['table_name']
    order_by = request.query['order_by']
    
    with_time = request.query.with_time or ""
    
    download_file_name = "{table_name}.tsv".format(table_name=table_name)

    cols = get_table_cols(db_name, table_name)
    if with_time == "on":
        cols.append("time")

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
            line = "\t".join(map(str,row)) + "\n"
            content += line


    response.content_type = 'ext/tsv'
    response.set_header('Content-Length', str(len(content)))
    response.set_header('Content-Disposition', 'attachment; filename="%s"' % download_file_name)
    return content


def edit_copy_form_html(from_db_name,from_table_name,to_db_name,to_table_name):
    return """
<form action="/copy_table">

  <div class="form-group">
    <label>from db</label>
    <input class="form-control" type="text" name="from_db_name" value="{from_db_name}">
  </div>
  <div class="form-group">
    <label>from table</label>
    <input class="form-control" type="text" name="from_table_name" value="{from_table_name}">
  </div>

  <div class="form-group">
    <label>to db</label>
    <input class="form-control" type="text" name="to_db_name" value="{to_db_name}">
  </div>
  <div class="form-group">
    <label>to table</label>
    <input class="form-control" type="text" name="to_table_name" value="{to_table_name}">
  </div>
  
  
  <label class="form-check-inline">
    <input class="form-check-input" type="radio" name="copy_mode" id="inlineRadio1" value="all" checked> copy schema and data 
  </label>
  <label class="form-check-inline">
    <input class="form-check-input" type="radio" name="copy_mode" id="inlineRadio2" value="schema"> schema only 
  </label>
  <label class="form-check-inline">
    <input class="form-check-input" type="radio" name="copy_mode" id="inlineRadio3" value="data"> data only 
  </label>  
  
  <div class="form-check">
    <label class="form-check-label">
      <input class="form-check-input" type="checkbox" name="with_time" checked="checked">
      time
    </label>
  </div>
  <button type="submit" class="btn btn-primary">copy</button>
</form>
 """.format(
     from_db_name = from_db_name,
     from_table_name = from_table_name,
     to_db_name = to_db_name,
     to_table_name = to_table_name,
    )
    

@route('/copy_form')
def copy_form():
    
    from_db_name    = request.query.from_db_name or ""
    from_table_name = request.query.from_table_name or ""
    to_db_name    = request.query.to_db_name or ""
    to_table_name = request.query.to_table_name or ""
    
    
    body = """<h1>copy</h1>"""
    body += edit_copy_form_html(from_db_name,from_table_name,to_db_name,to_table_name)
    
    return html(body)

run(host=myenv.host, port=myenv.port, debug=True, reloader=True)




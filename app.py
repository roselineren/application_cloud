from flask import Flask, render_template, redirect, request
from ssh_pymongo import MongoSession
import pandas as pd

app = Flask(__name__)
session = MongoSession(
    host='mesiin592023-00057.westeurope.cloudapp.azure.com',
    uri='mongodb://mesiin592023-00057:30000',
    user='administrateur',
    password='SuperPassword!1'
)

db = session.connection['employee']
collection = db["employees"]

classes = "table table-striped table-bordered"
lim = 5
@app.route('/', methods=['GET', 'POST'])


def home():
    global lim
    lim = request.args.get("lim")
    lim = 5 if lim is None else int(lim)
    return render_template('base.html')

def index():
    return render_template('index.html')

## Queries
def query_ru1():
    
    pipeline = [
        {"$unwind": "$departments"},
        {"$group": {
            "_id": {
                "department": "$departments.dept_name",
                "gender": "$gender"
            },
            "count": {"$sum": 1}
        }},
        {"$project": {
            "department": "$_id.department",
            "gender": "$_id.gender",
            "count": 1,
            "_id": 0
        }}
    ]
    result = list(db.employees.aggregate(pipeline))
    return result

def query_ru2():
    query = {
        "birth_date": {"$gt": "1963-01-01"}
    }
    projection = {
        "_id": 0,
        "first_name": 1,
        "last_name": 1,
        "birth_date": 1,
        "departments.dept_name": 1
    }
    result = list(db.employees.find(query, projection))
    df = pd.DataFrame(result)
    return df

def query_ru3():
    query = {
        "titles": {
            "$not": {
                "$elemMatch": {
                    "to_date": "9999-01-01"
                }
            }
        }
    }
    projection = {
        "_id": 0,
        "emp_no": 1,
        "first_name": 1,
        "last_name": 1,
        "titles": 1,
        "hire_date": 1,
        "salaries": 1
    }
    result = list(db.employees.find(query, projection))
    df = pd.DataFrame(result)
    return df
def query_ru6():
    pipeline = [
        {"$match": {"departments.dept_name": "Finance"}},
        {"$unwind": "$salaries"},
        {"$match": {"salaries.to_date": "9999-01-01"}},
        {"$sort": {"salaries.salary": -1}},
        {"$limit": 3},
        {"$project": {
            "_id": 0,
            "first_name": 1,
            "last_name": 1,
            "salary": "$salaries.salary"
        }}
    ]
    result = list(db.employees.aggregate(pipeline))
    df = pd.DataFrame(result)
    return df

def query_rda4():
    pipeline = [
        {"$unwind": "$departments"},
        {"$unwind": "$salaries"},
        {"$group": {
            "_id": "$departments.dept_name",
            "totalEmployees": {"$sum": 1},
            "averageSalary": {"$avg": "$salaries.salary"},
            "minSalary": {"$min": "$salaries.salary"},
            "maxSalary": {"$max": "$salaries.salary"}
        }},
        {"$project": {
            "department": "$_id",
            "totalEmployees": 1,
            "averageSalary": 1,
            "minSalary": 1,
            "maxSalary": 1
        }},
        {"$sort": {"department": 1}}
    ]
    result = list(db.employees.aggregate(pipeline))
    df = pd.DataFrame(result)
    return df

def query_rda5():
    pipeline = [
        {"$unwind": "$departments"},
        {"$match": {"departments.dept_name": {"$in": ["Sales", "Marketing"]}}},
        {"$unwind": "$salaries"},
        {"$match": {"salaries.to_date": "9999-01-01"}},
        {"$group": {
            "_id": "$departments.dept_name",
            "averageSalary": {"$avg": "$salaries.salary"}
        }},
        {"$project": {"_id": 0, "department": "$_id", "averageSalary": 1}}
    ]
    result = list(db.employees.aggregate(pipeline))
    df = pd.DataFrame(result)
    return df

def query_rda6():
    query = {
        "departments.dept_name": "Production"
    }
    projection = {
        "_id": 0,
        "emp_no": 1,
        "first_name": 1,
        "last_name": 1,
        "birth_date": 1,
        "hire_date": 1,
        "gender": 1
    }
    result = list(db.employees.find(query, projection))
    df = pd.DataFrame(result)
    return df




## Routes
@app.route('/ru1')

def ru1():
    data = query_ru1()
    html_data = data.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete 1", result=html_data)

@app.route('/ru2')
def ru2():
    df = query_ru2()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete 2", result=html_data)


@app.route('/ru3')
def ru3():
    df = query_ru3()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete 3", result=html_data)


@app.route('/ru6')
def ru6():
    df = query_ru6()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete 6", result=html_data)

@app.route('/rda4')
def rda4():
    df = query_rda4()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda4", result=html_data)

@app.route('/rda5')
def rda5():
    df = query_rda5()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda5", result=html_data)

@app.route('/rda6')
def rda6():
    df = query_rda6()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda6", result=html_data)

@app.route('/adminView', methods=("POST", "GET"))
def adminView():

    #stats et temps execution 

if __name__ == '__main__':
    app.run(debug=True)

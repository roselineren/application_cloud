from flask import Flask, render_template, redirect, request,url_for
from ssh_pymongo import MongoSession
import pandas as pd
import matplotlib.pyplot as plt
import pickle

app = Flask(__name__)
session = MongoSession(
    host='mesiin592023-00057.westeurope.cloudapp.azure.com',
    uri='mongodb://mesiin592023-00057:30000',
    user='administrateur',
    password='SuperPassword!1'
)

db = session.connection['employee_db']
collection = db["employees"]

classes = "table table-striped table-bordered"
lim = 5
@app.route('/', methods=['GET', 'POST'])


def home():
    global lim
    lim = request.args.get("lim")
    lim = 5 if lim is None else int(lim)
    return render_template('base.html')

## Execution times 
with open('dictionaries.pkl', 'rb') as f:
    execution_times_1shard, execution_times_2shard, execution_times_3shard, execution_times_4shard, execution_times_5shard, execution_times_6shard = pickle.load(f)
queries = ['ru1', 'ru2', 'ru3', 'ru6', 'rda1', 'rda4', 'rda5', 'rda6']
def plot_times(queries):
    

    for query in queries:
        plt.plot(execution_times_1shard[query], label='execution_times_1shard')
        plt.plot(execution_times_2shard[query], label='execution_times_2shard')
        plt.plot(execution_times_3shard[query], label='execution_times_3shard')
        plt.plot(execution_times_4shard[query], label='execution_times_4shard')
        plt.plot(execution_times_5shard[query], label='execution_times_5shard')
        plt.plot(execution_times_5shard[query], label='execution_times_6shard')

        plt.title(f'Comparaison des valeurs de {query}')
        plt.xlabel('Index')
        plt.ylabel('Valeur')
        plt.legend()
         # Save the figure to a file
        filename = f'static/{query}_times.png'
        plt.savefig(filename)
        plt.close()  # Close the figure to free up memory
    return [url_for('static', filename=f'{query}_times.png') for query in queries]



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
    df = pd.DataFrame(result)
    return df

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


def query_rda1():
    pipeline = [
        {"$match": {"gender": "F", "departments.is_manager": True}},
        {"$project": {
            "hireYear": {"$year": {"$dateFromString": {"dateString": "$hire_date"}}}
        }},
        {"$group": {
            "_id": "$hireYear",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
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
    return render_template('simple.html', requete_nb="Requete Ru 1: Afficher le nombre d'employés par sexe et par département.", result=html_data)

@app.route('/ru2')
def ru2():
    df = query_ru2()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Ru 2:Afficher la liste des employés nés après 1963 et le nom de leur département.	", result=html_data)


@app.route('/ru3')
def ru3():
    df = query_ru3()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Ru 3:Afficher les détails des employés qui ont quitté l'entreprise.", result=html_data)


@app.route('/ru6')
def ru6():
    df = query_ru6()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Ru6:Donner les noms  des trois employés les mieux payés du département 'Finances'", result=html_data)


@app.route('/rda1')
def rda1():
    df = query_rda1()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda1:Afficher le nombre de femmes managers par année d'embauche", result=html_data)

@app.route('/rda4')
def rda4():
    df = query_rda4()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda4:Pour chaque département, affichez les statistiques sur les employés et les salaires.", result=html_data)

@app.route('/rda5')
def rda5():
    df = query_rda5()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda5:Comparer les salaires moyens dans les services de vente et de marketing	", result=html_data)

@app.route('/rda6')
def rda6():
    df = query_rda6()
    html_data = df.to_html(classes="table table-striped table-bordered", index=False)
    return render_template('simple.html', requete_nb="Requete Rda6: Dresser la liste des employés du département Production", result=html_data)

@app.route('/adminView', methods=("POST", "GET"))
def adminView():
    infos = db.command("collstats", "employees")
    indexes_existants = list(infos["indexSizes"].keys())
    nb_shards = len(infos["shards"])
    nb_chunks = infos["nchunks"]
    stats = []
    plots_urls=plot_times(queries)
    for k, v in infos["shards"].items():
        stats.append({
            "Nom du Shard": k,
            "Nombre de documents": infos["shards"][k]["count"],
            "Taille des données stockées": str(round(infos["shards"][k]["size"] * 1e-6, 2)) + " Mo",
            "Pourcentage des données stockées": str(round(infos["shards"][k]["size"] / infos["size"] * 100, 2)) + "%"
        })
    stats = pd.DataFrame(stats).sort_values("Nom du Shard")
    
    return render_template('admin.html',
                           indexes_existants=indexes_existants,
                           nb_shards=nb_shards,
                           nb_chunks=nb_chunks,
                           result=stats.to_html(classes="table table-striped table-bordered",index=False), plot_urls=plot_urls)



if __name__ == '__main__':
    app.run(debug=True)

import psycopg2

db_name = 'testdb'
db_user = 'postgres'
db_pass = 'postgres'
db_host = 'db'
db_port = '5432'

def get_column_names(cursor):
    return [desc[0] for desc in cursor.description]

def get_connections(db_name, db_user, db_pass, db_host, db_port):
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
    conn = psycopg2.connect(db_string)
    cursor = conn.cursor()

    return cursor

def get_data(table_name):
    # Retrieve the last number inserted inside the 'numbers'
    cursor = get_connections(db_name, db_user, db_pass, db_host, db_port)
    cursor.execute(f"select * from {table_name}")
    columns = get_column_names(cursor)
    data = cursor.fetchall()
    return data

def calc_index_mass(data):
    print("--------------------")
    print("user_id |", "index_mass")
    print("--------------------")
    for i in data:
        print(str(i[0]) + "       |", round(i[1]/((i[2]/100)**2), 2))

if __name__ == '__main__':
    print('Application started')
    calc_index_mass(get_data("index_mass"))

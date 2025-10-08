import json
import psycopg2
import os

#dbname = os.environ['DB_NAME']
user = os.environ['DB_USER']
password = os.environ['DB_PASSWORD']
host = os.environ['DB_HOST']
port = os.environ.get('DB_PORT', '5432')  
    
    
def lambda_handler(event, context):
    
    parameters = event.get('queryStringParameters')
    sql_query = parameters.get("query")
    timestamp_column = parameters.get("time_column")
    dbname = parameters.get("db_name")
    
    try:
        result = execute_sql_query(sql_query, dbname)
        
        for data in result:
            if timestamp_column in data:
                data.update(format_date(data, timestamp_column))
            
            # Solo convierte decimales a floats si el query es un SELECT
            if not (sql_query.strip().upper().startswith("INSERT") or sql_query.strip().upper().startswith("UPDATE")):
                data = convert_decimals_to_floats(data)

        
        response = {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    
    except Exception as e:
        response = {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    
    return response


def execute_sql_query(query, dbname):
    
    conn = None
    cur = None
    result = []
    
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        cur = conn.cursor()
        
        if query.strip().upper().startswith('INSERT') or query.strip().upper().startswith('UPDATE'):
            # La consulta es un INSERT
            cur.execute(query)
            conn.commit()  # Realizar commit para aplicar los cambios en la base de datos
            result = "Inserción exitosa"
        else:
            cur.execute(query)
            
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            
            for row in rows:
                result.append(dict(zip(columns, row)))
                
    except psycopg2.Error as e:
        raise Exception(f"Error al ejecutar la consulta: {e}")
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
    print(result)
    return result


def format_date(row, timestamp_column):
    formatted_row = dict(row)
    timestamp = formatted_row.get(timestamp_column)
    if timestamp:
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        formatted_row[timestamp_column] = formatted_timestamp
    return formatted_row
    

def convert_decimals_to_floats(row):
    """
    Revisa si alguno de los valores en el diccionario es un decimal y lo convierte a float.
    """
    for key, value in row.items():
        if isinstance(value, (int, float)):
            continue  # No necesita conversión
        try:
            row[key] = float(value)
        except (ValueError, TypeError):
            pass  # Si no es convertible a float, sigue con el siguiente
    
    return row
import json
from datetime import datetime, timezone, timedelta


def convertir_valores_fecha(data):
    """
    Convierte valores tipo timestamp (en milisegundos) a formato legible
    solo si la clave contiene la palabra 'fecha' (insensible a mayúsculas).
    Interpreta el timestamp como UTC y lo convierte a UTC-5.
    Funciona de forma recursiva para dicts y listas.
    """

    def convertir_fecha(key, valor):
        try:
            if "fecha" in key.lower():
                if isinstance(valor, (int, float)) or (isinstance(valor, str) and valor.isdigit()):
                    timestamp = int(valor) / 1000  # convertir a segundos
                    # Interpretar en UTC y convertir a UTC-5
                    dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    #dt_utc_minus_5 = dt_utc.astimezone(timezone(timedelta(hours=-5)))
                    #return dt_utc_minus_5.strftime("%Y-%m-%d %H:%M:%S")
                    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Error al convertir fecha ({key}): {e}")
        return valor

    if isinstance(data, dict):
        nuevo_dict = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                nuevo_dict[k] = convertir_valores_fecha(v)
            else:
                nuevo_dict[k] = convertir_fecha(k, v)
        return nuevo_dict

    elif isinstance(data, list):
        return [convertir_valores_fecha(v) for v in data]

    else:
        return data


def build_bulk_upsert_sql(table_name: str, rows: list[dict], conflict_key: str = "PARENT_ID") -> str:
    """
    Construye un SQL con múltiples VALUES y un único ON CONFLICT.
    """
    if not rows:
        return ""

    # Asegurar que todas las filas tengan las mismas columnas
    columns = list(rows[0].keys())
    col_names = ", ".join([f'"{c}"' for c in columns])

    values_sql = []
    for row in rows:
        vals = []
        for c in columns:
            v = row.get(c, "NULL")
            if str(v).upper() == "NULL":
                vals.append("NULL")
            elif isinstance(v, str):
                vals.append(f"'{v}'")
            else:
                vals.append(str(v))
        values_sql.append(f"({', '.join(vals)})")

    update_clause = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in columns if c != conflict_key]
    )

    sql = f"""
    INSERT INTO "{table_name}" ({col_names})
    VALUES
        {', '.join(values_sql)}
    ON CONFLICT ("{conflict_key}")
    DO UPDATE SET
        {update_clause};
    """.strip()
    #print("SQL ",sql )
    return sql


def db_upsert_capa_principal(json_data):
    #print(json_data)
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    puntos_capa_principal_fields = ["GlobalID","TIPO_PUNTO","FECHA_CREACION","FECHA_EDICION","CIRCUITO_ACU","SUBCIRCUIT_ACU",
                                "CUENCA_ALC","FID_ELEM","DIRECCION_ACU","CODIGO_CAJA_ACU","x","y","PUNTO_EXISTENTE","IPID_ELEM_ACU",
                                "IPID_ALC","FASE_INICIAL","VARIABLE_A_MEDIR","HABILITADO_FASE3"]

    all_rows = []

    
    #Caso: dict con múltiples claves
    for key, items in json_data.items():
        try:
            for item in items:  # porque item es una lista con objetos
                payload = item.get("payload", {})
                attributes = payload  # tus campos están dentro de payload

                attributes = convertir_valores_fecha(attributes)
                capa_principal_values = {}

                for field in puntos_capa_principal_fields:
                    value = attributes.get(field, None)
                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        value = str(value)
                    capa_principal_values[field] = value

                all_rows.append(capa_principal_values)
        except Exception as e:
            print(f"Error procesando registro {key}: {e}")


    upsert_sql = build_bulk_upsert_sql("puntos_capa_principal", all_rows, "GlobalID")

    #print("upsert capa principal", upsert_sql)

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }

    return payload_db


def db_update_habilitado_fase3(parents_relation):
    #Toma los global Id que fueron actualizados: 
    global_id = [item["padre"] for item in parents_relation]

    # Convertirlos a formato ('', '', '') para postgress
    global_ids_tupla = tuple(global_id)

    """
    Actualiza HABILITADO_FASE3 en puntos_capa_principal usando:
    1. habilitado_medicion de fase_1 (prioridad)
    2. si fase_1 es 0, NULL o vacío → usar habilitado_medicion de fase_2
    """

    update= """
            UPDATE puntos_capa_principal p
            SET HABILITADO_FASE3 = 
                CASE
                    WHEN f1.habilitado_medicion = 1 THEN 1
                    WHEN COALESCE(NULLIF(f1.habilitado_medicion, ''), '0')::integer = 0
                        THEN f2.habilitado_medicion
                    ELSE NULL
                END
            FROM fase_1 f1
            LEFT JOIN fase_2 f2 ON p.GlobalID = f2.PARENT_ID
            WHERE p.GlobalID = f1.PARENT_ID
            AND p.GlobalID IS NOT NULL;

    """
        # Query para actualizar
    update_sql = f"""
            UPDATE puntos_capa_principal p
            SET "HABILITADO_FASE3" = f.habilitado_medicion
            FROM fase_1 f
            WHERE p."GlobalID" = f."PARENT_ID"
            AND f.habilitado_medicion IS NOT NULL
            AND p."GlobalID" IN {global_ids_tupla};


    """

    #print("update_habilitado_fase3:", update_sql)

    payload_db = {
        "queryStringParameters": {
            "query": update_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION"
        }
    }

    return payload_db

    

import json
from DB_capa_principal import build_bulk_upsert_sql, convertir_valores_fecha



def db_upsert_fase_1(json_data):

    #verifica si es string
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_1_fields = [
        "PARENT_ID", "TIPO_PUNTO", "FECHA_CREACION", "FECHA_EDICION",
        "condicion_fisica_general", "conexiones_hidraulicas", "UBICACION_GEO_CRITICA",
        "habilitado_medicion", "UBICACION_ACTUALIZADA", "SENAL_4G",
        "REQUIERE_INST_TAPA_ACU", "REQ_LIMPIEZA_ACU", "FID_ELEM",
        "REQUIERE_FASE1", "PUNTO_ENCONTRADO", "EXPOSICION_FRAUDE", "FECHA_FASE1"
    ]

    all_rows = []

    
    #Caso: dict con múltiples claves
    for key, items in json_data.items():
        try:
            # Si cada clave tiene una lista de objetos, iterar sobre ella
            if isinstance(items, list):
                iterable = items
            else:
                iterable = [items]

            for item in iterable:
                payload = item.get("payload", {})
                attributes = payload.get("attributes", payload)  # usar payload directo si no hay 'attributes'

                attributes = convertir_valores_fecha(attributes)
                fase_1_values = {}

                for field in fase_1_fields:
                    value = attributes.get(field, None)
                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        # Normalizar valores tipo Sí/No -> 1/0
                        if field not in ["REQUIERE_FASE1", "PUNTO_ENCONTRADO"]:
                            if isinstance(value, str):
                                if value.strip().lower() == "si":
                                    value = 1
                                elif value.strip().lower() == "no":
                                    value = 0
                        value = str(value)
                    fase_1_values[field] = value

                # condicion_fisica_general
                if (
                    attributes.get("SIGNOS_DESGASTE_ACU") in ["Si", 1] 
                    or attributes.get("DANOS_ESTRUCT_ACU") in ["Si", 1]
                    or attributes.get("TAPA_ASEGURADA_ACU") in ["No", 0]
                ):
                    fase_1_values["condicion_fisica_general"] = 0
                    
                elif( attributes.get("SIGNOS_DESGASTE_ACU") in ["No", 0]
                    and attributes.get("DANOS_ESTRUCT_ACU") in ["No", 1]
                ):
                    
                    fase_1_values["condicion_fisica_general"] = 1

                # conexiones_hidraulicas 
                if (attributes.get("ESTADO_OPTIMO_CON_HID_ACU") in ["No", 0] or
                    attributes.get("ESTADO_ADECUADO_TUBERIA_ACU") in ["No", 0] or
                    attributes.get("VALVULA_FUNCIONAL_ACU") in ["No", 0] or
                    attributes.get("PRESENTA_FUGAS_ACU") in ["Si", 1] or
                    attributes.get("FLUJO_DE_AGUA_ACU") in ["No", 0] or
                    attributes.get("VERIFICA_CONEX_ROSCADA_ACU") in ["No", 0] or
                    attributes.get("CUMPLE_MEDIDAS_MIN_MED_CAU_ACU") in ["No", 0]):
                    fase_1_values["conexiones_hidraulicas"] = 0
                    
                elif(attributes.get("ESTADO_OPTIMO_CON_HID_ACU") in ["Si", 1] and
                    attributes.get("ESTADO_ADECUADO_TUBERIA_ACU") in ["Si", 1] and
                    attributes.get("VALVULA_FUNCIONAL_ACU") in ["Si", 1] and
                    attributes.get("PRESENTA_FUGAS_ACU") in ["No", 0] and
                    attributes.get("FLUJO_DE_AGUA_ACU") in ["Si", 1] and
                    attributes.get("VERIFICA_CONEX_ROSCADA_ACU") in ["Si", 1] ):
                    fase_1_values["conexiones_hidraulicas"] = 1

                # habilitado_medicion
                if (
                    attributes.get("PUNTO_REQUIERE_FASE2") in ["Si", 1]
                    and attributes.get("PUNTOS_HABILITADO_FASE3") in ["No", 0]
                ):
                    fase_1_values["habilitado_medicion"] = 0
                elif (
                    attributes.get("PUNTOS_HABILITADO_FASE3") in ["Si", 1]
                    and attributes.get("PUNTO_REQUIERE_FASE2") in ["No", 0]
                ):
                    fase_1_values["habilitado_medicion"] = 1

                all_rows.append(fase_1_values)

        except Exception as e:
            print(f"Error procesando registro {key}: {e}")

    upsert_sql = build_bulk_upsert_sql("fase_1", all_rows, "PARENT_ID")

    print("upsert_fase_1", upsert_sql)

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    return payload_db

    

import requests, psycopg2, os
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import Variable

def create_tables():
    """
    Crea la tabla en la base de datos en caso de que no exista
    """
    create_table = """
            CREATE TABLE IF NOT EXISTS arturomontesdeoca4892_coderhouse.informacion_geografica_mexico
            (id_estado VARCHAR(5) NOT NULL PRIMARY KEY,
            nombre VARCHAR(20) NOT NULL,
            temperatura NUMERIC(15,2) NOT NULL,
            indice_uv NUMERIC(15,2) NOT NULL,
            prob_precipitacion NUMERIC(15,2) NOT null,
            humedad NUMERIC(15,2) not null,
            velocidad_viento_Kmph NUMERIC(15,2) not null,
            direccion_viento VARCHAR(5) not null,
            descripcion VARCHAR(100) not null,
            zona_cercana VARCHAR(60) not null,
            iluminacion_lunar NUMERIC(15,2) not null,
            fase_lunar VARCHAR(30) not null,
            latitud NUMERIC(15,2),
            longitud NUMERIC(15,2),
            sismo INT,
            magnitud NUMERIC(5,2),
            lugar VARCHAR(50),
            hora TIMESTAMP,
            url_detalle VARCHAR(100),
            info_date DATE not null);
            """
    conn = psycopg2.connect(Variable.get("secret_redshift_string_connection"))
    cur = conn.cursor()
    cur.execute(create_table)
    conn.commit()
    conn.close()


def weather_info(city):
    """
    Extrae la informacion de los datos de la ciudad establecida

    args:
        city (str) : Ciudad objetivo para extraer datos climaticos del momento
    
    return:
        result (tuple) : Contiene la información climatica de la ciudad establecida
    """
    url = f"https://es.wttr.in/{city}?format=j1"
    response = requests.get(url)
    if response.status_code == 200:
        weather = response.json()
        temperature = int(weather["current_condition"][0]["temp_C"])
        uv_index = int(weather["current_condition"][0]["uvIndex"])
        prob_precipitation = float(weather["current_condition"][0]["precipMM"])
        humidity = float(weather["current_condition"][0]["humidity"])
        wind_speed_Kmph = int(weather["current_condition"][0]["windspeedKmph"])
        direction_wind = weather["current_condition"][0]["winddir16Point"]
        description = weather["current_condition"][0]["lang_es"][0]["value"]
        nearest_area = weather["nearest_area"][0]["areaName"][0]["value"]
        moon_illumination = float(weather["weather"][0]["astronomy"][0]["moon_illumination"])
        moon_phase = weather["weather"][0]["astronomy"][0]["moon_phase"]
        latitude = weather["nearest_area"][0]["latitude"]
        longitude = weather["nearest_area"][0]["longitude"]

        result = (temperature, uv_index, prob_precipitation, humidity,
                wind_speed_Kmph, direction_wind, description, 
                nearest_area, moon_illumination, moon_phase, latitude, longitude)
        
        return result
    else:
        print(f'Error al conectar con la API. Código de error: {response.status_code}')


def seismological_data(place, altitude, longitude):
    """
    Busca si se registro un sismo durante el día en las coordenadas establecidas y en un radio de 100 km

    args:
        place (str) : Ciudad objetivo 
        altitude (float) : Altitud
        longitude (float) : Longitud
    
    return:
        result (tuple) : Contiene la información sismica
    """
    yestarday_date = (datetime.now()).strftime('%Y-%m-%d')
    maxradiuskm = Variable.get("maxradiuskm")
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={yestarday_date}T00:00:00-06:00&endtime={yestarday_date}T23:59:59-06:00&minmagnitude=0&latitude={altitude}&longitude={longitude}&maxradiuskm={maxradiuskm}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        info_finded = []
        
        if data['metadata']['count'] > 0:
            for earthquake in data['features']:
                properties = earthquake['properties']
                magnitude = round(properties['mag'],1)
                location = properties['place']
                timestamp = datetime.fromtimestamp(properties['time'] / 1000.0)
                details = properties['url']
                info = (1, magnitude, location, timestamp, details)
                info_finded.append(info)
                print("Magnitud:", magnitude)
                print("Ubicación:", location)
                print("Hora:", timestamp)
                print("URL para más detalles:", details)
                print("-" * 30)

            return info_finded
        else:
            print(f"No se encontraron terremotos en {place}, México, el {yestarday_date}.")
            return [(0 ,None , None, None, None)]
    else:
        print("Error al consultar los datos. Código de respuesta:", response.status_code)

def insert_weather_info():
    """
    Procedimiento principal que obtiene la informacion climatica de todos los estados de México
    con esa informacion consulta otra api de sismologia para revisar hay algun sismo registrado para cada estado
    """
    estados_mexico = [
        ("AGS", "Aguascalientes"),
        ("BC", "Baja California"),
        ("BCS", "Baja California Sur"),
        ("CAM", "Campeche"),
        ("COAH", "Coahuila"),
        ("COL", "Colima"),
        ("CHIS", "Chiapas"),
        ("CHIH", "Chihuahua"),
        ("CDMX", "Ciudad de México"),
        ("DGO", "Durango"),
        ("GTO", "Guanajuato"),
        ("GRO", "Guerrero"),
        ("HGO", "Hidalgo"),
        ("JAL", "Jalisco"),
        ("MEX", "México"),
        ("MICH", "Michoacán"),
        ("MOR", "Morelos"),
        ("NAY", "Nayarit"),
        ("NL", "Nuevo León"),
        ("OAX", "Oaxaca"),
        ("PUE", "Puebla"),
        ("QRO", "Querétaro"),
        ("QROO", "Quintana Roo"),
        ("SLP", "San Luis Potosí"),
        ("SIN", "Sinaloa"),
        ("SON", "Sonora"),
        ("TAB", "Tabasco"),
        ("TAMPS", "Tamaulipas"),
        ("TLAX", "Tlaxcala"),
        ("VER", "Veracruz"),
        ("YUC", "Yucatán"),
        ("ZAC", "Zacatecas")
    ]

    all_dates = []

    # Se obtiene la información de cada estado y se agrega a la lista all_dates
    for estado in estados_mexico:
        # Llmado de la primera API
        weather_data = weather_info(estado[1])
        # --------------------------------------

        # Llmado de la segunda API usando informacion de la llamada anterior
        sismic_data = seismological_data(estado[1], weather_data[10], weather_data[11])
        # --------------------------------------

        if len(sismic_data) > 1:
            for record in sismic_data:
                all_dates.append(estado + weather_data + record)
        else:
            all_dates.append(estado + weather_data + sismic_data[0])

    # Se construye el dataframe y se agrega la columna info_date con la fecha del momento de ejecución
    df = pd.DataFrame(all_dates, columns=["id_estado", "nombre", "temperatura", "indice_uv",
                                    "prob_precipitacion", "humedad", "velocidad_viento_kmph",
                                    "direccion_viento", "descripcion", "zona_cercana",
                                    "iluminacion_lunar", "fase_lunar", "latitud", "longitud",
                                    "sismo", "magnitud", "lugar", "hora", "url_detalle"])
    date_str = datetime.now().strftime("%Y-%m-%d")
    df["info_date"] = date_str
    df = df.fillna("")
    df = df.drop_duplicates()


    # Se establace la conexión a la base de datos
    conn = psycopg2.connect(Variable.get("secret_redshift_string_connection"))
    # Se convierte el dataframe a una lista de tuplas donde cada una de ellas contiene los valores de cada registro 
    # ej. = ('AGS', 'Aguascalientes', 29, 7, 0., 29., 19, 'E', 'Parcialmente nublado', 'El Taray', 99., '99', '2023-08-01 14:12:58')
    records = df.to_records(index=False)
    data_tuples = list(records)
    # Se obtiene la lista de columnas
    cols = ', '.join(df.columns)
    cursor = conn.cursor()

    # Se itera sobre cada registro y se construye un query para insertar el registro en la base de datos
    for record in data_tuples:
        date_str = datetime.now().strftime("%Y-%m-%d %X")
        record = str(record).replace("''", "NULL").replace("NaT", date_str)
        query = f"INSERT INTO informacion_geografica_mexico ({cols}) VALUES {record}"
        print(query)
        cursor.execute(query, data_tuples)
        conn.commit()

    print("Dataframe insertado en la base de datos")
    # Cerrar el cursor y la conexión
    conn.close()
    cursor.close()

def chek_seismological():
    """
    Funcion que revisa si se registro algun sismo en la insercion

    return
        (list)
    """
    conn = psycopg2.connect(Variable.get("secret_redshift_string_connection"))
    date_str = datetime.now().strftime("%Y-%m-%d")
    query = f"SELECT * FROM arturomontesdeoca4892_coderhouse.informacion_geografica_mexico WHERE info_date = '{date_str}' and magnitud IS NOT NULL"
    df = pd.read_sql_query(query, conn)
    conn.commit()
    conn.close()
    if df.shape[0] >= 1:
        df.to_csv(f"sismos_registrados_{date_str}.csv")
        return ["Send_alert"]
    else:
        return ["No_alert"]


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def send_email():
    """
    Procedimiento que envia un correo de alerta con un archivo adjunto
    """
    subject = "Alerta! Sismos encontrados"
    body = """<h1"> 
                    Se han detectado sismos y la información se encuentra 
                    en el archivo adjunto
              </h1>"""
    sender = "arturomontesdeoca4892@gmail.com"
    recipients = Variable.get("destinatarios").strip().split(',') + ["mmao100087@gmail.com"]
    # password = "qwwlilhfsrfafeyw"
    password = Variable.get("email_secret_key")
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    part2 = MIMEText(body, 'html')
    msg.attach(part2)

    date_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"sismos_registrados_{date_str}.csv"

    with open(file_name, "rb") as attachment:
        part3 = MIMEApplication(attachment.read())
        part3.add_header("Content-Disposition", "attachment", filename=file_name)
        msg.attach(part3)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())

    if os.path.exists(file_name):
        os.remove(file_name)
    
    print(f"Message sent! to: ", recipients)

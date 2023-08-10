CREATE TABLE IF NOT EXISTS arturomontesdeoca4892_coderhouse.clima_mexico
            (id_estado VARCHAR(5) NOT NULL PRIMARY KEY,
            nombre VARCHAR(20) NOT NULL,
            temperatura NUMERIC(15,2) NOT NULL,
            uv_index NUMERIC(15,2) NOT NULL,
            prob_precipitacion NUMERIC(15,2) NOT null,
            humedad NUMERIC(15,2) not null,
            velocidad_viento_Kmph NUMERIC(15,2) not null,
            direccion_viento VARCHAR(5) not null,
            descripcion VARCHAR(100) not null,
            zona_cercana VARCHAR(60) not null,
            iluminacion_lunar NUMERIC(15,2) not null,
            fase_lunar VARCHAR(30) not null,
            info_date timestamp not null);commit;

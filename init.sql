DROP TABLE IF EXISTS fact_measure;
DROP TABLE IF EXISTS dim_metric;
DROP TABLE IF EXISTS dim_station;

CREATE TABLE dim_metric (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL
);

CREATE TABLE dim_station (
    station_id SERIAL PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    longitude DECIMAL(9, 7) NOT NULL,
    latitude DECIMAL(9, 7) NOT NULL
);

-- Crear la tabla de hechos (measure)
CREATE TABLE fact_measure (
    measure_id SERIAL PRIMARY KEY,
    metric_id INT NOT NULL,
    station_id INT NOT NULL,
    measure DECIMAL(10, 2) NOT NULL,
    date TIMESTAMP NOT NULL,
    FOREIGN KEY (metric_id) REFERENCES dim_metric (metric_id),
    FOREIGN KEY (station_id) REFERENCES dim_station (station_id)
);


INSERT INTO dim_metric (metric_name) VALUES 
    ('Dióxido de Azufre SO2'),
    ('Monóxido de Carbono CO'),
    ('Monóxido de Nitrógeno NO'),
    ('Dióxido de Nitrógeno NO2'),
    ('Partículas < 2.5 µm PM2.5'),
    ('Partículas < 10 µm PM10'),
    ('Óxidos de Nitrógeno NOx'),
    ('Ozono O3'),
    ('Tolueno TOL'),
    ('Benceno BEN'),
    ('Etilbenceno EBE'),
    ('Metaxileno MXY'),
    ('Paraxileno PXY'),
    ('Ortoxileno OXY'),
    ('Hidrocarburos totales (hexano) TCH'),
    ('Metano CH4'),
    ('Hidrocarburos no metánicos (hexano) NMHC');

INSERT INTO dim_station (station_name, district_name, longitude, latitude) VALUES 
    ('Plaza de España', 'Moncloa - Aravaca', 3.7122567, 40.4238823),
    ('Escuelas Aguirre', 'Barrio de Salamanca', 3.6823158, 40.4215533),
    ('Ramón y Cajal', 'Chamartín', 3.6773491, 40.4514734),
    ('Arturo Soria', 'Ciudad Lineal', 3.6392422, 40.4400457),
    ('Villaverde', 'Villaverde', 3.7133167, 40.347147),
    ('Farolillo', 'Carabanchel', 3.7318356, 40.3947825),
    ('Casa de Campo', 'Moncloa - Aravaca', 3.7473445, 40.4193577),
    ('Barajas Pueblo', 'Barajas', 3.5800258, 40.4769179),
    ('Plaza del Carmen', 'Centro', 3.7031662, 40.4192091),
    ('Moratalaz', 'Moratalaz', 3.6453104, 40.4079517),
    ('Cuatro Caminos', 'Tetuán', 3.7071303, 40.4455439),
    ('Barrio del Pilar', 'Fuencarral - El Pardo', 3.7115364, 40.4782322),
    ('Vallecas', 'Puente de Vallecas', 3.6515286, 40.3881478),
    ('Méndez Álvaro', 'Arganzuela', 3.6868138, 40.3980991),
    ('Castellana', 'Chamartín', 3.6903729, 40.4398904),
    ('Parque del Retiro', 'Retiro', 3.6824999, 40.4144444),
    ('Plaza Castilla', 'Chamartín', 3.6887449, 40.4655841),
    ('Ensanche de Vallecas', 'Villa de Vallecas', 3.6121394, 40.3730118),
    ('Urb. Embajada', 'Barajas', 3.5805649, 40.4623628),
    ('Plaza Elíptica', 'Usera', 3.7187679, 40.3850336),
    ('Sanchinarro', 'Hortaleza', 3.6605173, 40.4942012),
    ('El Pardo', 'Fuencarral - El Pardo', 3.7746101, 40.5180701),
    ('Juan Carlos I', 'Barajas', 3.6163407, 40.4607255),
    ('Tres Olivos', 'Fuencarral - El Pardo', 3.6897308, 40.5005477);
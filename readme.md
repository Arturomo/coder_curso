<!-- PROJECT POD -->
<br />
<p align="center">
  <h3 align="center"> Informacion climatica </h3>
</p>


<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table de contenido</summary>
  <ol>
    <li>
      <a href="#Objetivo">Objetivo</a>
      <ul>
        <li><a href="#Descripción">Descripción</a></li>
      </ul>
    </li>
    <li><a href="#Airflow graph">Airflow graph</a></li>
    <li><a href="#Variables de Airflow">Variables de Airflow</a></li>
    <li><a href="#Deployment">Deployment</a></li>
  </ol>
</details>

# Objetivo
Extraer diariamente la informacion climatica de todos los estados de la republica mexicana e insertarla en una base de datos **AWS Redshift**
 
## Descripción
Primero se crea tabla **clima_mexico** si no existe en la base de datos. Posteriormente se extrae la informacion climatica de cada estado de la Republica Mexicana y se agrega a una **Pandas Dataframe**.
Posteriormente el dataframe se carga a la base de datos utilizando una **string connection**

# Airflow Variables

Este proceso necesita las siguientes variables de ambiente para funcionar correctamente.

```
secret_redshift_string_connection = string connection

Example:

secret_redshift_string_connection = postgresql://my_user:my_password@my_host:port/database_name
```
Esta variable se debe establecer en la interfez de usuario de Apache Airflow en la pestaña Admin opción Variables:

# Deployment
Para poder ejecutar este DAG se debe descargar el archivo docker-compose.yaml y la caroeta dags y posteriormente ejecutar los siguientes 2 comandos
```
docker-compose up airflow-init

docker-compose up
```

Posteriormente puede ingresar a la interfaz de usuario en localhost:8080

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
Extraer diariamente la informacion climatica de todos los estados de la republica mexicana, revisar si se ha registrado algun sismo para complementar la informacion e insertarla en una base de datos **AWS Redshift**
 
## Descripción
Primero se crea tabla **informacion_geografica_mexico** si no existe en la base de datos. Posteriormente se extrae la informacion climatica de cada estado de la Republica Mexicana. Con la información geografica se cunsulta si ha habido un sismo registrado en sus arrededores para completar la información y se agrega a una **Pandas Dataframe**.
Posteriormente el dataframe se carga a la base de datos utilizando una **string connection**. Finalmente otro proceso revisa si hay algun registro de sismo para ese dia para enviar una alarmapor email asi como la información de sismo o no enviar la alarma.

# Airflow Variables

Este proceso necesita las siguientes variables de ambiente para funcionar correctamente.

```
secret_redshift_string_connection = string connection
destinatarios = Correos a los que se va a enviar el correo
email_secret_key = string
maxradiuskm = numero que representa el radio en km que deseamos monitorear alrrededor del area objetivo

Example:

secret_redshift_string_connection = postgresql://my_user:my_password@my_host:port/database_name
destinatarios = mail1@example.com,mail2@example.com, . . .,mailn@example.com,
email_secret_key = my_secret
maxradiuskm = 1000
```
Esta variable se debe establecer en la interfez de usuario de Apache Airflow en la pestaña Admin opción Variables:

# Deployment
Para poder ejecutar este DAG se debe descargar el archivo docker-compose.yaml y la caroeta dags y posteriormente ejecutar los siguientes 2 comandos
```
docker-compose up airflow-init

docker-compose up
```

Posteriormente puede ingresar a la interfaz de usuario en localhost:8080

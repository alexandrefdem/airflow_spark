# ETL Project
## Alexandre de Magalhães

The proposed ETL solutions aims to fulfill the requirements for the project. It features a Airflow distributed orchestration tool with access to a spark instance for the three following tasks.

- Extract data from B2B database and deliver to a known data warehouse
- Generate and Parse weblogs (with Spark) and deliver to a known data warehouse
- Extract marketing leads data from a spreadsheet and deliver to a known data warehouse

## Requirements

- Docker
- Docker Compose
- Windows (not tested on linux distributions)
- At least 8 core 16 gb ram machine 
- Ports 6379, 8080, 8081, 8082, 5555, 7077, 8083 free on host machine

## Run
Execute the following command on docker-compose.yml directory
```sh
docker-compose up -d --build
```
## Usage
Please refer to the Docs folder 

## Credentials
Most of the time all the work can be supervised through airflow web interface. 

| Interface | Login | password | password |
| ------ | ------ |  ------  |  ------  |
| Airflow Webserver | airflow | airflow |  http://localhost:8080  |
| SQL Server  | sa | toptal |  localhost,1533 |
| Spark Master  | n/a | n/a |  http://localhost:8000  |

## License

MIT




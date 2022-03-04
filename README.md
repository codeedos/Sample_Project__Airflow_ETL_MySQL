This project uses Airflow DAGs to simulate data migration from one database to another database in MySQL

Steps to run this project:

- Install MySQL, MySQL Workbench, Docker, and Airflow
- Launch MySQL server on localhost
- Use codes in MySQL folder to create the databases and tables in MySQL Workbench.
- Use codes in GenerateData folder to generate weblog and B2B data, and insert them into MySQL tables.
- Launch Docker Desktop, and then inside the project's AirflowDocker folder run "docker-compose up airflow-init" and then "docker-compose up" to launch Airflow
- Run the DAGs to extract data from B2B and WebServer databases in MySQL, transform and load them into Target database in MySQL.

> > > > > > > f6dab04c7633f6e15177d9e9992a1e584f1b68c7

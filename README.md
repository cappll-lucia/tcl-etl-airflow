# Student Course DATA Migration

This project automates the migration of historical student course data stored in 46.000 `.xlsx` files into a structured PostgreSQL database.

## Tools & Technologies

- **Apache Airflow** – for orchestration and workflow scheduling  
- **Astronomer** – for deploying and executing Airflow pipelines in a managed environment  
- **Google Cloud Storage** – for hosting input Excel files and storing processed artifacts  
- **Python** – for file parsing and data transformation  
- **Pandas** – for reading and processing Excel files  
- **PostgreSQL** – as the target relational database  
- **Docker** – for containerized development and deployment

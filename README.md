# Pyrrha rules and decision engine

[![License](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Slack](https://img.shields.io/static/v1?label=Slack&message=%23prometeo-pyrrha&color=blue)](https://callforcode.org/slack)

This repository contains the [Pyrrha](https://github.com/Pyrrha-Platform/Pyrrha) solution application for determining thresholds in toxin exposure measured by the [sensor device](https://github.com/Pyrrha-Platform/Pyrrha-Firmware) and sent to the cloud from the Samsung [smartphone](https://github.com/Pyrrha-Platform/Pyrrha-Mobile-App) carried by the firefighters.

This service wakes up every minute and calculates time weighted average exposures for all fire fighters and compares them to the configured limits.

## Contents

- [Contents](#contents)
- [Prerequisites](#prerequisites)
- [Running MariaDB](#running-mariadb)
- [Run locally with Python](#run-locally-with-python)
- [Run locally with Docker](#run-locally-with-docker)
- [Run on Kubernetes](#run-on-kubernetes)
- [Troubleshooting](#troubleshooting)
- [Built with](#built-with)
- [Contributing](#contributing)
- [License](#license)

## Prerequisites

1. [Docker](https://docs.docker.com/desktop/)
2. [Docker-Compose](https://docs.docker.com/compose/)
3. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
4. [Helm](https://helm.sh/docs/intro/install/)
5. [Skaffold](https://skaffold.dev/docs/install/)
6. [IBM CLI](https://cloud.ibm.com/docs/cli?topic=cli-install-ibmcloud-cli)

## Running MariaDB

MariaDB is a prerequisite for running the rules and decision engine service. Follow the instructions in [Pyrrha-Database repository](https://github.com/Pyrrha-Platform/Pyrrha-Database) to build and run MariaDB locally using Docker. The following steps assume you have MariaDB running locally as a standalone service or as a docker container.

## Run locally with Python

1. Copy `src/.env.example` to `src/.env` and fill out with MariaDB credentials that you set up in the previous step.
   ```
   MARIADB_HOST=localhost
   MARIADB_PORT=3306
   MARIADB_USERNAME=root
   MARIADB_PASSWORD=${MDB_PASSWORD}
   MARIADB_DB=pyrrha
   ```
2. Create python virtual environment
   ```
   python3 -m venv python3
   ```
3. Activate virtual environment
   ```
   source python3/bin/activate
   ```
4. Install the dependencies
   ```
   pip install -r requirements.txt
   ```
5. Run the application
   ```
   python src/core_decision_flask_app.py 8080
   ```
6. You should see the following output
   ```
        starting application
        * Serving Flask app "core_decision_flask_app" (lazy loading)
        * Environment: production
        WARNING: Do not use the development server in a production environment.
        Use a production WSGI server instead.
        * Debug mode: off
           Use a production WSGI server instead.
        * Debug mode: off
        INFO:werkzeug: * Running on http://0.0.0.0:8080/ (Press CTRL+C to quit)
   ```

## Run locally with Docker

If you are running MariaDB as a docker container, we recommend you use the build file located in the [Pyrrha-Deployment-Configurations](https://github.com/Pyrrha-Platform/Pyrrha-Deployment-Configurations) repository to start the rules and decision engine service. Run the following after configuring the services as explained in [the instructions](https://github.com/Pyrrha-Platform/Pyrrha-Deployment-Configurations/blob/main/Docker_Compose.md).

   ```
   docker-compose up --build pyrrha-rulesdecision
   ```
You can stop the services with:
   ```
   docker-compose stop pyrrha-rulesdecision
   ```

If you want to run build and run this image as a docker container and not use docker-compose, you can follow these steps:

1. Build the image
   ```
   docker build . -t rulesdecision
   ```
2. Run the image using the following command. Notice we are passing in the `src/.env` file as environment variable. This will not work if MariaDB is running in a docker image as `localhost:3306` will not resolve to the right image.
   ```
   docker run -p8080:8080 --env-file src/.env -t rulesdecision
   ```
3. You should see the application logs
   ```
        starting application
        * Serving Flask app "core_decision_flask_app" (lazy loading)
        * Environment: production
        WARNING: Do not use the development server in a production environment.
        Use a production WSGI server instead.
        * Debug mode: off
        * Running on http://0.0.0.0:8080/ (Press CTRL+C to quit)
   ```
You can also use docker-compose to run all the services by using the build file located in the [Pyrrha-Deployment-Configurations](https://github.com/Pyrrha-Platform/Pyrrha-Deployment-Configurations) repository.

## Run on Kubernetes

You can run this application on Kubernetes using the helm charts provided in [Pyrrha-Deployment-Configurations](https://github.com/Pyrrha-Platform/Pyrrha-Deployment-Configurations/tree/main/k8s) repository. The [skaffold.yaml](https://github.com/Pyrrha-Platform/Pyrrha-Rules-Decision/blob/main/skaffold.yaml) file provided here let's you quickly run the application on the cluster by using [Skaffold](https://skaffold.dev/docs/pipeline-stages/deployers/helm/). There are two profiles provided. To run the solution on the `test` namespace use:
`skaffold dev -p test`

## Troubleshooting

1. Database does not connect
   1. ensure `.env` file has the correct values for database connection
2. Change the db password

## Built with

- Docker
- MariaDB
- Flask
- [IBM Kubernetes Service](https://cloud.ibm.com/kubernetes/overview)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting Pyrrha pull requests.

## License

This project is licensed under the Apache 2 License - see the [LICENSE](LICENSE) file for details.

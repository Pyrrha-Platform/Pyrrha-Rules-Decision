# Prometeo rules and decision engine

This repository contains the [Prometeo](https://github.com/Code-and-Response/Prometeo) solution application for determining thresholds in toxin exposure measured by the [sensor device](https://github.com/Code-and-Response/Prometeo-Firmware) and sent to the cloud from the Samsung [smartphone](https://github.com/Code-and-Response/Prometeo-Mobile-App) carried by the firefighters.

This service wakes up every minute and calculates time weighted average exposures for all fire fighters and compares them to the configured limits.

[![License](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Slack](https://img.shields.io/badge/Join-Slack-blue)](https://callforcode.org/slack)

## Contents
- [Prometeo rules and decision engine](#prometeo-rules-and-decision-engine)
  - [Contents](#contents)
  - [Prerequisites](#prerequisites)
  - [Run locally with Python](#run-locally-with-python)
  - [Run locally with Docker](#run-locally-with-docker)
  - [Run on Kubernetes](#run-on-kubernetes)
  - [Troubleshooting](#troubleshooting)
  - [Built with](#built-with)
  - [Contributing](#contributing)
  - [License](#license)

## Prerequisites
1. [Docker](https://docs.docker.com/desktop/)
2. [IBM CLI](https://cloud.ibm.com/docs/cli?topic=cli-install-ibmcloud-cli)
3. [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
4. [Helm](https://helm.sh/docs/intro/install/)
5. [Skaffold](https://skaffold.dev/docs/install/)

## Run locally with Python
You can run this solution locally in docker as follows

1. Set up environment variables in the `src/.env` file
2. Install mariadb locally
   1. pull mariadb from dockerhub
    ```
        docker pull mariadb
    ```
   2. run the image
    ```
        docker run -p 3306:3306 --name prometeo-mariadb -e MYSQL_ROOT_PASSWORD='' -d mariadb
    ```
    3. Test the image - TBD
3. Create python virtual environment
   ```
        python3 -m venv python3
   ```
4. Activate virtual environment
   ```
        source python3/bin/activate
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
 * Running on http://0.0.0.0:8080/ (Press CTRL+C to quit)
   ```

## Run locally with Docker
1. Build the image
    ```
        docker build . -t rulesdecision
    ```
2. Run the image
   ```
        docker run -p8080:8080 -t rulesdecision
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

## Run on Kubernetes
You can run this application on Kubernetes. The skaffold.yaml file let's you quickly run the application on the cluster by using [Skaffold](https://skaffold.dev/docs/pipeline-stages/deployers/helm/). There are two profiles provided. To run the solution on the `test` namespace use:
    ```
        skaffold dev -p test
    ```

## Troubleshooting
1. Database does not connect
   1. ensure `.env` file has the correct values for database connection
2. Change the db password

## Built with

* Docker
* MariaDB
* Flask
* [IBM Kubernetes Service](https://cloud.ibm.com/kubernetes/overview)


## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting Prometeo pull requests.

## License

This project is licensed under the Apache 2 License - see the [LICENSE](LICENSE) file for details.
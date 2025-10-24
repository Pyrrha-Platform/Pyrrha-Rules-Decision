# Pyrrha rules and decision engine

[![License](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Slack](https://img.shields.io/static/v1?label=Slack&message=%23prometeo-pyrrha&color=blue)](https://callforcode.org/slack)

This repository contains the [Pyrrha](https://github.com/Pyrrha-Platform/Pyrrha) solution application for determining thresholds in toxin exposure measured by the [sensor device](https://github.com/Pyrrha-Platform/Pyrrha-Firmware) and sent to the cloud from the Samsung [smartphone](https://github.com/Pyrrha-Platform/Pyrrha-Mobile-App) carried by the firefighters.

This service wakes up every minute and calculates time-weighted average exposures for all firefighters and compares them to the configured limits.

## Contents

- [Contents](#contents)
- [Background](#background)
- [Prerequisites](#prerequisites)
- [Running MariaDB](#running-mariadb)
- [Run locally with Python](#run-locally-with-python)
- [Run locally with Docker](#run-locally-with-docker)
- [Run on Kubernetes](#run-on-kubernetes)
- [Troubleshooting](#troubleshooting)
- [Built with](#built-with)
- [Contributing](#contributing)
- [License](#license)

## Background

In this repository you'll find a solution that goes beyond just reading the real-time parts-per-million readings that come from the sensor. The code here assesses the cumulative effect of exposure by calculates short-term exposure and time-weighted averages over 10 minute, 30 minute, 60 minute, 4 hour, and 8 hours.

The goals for this project are to:

- Present gas exposure information to firefighters in a way that is helpful and actionable
- Use standard metrics for gas exposure that align with regulations and standards
- Understand expectations across several regions (EU, US, Australia, etc)

### Understanding the terminology

An excellent top-level summary is available from [OSHA Environmental Compliance Systems](https://oecscomply.com/difference-pel-tlv-rel/).

That resource summarizes all the different standards into 3 main concepts. Generally speaking, PEL/TLV/REL have three subcategories:

1. *Time-weighted average (TWA)* - for the whole workday.
1. *Ceiling value* - should  never be exceed at any time.
1. *Short term exposure limit (STEL)* - the 10 or 15 min TWA concentration (not 8 hours).

### Other useful information

- Regulatory organizations and federal agencies establish safety and health laws and recommendations. This includes federal laws established by the Occupational Safety and Health Administration (OSHA) and NIOSH recommendations, which are called recommended exposure levels (RELs). Nongovernmental organizations, such as the American Conference of Governmental Industrial Hygienists (ACGIH) also publish threshold limit values (TLVs). Together these are called occupational exposure levels (OELs) designed to protect the health and safety of workers.
- [Wildland Firefighter Smoke Exposure](https://www.fs.fed.us/t-d/pubs/pdfpubs/pdf13511803/pdf13511803dpi100.pdf)
- [NIH glossary](https://www.ncbi.nlm.nih.gov/books/NBK219910/) that is helpful for understanding how an PEL relates to a REL or a STEL or a TWA. In the context of submarines, but still covers all the standards and what they mean.
- ACGIH and NIOSH make recommendations/guidelines. OSHA makes law: [Permissible Exposure Limits – Annotated Tables](https://www.osha.gov/dsg/annotated-pels/)
- [OSHA carbon monoxide limits](https://www.cdc.gov/niosh/pel88/630-08.html#:~:text=OSHA's%20former%20limit%20for%20carbon,with%20a%20200%2Dppm%20ceiling.)
- [Carbon monoxide limits for Spain](proposta-esquema-indicacions-i-relacio-limits-ppm-temp-humitat-prometeo.pdf), in Spanish from Joan Herrera
- [Australian limits](https://www.researchgate.net/publication/23179784_Respiratory_Irritants_in_Australian_Bushfire_Smoke_Air_Toxics_Sampling_in_a)
- [AEGL / IDLH (Immediately Dangerous to Life or Health Concentrations (IDLH))](https://www.cdc.gov/niosh/idlh/630080.html) from the CDC.
- [Honeywell personal gas monitor user manual](https://www.honeywellanalytics.com/~/media/honeywell-analytics/products/gasalertmax-xt-ii/documents/gasalertmaxxt-ii-user-manual-129541_en_b.pdf?la=en) gives a sense of how people are used to thinking about these limits

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

   ```sh
   MARIADB_HOST=localhost
   MARIADB_PORT=3306
   MARIADB_USERNAME=root
   MARIADB_PASSWORD=${MDB_PASSWORD}
   MARIADB_DB=pyrrha
   ```

2. Create python virtual environment

   ```sh
   python3 -m venv python3
   ```

3. Activate virtual environment

   ```sh
   source python3/bin/activate
   ```

4. Install the dependencies

   ```sh
   pip install -r requirements.txt
   ```

5. Run the application

   ```sh
   python src/core_decision_flask_app.py 8080

You can run this solution locally in docker as follows

1. Set up environment variables in the `src/.env` file

1. Install MariaDB locally

   1. Pull MariaDB from DockerHub

      ```sh
      docker pull mariadb 
      ```

   1. Run the image

      ```sh
      docker run --rm -p 3306:3306 --name pyrrha-mariadb -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=true -d mariadb
      ```

   1. Test the image - TBD

1. Create Python virtual environment

   ```sh
   python3 -m venv python3
   ```

1. Activate virtual environment

   ```sh
   source python3/bin/activate
   ```

1. Run the application

   ```sh
   python src/core_decision_flask_app.py 8080
   ```

   ```sh
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

1. You should see the following output

   ```sh
   starting application
   * Serving Flask app "core_decision_flask_app" (lazy loading)
   * Environment: production
   WARNING: Do not use the development server in a production environment.
   Use a production WSGI server instead.
   * Debug mode: off
   * Running on <http://0.0.0.0:8080/> (Press CTRL+C to quit)
   ```

## Run locally with Docker

If you are running MariaDB as a docker container, we recommend you use the build file located in the [Pyrrha-Deployment-Configurations](https://github.com/Pyrrha-Platform/Pyrrha-Deployment-Configurations) repository to start the rules and decision engine service. Run the following after configuring the services as explained in [the instructions](https://github.com/Pyrrha-Platform/Pyrrha-Deployment-Configurations/blob/main/Docker_Compose.md).

   ```sh
   docker-compose up --build pyrrha-rulesdecision
   ```

You can stop the services with:

   ```sh
   docker-compose stop pyrrha-rulesdecision
   ```

If you want to run build and run this image as a docker container and not use docker-compose, you can follow these steps:

1. Build the image

   ```sh
   docker build . -t rulesdecision
   ```

1. Run the image using the following command. Notice we are passing in the `src/.env` file as environment variable. This will not work if MariaDB is running in a docker image as `localhost:3306` will not resolve to the right image.

   ```sh
   docker run -p8080:8080 --env-file src/.env -t rulesdecision
   ```

1. You should see the application logs

1. Build the image

    ```sh
    docker build . -t rulesdecision
    ```

1. Run the image

   ```sh
   docker run -p8080:8080 -t rulesdecision
   ```

1. You should see the application logs

   ```sh
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

1. Change the db password

## Built with

- Docker
- MariaDB
- Flask
- [IBM Kubernetes Service](https://cloud.ibm.com/kubernetes/overview)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting Pyrrha pull requests.

## License

This project is licensed under the Apache 2 License - see the [LICENSE](LICENSE) file for details.

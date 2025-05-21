# Rococo Services spec

Services are standalone Docker applications that provide specific functionality. They are not tied to any particular model or repository and can be used independently. Services can be used to perform complex operations, such as data processing, integration with external systems, or any other business logic that does not fit into the normal flow of web applications.


## Service File Structure

- Each service is defined in a separate directory under `services/` and follows the naming convention `<service_name>`.
- Each service has access to the common library and can use any of the models, repositories, or other utilities defined in the common library.
- Each service directory contains the following files and directories:
    - `Dockerfile`: The Dockerfile for building the service image.
    - `pyproject.toml`: The requirements file for the service.
    - `processor.py`: The main entry point for the service.
    - `docker-entrypoint.sh`: The entrypoint script for the service.
    - `README.md`: A README file describing the service and its usage.
    - `version.py`: A version file for the service.
    - `lib/`: A directory that handles the service's business logic.


### Dockerfile

All services build from standard rococo base image. The Dockerfile should contain the following lines:

```dockerfile
# Use the base image as a parent image
FROM ecorrouge/rococo-service-host:latest

# Path to <service name>
ARG SERVICEPATH=./services/<service_name>

ENV PYTHONPATH /app


# ENV EXECUTION_TYPE can be either `MESSAGING` or `CRON`

# -- If ENV EXECUTION_TYPE=MESSAGING --
ENV EXECUTION_TYPE=MESSAGING
# ENV MESSAGING_TYPE is required, which should be either `SqsConnection` or `RabbitMqConnection`.
ENV MESSAGING_TYPE=SqsConnection  # <-- Can be either SqsConnection or RabbitMqConnection depending on requirements.

# -- Else if ENV EXECUTION_TYPE=CRON --
ENV EXECUTION_TYPE=CRON
ENV CRON_TIME_AMOUNT=5  # <-- Can be any number
ENV CRON_TIME_UNIT=minutes  # <-- Can be one of: seconds, minutes, hours, days, weeks
ENV CRON_RUN_AT=13:00:00  # <-- Time in HH:MM:SS when the service should run. Can be only provided if CRON_TIME_UNIT is `days`.

# -- End if --

ENV PROCESSOR_TYPE=<Class name of the main processor in processor.py>
ENV PROCESSOR_MODULE=processor

WORKDIR /app

COPY ${SERVICEPATH}/pyproject.toml ${SERVICEPATH}/poetry.lock* ./

# Allow installing dev dependencies to run tests
ARG INSTALL_DEV=false
RUN bash -c "if [ $INSTALL_DEV == 'true' ] ; then poetry install --no-root ; else poetry install --no-root --no-dev ; fi"

COPY ${SERVICEPATH} ./src

COPY ./common ./src/common

COPY ${SERVICEPATH}/docker-entrypoint.sh ./

RUN chmod +x ./docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
```

The dockerfile needs to define some special environment variables depending on requirements:

- `ENV EXECUTION_TYPE` can be either `MESSAGING` or `CRON`. This variable defines how the service will be executed.
-- If `EXECUTION_TYPE` is `MESSAGING`, the service will be executed when a message is received from the message queue.
--- The `MESSAGING_TYPE` variable is required, which should be either `SqsConnection` or `RabbitMqConnection`.

-- If `ENV EXECUTION_TYPE` is `CRON`, the service will be executed at a specific time or interval defined by the `CRON_TIME_AMOUNT` and `CRON_TIME_UNIT` variables.
--- The `CRON_TIME_AMOUNT` variable defines the amount of time to wait before executing the service again.
--- The `CRON_TIME_UNIT` variable defines the unit of time to wait before executing the service again. It can be one of: seconds, minutes, hours, days, weeks.
--- The `CRON_RUN_AT` variable defines the time in HH:MM:SS when the service should run. This variable can only be provided if `CRON_TIME_UNIT` is `days`.


### docker-entrypoint.sh

Use the following script exactly AS-IT-IS for `docker-entrypoint.sh` file:

```sh
#!/bin/bash

python3 src/version.py
python3 src/process.py
```

### version.py

Use the following script exactly AS-IT-IS for `version.py` file:

```python
from common.version import main

if __name__ == "__main__":
    main()
```

### processor.py
The `processor.py` file is the main entry point for the service. 

- It should contain the following code for the `EXECUTION_TYPE=MESSAGING` service:

```python
from common.app_logger import logger
from common.app_config import config
from rococo.messaging import BaseServiceProcessor
from lib.handler import message_handler

# This is an example implementation of a BaseServiceProcessor class.
# This should be done in the child image
class ChildLoggingServiceProcessor(BaseServiceProcessor):  # <-- Inherits from BaseServiceProcessor for MESSAGING.
    """
    Service processor that logs messages
    """

    def __init__(self):
        super().__init__()
        set_rollbar_exception_catch()
        # Initialize any other resources needed for the processor

    def process(self, message):
        logger.info("Received message: %s to the child image!", message)
        # Do something with the message
        # For example, you can call a method from the service's lib directory
        message_handler(message)
```

- It should contain the following code for the `EXECUTION_TYPE=CRON` service:

```python
import datetime
from common.app_logger import logger, set_rollbar_exception_catch
from common.app_config import config
from lib.handler import task_handler

# This is an example implementation of a processor class that is fired by crontab.
# This should be done in the child image
class LoggingServiceProcessor():  # <-- Does not inherit from BaseServiceProcessor for CRON.
    """
    Service processor that logs messages
    """
    def __init__(self):
        set_rollbar_exception_catch()
        # Initialize any other resources needed for the processor

    def process(self):
        """Main processor loop"""
        logger.info("Cron processor execution started at %s ...",datetime.datetime.utcnow())
        # Do something that needs to be done on a schedule
        # For example, you can call a method from the service's lib directory
        task_handler()
        logger.info("Cron processor execution finished at %s ...",datetime.datetime.utcnow())

```


### README.md

The README file should contain a brief description of the service, its purpose, and how to use it. It should also include any dependencies or requirements for running the service.

### __init__.py
The `__init__.py` file should be an empty file.

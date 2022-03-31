FROM python:latest
WORKDIR /usr/app/src
COPY requirements.txt ./
RUN pip install azure-servicebus
COPY ./src/utils/L2_Utils.py /usr/app/src/utils
COPY ./src/utils/L2Logger.py /usr/app/src/utils
COPY ./src/utils/subscriber.py /usr/app/src/utils
COPY ./src/main.py /usr/app/src

# This means our worker will be located at /usr/app/src/main.py
CMD ["python3", "./main.py"]
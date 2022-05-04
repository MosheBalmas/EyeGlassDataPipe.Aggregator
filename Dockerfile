FROM python:latest
WORKDIR /tmp
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN export PATH="$PATH:/opt/mssql-tools/bin"
RUN apt install -y unixodbc-dev
WORKDIR /usr/app/EyeGlassDataPipe.FileProcessor
COPY ./requirements.txt ./
RUN pip install --proxy proxy-dmz.intel.com:912 -r requirements.txt
COPY ./src ./src
COPY ./main.py ./
RUN mkdir ./Logs

# This means our worker will be located at /usr/app/src/main.py
CMD ["python3", "main.py"]
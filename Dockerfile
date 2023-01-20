FROM python:3.9

WORKDIR /src
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /src
ENTRYPOINT [ "python", "ingestion.py" ]
FROM python:3.11-slim

WORKDIR /app

COPY ./requirements.txt ./

COPY ./pyspark_nomina_pjud ./pyspark_nomina_pjud

COPY ./docker/batch_parse/entrypoint.sh ./

RUN apt-get update && apt-get install -y default-jdk

RUN pip install -r requirements.txt && chmod u+x entrypoint.sh

CMD [ "./entrypoint.sh" ]

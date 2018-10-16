FROM srcd/jupyter-spark:5.7.0

RUN mkdir -p /opt/

# engine jar location
ENV SPARK_JARS spark.jars
ENV SRCD_JAR /opt/jars/gitbase-spark-connector-assembly-*.jar

# bblfsh endpoint variables
ENV SPARK_BBLFSH_HOST spark.tech.sourced.bblfsh.grpc.host
ENV BBLFSH_HOST bblfshd
ENV SPARK_BBLFSH_PORT spark.tech.sourced.bblfsh.grpc.port
ENV BBLFSH_PORT 9432

USER root

RUN apt-get update && \
    apt-get install -y --no-install-suggests --no-install-recommends locales curl g++ libxml2-dev && \
    apt-get clean && \
    locale-gen en_US.UTF-8

ENV LANG en_US.UTF-8

COPY /target/scala-2.11/gitbase-spark-connector-assembly-*.jar /opt/jars/


RUN pip install jupyter-spark \
    && jupyter serverextension enable --py jupyter_spark \
    && jupyter nbextension install --py jupyter_spark \
    && jupyter nbextension enable --py jupyter_spark \
    && jupyter nbextension enable --py widgetsnbextension

# Separate the config file in a different RUN creation as this may change more often
RUN echo "$SPARK_JARS $SRCD_JAR" >> /usr/local/spark/conf/spark-defaults.conf \
    && echo "$SPARK_BBLFSH_HOST $BBLFSH_HOST\n$SPARK_BBLFSH_PORT $BBLFSH_PORT" >> /usr/local/spark/conf/spark-defaults.conf

# Disable jupyter token
RUN mkdir -p /root/.jupyter && \
    echo "c.NotebookApp.token = ''" > ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.notebook_dir = '/home'" >> ~/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8080" >> ~/.jupyter/jupyter_notebook_config.py
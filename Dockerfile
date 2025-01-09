FROM quay.io/astronomer/astro-runtime:12.6.0

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER root

COPY ./dados /opt/bitnami/spark/jobs

RUN chown -R astro:astro /opt/bitnami/spark/jobs && chmod -R 755 /opt/bitnami/spark/jobs

USER astro
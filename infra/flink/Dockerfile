# Stage 1: build the H3 scalar UDF (shaded jar incl. Uber h3-java natives).
FROM maven:3.9-eclipse-temurin-11 AS udf
WORKDIR /build
COPY udf/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY udf/src ./src
RUN mvn -q -B package -DskipTests

# Stage 2: Flink + Kafka SQL connector + Iceberg runtime + S3 plugin + UDF.
FROM flink:1.20.0-scala_2.12-java11

ARG MVN=https://repo.maven.apache.org/maven2
ARG ICEBERG_VERSION=1.7.1
ARG KAFKA_CONNECTOR=3.3.0-1.20

# Iceberg's Flink runtime needs Hadoop Configuration classes on the classpath even
# when using S3FileIO. flink-shaded-hadoop-2-uber is the lowest-friction option.
# (Exercise: replace it with the minimal hadoop-client-api/runtime pair.)
RUN set -eux; cd /opt/flink/lib; \
    curl -fsSLO ${MVN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar; \
    curl -fsSLO ${MVN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MVN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MVN}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# S3 filesystem plugin for checkpoints/savepoints to MinIO.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY --from=udf /build/target/h3-udf.jar /opt/flink/lib/h3-udf.jar

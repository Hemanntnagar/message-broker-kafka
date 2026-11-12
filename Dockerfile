# syntax=docker/dockerfile:1

FROM eclipse-temurin:26-jdk-noble AS build

WORKDIR /build
COPY mvnw pom.xml ./
COPY .mvn ./.mvn
RUN chmod +x mvnw
COPY src ./src

RUN ./mvnw -B -DskipTests package \
    && JAR="$(ls target/javastream-*-jar-with-dependencies.jar)" \
    && cp "$JAR" /build/javastream.jar

FROM eclipse-temurin:26-jre-noble AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/javastream
COPY --from=build /build/javastream.jar ./javastream.jar
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV JAVASTREAM_STORAGE_DATA_DIR=/var/lib/javastream/data \
    JAVASTREAM_LOG_DIR=/var/lib/javastream/logs \
    JAVASTREAM_MONITORING_HTTP_HOST=0.0.0.0 \
    JAVASTREAM_NETWORK_LISTEN_HOST=0.0.0.0

VOLUME ["/var/lib/javastream/data", "/var/lib/javastream/logs"]

EXPOSE 9092 19092 8080

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=15s --timeout=5s --start-period=45s --retries=5 \
  CMD curl -fsS "http://127.0.0.1:${JAVASTREAM_MONITORING_HTTP_PORT:-8080}/health" || exit 1

ENTRYPOINT ["/entrypoint.sh"]

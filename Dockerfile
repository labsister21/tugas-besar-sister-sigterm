FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jdk
RUN apt-get update && \
    apt-get install -y iproute2 iputils-ping && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/target/raft-protocol-server-1.0-SNAPSHOT.jar ./app.jar

# run the command in the docker compose
FROM docker.io/maven:3.8-jdk-11 as builder
FROM docker.io/adoptopenjdk/openjdk11:alpine-jre
WORKDIR /app
COPY pom.xml .
COPY src ./src
COPY /target/gcp-java-cloudrun-demo-app.jar /gcp-java-cloudrun-demo-app.jar
CMD ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/gcp-java-cloudrun-demo-app.jar", "--spring.profiles.active=${ZZJ_ENV}"]

#FROM openjdk:8
FROM nginx:alpine
COPY ./target/scala-2.13/storagenode.jar /app/src/app.jar
WORKDIR /app/src
RUN apk add openjdk8
ENTRYPOINT ["java", "-jar","app.jar"]
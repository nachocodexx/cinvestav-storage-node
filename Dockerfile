#FROM openjdk:8
FROM nginx:alpine
RUN apk add openjdk8
COPY ./target/scala-2.13/storagenode.jar /app/src/app.jar
COPY nginx.conf /etc/nginx/nginx.conf
COPY entrypoint.sh /app/src
RUN ["chmod","+x","/app/src/entrypoint.sh"]
WORKDIR /app/src
#ENTRYPOINT ["java", "-jar","app.jar"]
ENTRYPOINT ["/app/src/entrypoint.sh"]
#CMD ["nginx", "-g", "daemon off;"]

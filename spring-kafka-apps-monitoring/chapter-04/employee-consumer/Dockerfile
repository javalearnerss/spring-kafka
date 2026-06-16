
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY build/libs/employee-consumer-0.0.1-SNAPSHOT.jar ./employee-consumer.jar

EXPOSE 7051

ENTRYPOINT ["java", "-jar", "employee-consumer.jar"]

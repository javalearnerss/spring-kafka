
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY build/libs/employee-producer-0.0.1-SNAPSHOT.jar ./employee-producer.jar

EXPOSE 7050

ENTRYPOINT ["java", "-jar", "employee-producer.jar"]

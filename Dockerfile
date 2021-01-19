FROM maven:3.6.3-jdk-11-slim AS build
COPY src ./build/src
COPY pom.xml ./build
RUN mvn -f /build/pom.xml clean package

#
# Package stage
#
FROM openjdk:11-jre-slim
COPY --from=build ./build/target/kube-scheduler-1.0-SNAPSHOT.jar /usr/local/lib/kube-scheduler.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/usr/local/lib/kube-scheduler.jar"]
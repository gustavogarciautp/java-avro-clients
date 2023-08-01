FROM openjdk:8-jre-alpine3.9

COPY target/kafka-avro-msk-course-1.0-SNAPSHOT-jar-with-dependencies.jar /java-avro-clients.jar

WORKDIR /

CMD ["java", "-cp", "java-avro-clients.jar", "com.github.mskcourse.clients.avro.MSKAvroJavaConsumer"]


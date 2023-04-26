FROM openjdk:17-slim

WORKDIR /apps

ADD https://storage.googleapis.com/code-richardyin20230422/KafkaPerfPublisher-1.0.0-SNAPSHOT.jar .

CMD ["java", "-jar", "KafkaPerfPublisher-1.0.0-SNAPSHOT.jar"]

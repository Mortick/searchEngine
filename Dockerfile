FROM openkbs/jdk-mvn-py3-x11
WORKDIR C:\Users\Qing\School\CS1660\Project
COPY searchEngine.jar searchEngine.jar
COPY cs1660.json cs1660.json
COPY Data Data
ENV GOOGLE_APPLICATION_CREDENTIALS cs1660.json
CMD ["java","-jar","searchEngine.jar"]
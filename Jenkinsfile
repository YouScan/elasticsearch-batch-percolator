pipeline {
    agent any
    options {
        timestamps()
    }
    stages {
        stage("Build") {
            agent {
                docker {
                    image 'openjdk:8-jre-alpine'
                    args '-u 0'
                }
            }
            steps {
                sh './gradlew pluginZipFile'
            }
        }
    }
}
pipeline {
    agent any
    options {
        timestamps()
    }
    environment {
        MAVEN_URL = 'http://nexus.yscan.local/repository/maven-releases/'
    }
    stages {
        stage("Build") {
            agent {
                docker {
                    image 'gradle:4.5.1-jdk8'
                    args '-u 0'
                }
            }
            steps {
                sh './gradlew pluginZipFile'
                withCredentials([usernamePassword(credentialsId: 'maven_nexus', usernameVariable: 'MAVEN_USER', passwordVariable: 'MAVEN_PASSWORD')]) {
                    sh './gradlew publishing'
                }
            }
        }
    }
}
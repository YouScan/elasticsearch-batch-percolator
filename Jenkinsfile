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
                sh './gradlew pluginZipFile -P version=2.0.$BUILD_NUMBER$VERSION_SUFFIX'
                withCredentials([usernamePassword(credentialsId: 'maven_nexus', usernameVariable: 'MAVEN_USER', passwordVariable: 'MAVEN_PASSWORD')]) {
                    sh './gradlew publish -P version=2.0.$BUILD_NUMBER$VERSION_SUFFIX'
                }
            }
        }
    }
}
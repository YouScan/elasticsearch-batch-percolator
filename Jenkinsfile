pipeline {
    agent any
    options {
        timestamps()
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
            }
        }
    }
}
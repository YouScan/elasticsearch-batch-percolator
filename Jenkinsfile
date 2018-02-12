pipeline {
    agent any
    options {
        timestamps()
    }
    stages {
        stage("Build") {
            agent {
                docker {
                    image '4.5.1-jdk8-alpine'
                    args '-u 0'
                }
            }
            steps {
                sh './gradlew pluginZipFile'
            }
        }
    }
}
pipeline {
    agent any

    triggers {
        pollSCM('H/2 * * * *')  // sin webhook publico: Jenkins no es alcanzable desde GitHub
    }

    environment {
        GIT_COMMIT_SHORT = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
    }

    stages {
        stage('Build') {
            steps {
                sh 'GIT_COMMIT=${GIT_COMMIT_SHORT} docker compose build'
                sh 'docker build -t lifemap-tests ./tests'
            }
        }

        stage('Test') {
            steps {
                sh 'GIT_COMMIT=${GIT_COMMIT_SHORT} docker compose up -d mongo api'
                sh 'sleep 8'
                sh 'docker run --rm --network lifemap_network -e API_URL=http://lifemap_api:5000 lifemap-tests'
            }
        }

        stage('Deploy') {
            steps {
                sh 'GIT_COMMIT=${GIT_COMMIT_SHORT} docker compose up -d --build'
            }
        }
    }

    post {
        always {
            sh 'docker compose ps'
        }
        failure {
            echo 'El pipeline fallo: revisar la etapa de Test antes de desplegar. No se despliega la version anterior encima de una fallida.'
        }
    }
}

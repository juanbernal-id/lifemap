pipeline {
    agent {
        node {
            label ''
            customWorkspace '/Users/juan-bernal/Desktop/workspace/lifemap'
        }
    }

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

        stage('Ingesta (Kaggle)') {
            // Descarga del Yelp Open Dataset con la API de Kaggle dentro del pipeline.
            // El token vive como credencial de Jenkins (id: kaggle-api), nunca en el repo.
            // Idempotente: si los archivos ya estan en data/raw (gitignored) no se vuelve
            // a bajar el zip de ~4.3 GB en cada build.
            steps {
                withCredentials([usernamePassword(credentialsId: 'kaggle-api',
                                                  usernameVariable: 'KAGGLE_USERNAME',
                                                  passwordVariable: 'KAGGLE_KEY')]) {
                    sh '''
                        if [ -f data/raw/yelp_academic_dataset_business.json ] && \
                           [ -f data/raw/yelp_academic_dataset_review.json ]; then
                            echo "Dataset ya presente en data/raw, se omite la descarga:"
                            ls -lh data/raw/yelp_academic_dataset_business.json data/raw/yelp_academic_dataset_review.json
                        else
                            docker run --rm -v "$WORKSPACE":/app -w /app \
                                -e KAGGLE_USERNAME -e KAGGLE_KEY \
                                lifemap-dask python ingesta/descargar_kaggle.py
                        fi
                    '''
                }
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

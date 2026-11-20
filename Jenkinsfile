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
                sh 'docker run --rm --network lifemap_network -e API_URL=http://lifemap_api:5000 lifemap-tests pytest -v --ignore=test_streaming.py'
            }
        }

        stage('Prueba de streaming (E2.9)') {
            // Prueba de humo: el productor publica un lote chico y rapido,
            // el consumidor corre con un timeout corto, y se verifica por la
            // API que las predicciones llegaron a Mongo con el modelo
            // versionado (no reentrenado). Si algo de esto falla, no se
            // despliega (igual que con pytest en la etapa Test).
            steps {
                sh 'GIT_COMMIT=${GIT_COMMIT_SHORT} docker compose up -d kafka kafka-init spark-master spark-worker'
                sh '''
                    for i in $(seq 1 20); do
                        docker compose ps kafka | grep -q healthy && break
                        sleep 3
                    done
                '''
                sh '''
                    timeout 90 docker run --rm --network lifemap_network \
                        -v "$WORKSPACE":/app -w /app \
                        -e MONGO_URI=mongodb://mongo:27017 -e SPARK_MASTER=spark://spark-master:7077 \
                        lifemap-spark spark-submit --master spark://spark-master:7077 \
                        spark_jobs/streaming_consumidor.py > streaming_consumidor.log 2>&1 &
                    echo $! > consumidor.pid
                '''
                sh 'sleep 20'  // tiempo para que el consumidor arranque las dos queries
                sh '''
                    docker compose run --rm \
                        -e MONGO_URI=mongodb://mongo:27017 -e KAFKA_BOOTSTRAP=kafka:9092 \
                        -e LIMITE_MENSAJES=80 -e SEGUNDOS_POR_MES=0 \
                        productor
                '''
                sh 'sleep 25'  // tiempo para que el consumidor procese el lote
                sh 'docker run --rm --network lifemap_network -e API_URL=http://lifemap_api:5000 -e MIN_PREDICCIONES_ESPERADAS=50 lifemap-tests pytest -v test_streaming.py'
            }
            post {
                always {
                    sh 'kill $(cat consumidor.pid) 2>/dev/null || true'
                    sh 'cat streaming_consumidor.log || true'
                }
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

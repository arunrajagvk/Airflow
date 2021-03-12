astro dev start
python3 -V
where python3
gcc --version
where pip3

  170  python3 -V
  171  AIRFLOW_VERSION=2.0.1
  172  PYTHON_VERSION="$(python3 -V | cut -d " " -f 2 | cut -d "." -f 1-2)"
  173  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
  174  python3 -m pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
  175  airflow
  176  airflow info
  177  cd /Users/arunraja/airflow
  178  ls
  179  cd
  180  cat .zprofile
  181  cd /Users/arunraja/airflow
  182  ls
  183  airflow db init

## WORKED with Python 3.8 version for mac os X
##-- USING DOCKER COMPOSE 

302  brew install wget
  303  wget https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
  304  ls
  305  .code
  306  ls
  307  docker-compose -f docker-compose.yaml up -d
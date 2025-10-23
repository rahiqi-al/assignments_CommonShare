"""
docker exec -it -u airflow airflow bash
pip show ...
pip install ...
docker-compose restart airflow
"""
   
nessie : http://localhost:19120/api/v1/trees , http://localhost:19120/tree/main
spark : http://localhost:8081/
airflow : http://localhost:8080/home
dremio : http://localhost:9047/
minio : http://localhost:9001/

# since we used shared volumes no need to install in all of them just one is enough cause it will be mounted to all of them
"""
docker exec -it --user root spark-master bash
/opt/bitnami/python/bin/pip3 install python-dotenv
/opt/bitnami/python/bin/pip3 install PyYAML
docker exec -it --user root spark-worker-1 bash
/opt/bitnami/python/bin/pip3 install python-dotenv
/opt/bitnami/python/bin/pip3 install PyYAML
docker exec -it --user root spark-worker-2 bash
/opt/bitnami/python/bin/pip3 install python-dotenv
/opt/bitnami/python/bin/pip3 install PyYAML
"""


docker-compose restart spark-master spark-worker-1 spark-worker-2





-Connecting Superset to Dremio:
 Start the Containers:

 docker-compose up -d.
 docker exec -it superset pip install sqlalchemy-dremio

-Initialize Superset:
 Execute these commands in the superset container:

 docker exec -it superset superset fab create-admin

 ""Username: admin
  First name: admin
  Last name: admin
  Email: ali123rahiqi@gmail.com
  Password: simplepassword123 (confirm same password twice).""

  docker exec -it superset superset db upgrade
  docker exec -it superset superset init
-Access Superset:
  Open http://localhost:8088.
  Log in with the admin credentials you set during create-admin (default: admin / admin if not changed).

-correct url:
 dremio+flight://ali:alialiali1@192.168.32.13:32010/dremio?UseEncryption=false




-Solution to jars :
 ==>Create Local Folder Outside Project:
      mkdir -p ~/.spark-jars/cache ~/.spark-jars/jars
      sudo chown -R 1001:1001 ~/.spark-jars
 ==>Download JARs to Local Folder Run once to download JARs:
      docker run --rm -v $HOME/.spark-jars:/spark-jars bitnami/spark:3.5.1 bash -c "spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,com.amazon.deequ:deequ:2.0.7-spark-3.5 --conf spark.jars.ivy=/spark-jars /tmp/dummy.py && mv /spark-jars/*.jar /spark-jars/jars/ 2>/dev/null || true"
      docker run --rm -v $HOME/.spark-jars:/spark-jars bitnami/spark:3.5.1 bash -c "spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.jars.ivy=/spark-jars /tmp/dummy.py && mv /spark-jars/*.jar /spark-jars/jars/ 2>/dev/null || true"
 ==>Update docker-compose.yml Mount ~/.spark-jars to /opt/bitnami/spark/.ivy2
 ==>Verify:
      ls -R ~/.spark-jars/jars

 ""(Spark will automatically use the JARs in ~/.spark-jars (mounted to /opt/bitnami/spark/.ivy2) without downloading. The --packages command checks the cache first and finds the pre-downloaded JARs. No re-downloads occur after the initial setup)""


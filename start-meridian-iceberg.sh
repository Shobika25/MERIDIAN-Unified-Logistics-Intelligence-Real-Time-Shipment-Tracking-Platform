#!/bin/bash
set +x
umask 077

mkdir -p /tmp/hadoop-conf /tmp/ivy

TOKEN="${AZURE_SAS_TOKEN:?Set AZURE_SAS_TOKEN before starting the container}"
TOKEN_XML="$(printf '%s' "$TOKEN" | sed 's/&/\&amp;/g')"

cat > /tmp/hadoop-conf/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.azure.account.auth.type.adls2501.dfs.core.windows.net</name>
    <value>SAS</value>
  </property>
  <property>
    <name>fs.azure.sas.fixed.token.meridian.adls2501.dfs.core.windows.net</name>
    <value>${TOKEN_XML}</value>
  </property>
</configuration>
EOF

unset TOKEN TOKEN_XML
chmod 600 /tmp/hadoop-conf/core-site.xml
export HADOOP_CONF_DIR=/tmp/hadoop-conf

exec /opt/spark/bin/spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.hadoop:hadoop-azure:3.4.2 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.meridian=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.meridian.type=hadoop \
  --conf spark.sql.catalog.meridian.warehouse=file:///opt/iceberg/warehouse \
  --conf spark.sql.catalog.meridian_adls=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.meridian_adls.type=hadoop \
  --conf spark.sql.catalog.meridian_adls.warehouse=abfss://meridian@adls2501.dfs.core.windows.net/iceberg/warehouse
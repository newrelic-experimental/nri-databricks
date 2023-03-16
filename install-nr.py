# Databricks notebook source
# Initialization script to generate installer for nri-databricks

dbutils.fs.put("dbfs:/newrelic/nri-databricks-init.sh", """ 
#!/bin/sh

cat <<EOF >> /tmp/start-databricks-metric.sh
#!/bin/sh

set_execute_permission() {
  for file in "\$@"; do
    chmod 555 "\$file"
  done
}

timeout=60
while [ ! -e "/tmp/driver-env.sh" ] && [ \$timeout -gt 0 ]; do
  sleep 1
  timeout=\$((timeout-1))
done

if [ $DB_IS_DRIVER ]; then

    if [ -z "$NEWRELIC_ACCOUNT_ID" ]; then
      echo "Error: NEWRELIC_ACCOUNT_ID environment variable is not set."
      exit 1
    fi

    if [ -z "$NEWRELIC_LICENSE_KEY" ]; then
      echo "Error: NEWRELIC_LICENSE_KEY environment variable is not set."
      exit 1
    fi

    if ! curl -L --retry 3 --retry-delay 5 --silent --show-error -o /tmp/nri-databricks.tar.gz https://raw.githubusercontent.com/newrelic-experimental/nri-databricks/rewrite_init_script/nri-databricks.tar.gz; then
      echo "Error: Failed to download nri-databricks binary."
      exit 1
    fi

    mkdir -p /etc/nri-databricks
    if ! tar xvf /tmp/nri-databricks.tar.gz -C /etc/nri-databricks; then
      echo "Error: Failed to extract nri-databricks binary."
      exit 1
    fi

    if ! python -m pip install -r /etc/nri-databricks/requirements.txt; then
      echo "Error: Failed to install required packages."
      exit 1
    fi

    if [ -e "/tmp/driver-env.sh" ]; then
        . /tmp/driver-env.sh
    else
        CONF_PUBLIC_DNS='<<CONF_PUBLIC_DNS>>'
        CONF_UI_PORT='<<CONF_UI_PORT>>'
    fi
   
    if [ -e "/tmp/master-params" ]; then
        MASTER_UI_PORT=$(cat /tmp/master-params | cut -d ' ' -f 2)
    else
        MASTER_UI_PORT='<<MASTER_UI_PORT>>'
    fi

    if [ -z $NEWRELIC_ENDPOINT_REGION ]; then
        NEWRELIC_ENDPOINT_REGION="US"
    fi

    cat <<CONFIG > /etc/nri-databricks/config.yml
integration_name: com.nrlabs.databricks
run_as_service: True
poll_interval: 30
log_level: error
log_file: /tmp/nri-databricks.log
spark:
  cluster_name: \$DB_CLUSTER_NAME
  cluster_mode: driver_mode
  master_ui_port: \$MASTER_UI_PORT
  conf_ui_port: \$CONF_UI_PORT
  driver_host: \$CONF_PUBLIC_DNS
newrelic:
  api_endpoint: \$NEWRELIC_ENDPOINT_REGION
  account_id: \$NEWRELIC_ACCOUNT_ID
  api_key: \$NEWRELIC_LICENSE_KEY
labels:
  environment: prod
CONFIG

    cp --force /etc/nri-databricks/nrdatabricksd /etc/init.d/
    set_execute_permission /etc/nri-databricks/src/__main__.py /etc/nri-databricks/nrdatabricksd /etc/init.d/nrdatabricksd

    /etc/nri-databricks/nrdatabricksd stop
    /etc/nri-databricks/nrdatabricksd start

fi
EOF

chmod a+x /tmp/start-databricks-metric.sh
/tmp/start-databricks-metric.sh >> /tmp/start-databricks-metric.log 2>&1 &

""", True)

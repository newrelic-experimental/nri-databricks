# Databricks notebook source
# Initialization script to generate installer for nri-databricks

dbutils.fs.put("dbfs:/newrelic/nri-databricks-init.sh",""" 
#!/bin/sh

install_nri_databricks () {

if [ $DB_IS_DRIVER ]; then
    # Root user detection
    if [ $(echo " $UID ") = " 0 " ];                                      
    then                                                                     
        sudo=''                                                                
    else
        sudo='sudo'                                                        
    fi

    echo " >>> Check if this is driver ? $DB_IS_DRIVER "
    echo " >>> Spark Driver ip : $DB_DRIVER_IP "
    echo " >>> Public DNS: $CONF_PUBLIC_DNS "
    echo " >>> UI Port: $CONF_UI_PORT "
    
    # Download or copy nri-databricks binary
    wget https://github.com/newrelic-experimental/nri-databricks/releases/download/v1.0.1/nri-databricks.tar.gz -P /tmp
    cd /etc
    tar xvf /tmp/nri-databricks.tar.gz

    # fetch requirements
    cd /etc/nri-databricks
    python -m pip install -r requirements.txt || true
    
    if [ -e "/tmp/driver-env.sh" ]; then
        source /tmp/driver-env.sh
        echo "CONF_PUBLIC_DNS is $CONF_PUBLIC_DNS"
        echo "CONF_UI_PORT is $CONF_UI_PORT"
    else
        CONF_PUBLIC_DNS='<<CONF_PUBLIC_DNS>>'
        CONF_UI_PORT='<<CONF_UI_PORT>>'
    fi
    
    if [ -e "/tmp/master-params" ]; then
        MASTER_UI_PORT=$(cat /tmp/master-params | cut -d ' ' -f 2)
    else
        MASTER_UI_PORT='<<MASTER_UI_PORT>>'
    fi
    
        
    if [ $NEWRELIC_ENDPOINT_REGION == "" ]; then
        NEWRELIC_ENDPOINT_REGION="US"
    fi

    # Create config.yml file (take care to format properly as is required for a yml file)
    
    echo "
integration_name: com.nrlabs.databricks
run_as_service: True
poll_interval: 30
log_level: error
log_file: /tmp/nri-databricks.log
spark:
  cluster_name: $DB_CLUSTER_NAME
  cluster_mode: driver_mode
  master_ui_port: $MASTER_UI_PORT
  conf_ui_port: $CONF_UI_PORT
  driver_host: $CONF_PUBLIC_DNS
newrelic:
  api_endpoint: $NEWRELIC_ENDPOINT_REGION
  account_id: $NEWRELIC_ACCOUNT_ID
  api_key: $NEWRELIC_LICENSE_KEY
labels:
  environment: prod
         " > /etc/nri-databricks/config.yml

    echo " >>> Configured  config.yml \n $(</etc/nri-databricks/config.yml)"

    # copy service
    cp /etc/nri-databricks/nrdatabricksd /etc/init.d/
    # give execute permission
    chmod 555 /etc/nri-databricks/src/__main__.py
    chmod 555 /etc/nri-databricks/nrdatabricksd
    chmod 555 /etc/init.d/nrdatabricksd
    # start service
    /etc/init.d/nrdatabricksd start
fi
}

install_nri_databricks || true

""",True)


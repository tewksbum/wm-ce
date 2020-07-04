Apache Spark streaming ETL

- subs to a pubsub with netsuite order json data
- streaming etl
- writes to cloud sql

Dependencies
- apache spark 3.0
- scala 2.12

Required VSCode extensions
- Maven for Java

Optional VSCode extensions
- Scala Syntax (official)

Allow service account to run as dataproc worker
```
gcloud projects add-iam-policy-binding wemade-core \
    --role roles/dataproc.worker \
    --member="serviceAccount:dataproc-service-account@wemade-core.iam.gserviceaccount.com"
```

Allow service account to read a pubsub
```
gcloud beta pubsub subscriptions add-iam-policy-binding \
    wm-order-intake-sparkles \
    --role roles/pubsub.subscriber \
     --member="serviceAccount:dataproc-service-account@wemade-core.iam.gserviceaccount.com"
```

Allow service account to read a secret
```
gcloud secrets add-iam-policy-binding mariadb \
    --member="serviceAccount:dataproc-service-account@wemade-core.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```    

Create dataproc cluster, enable job logging to stackdriver
```
gcloud dataproc clusters create sparkles \
    --region=us-central1 \
    --zone=us-central1-c \
    --scopes=pubsub \
    --image-version="preview" \
	--bucket=wm_dataproc \
	--single-node \
	--master-machine-type n1-standard-2 \
	--master-boot-disk-size 200 \
    --service-account="dataproc-service-account@wemade-core.iam.gserviceaccount.com" \
	--properties dataproc:dataproc.logging.stackdriver.job.driver.enable=true
```    

Submit a Job to the dataproc cluster
```
gcloud dataproc jobs submit spark \
    --cluster=sparkles \
    --class=streamer.OrderStreamer \
    --jars=gs://wm_dataproc/netsuite-orders-1.0-SNAPSHOT.jar \
	--region=us-central1  \
	--properties spark.jars.packages=com.google.cloud:google-cloud-secretmanager:1.1.0
	
```
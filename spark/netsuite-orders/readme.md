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
	--master-boot-disk-size 1000 \
    --service-account="dataproc-service-account@wemade-core.iam.gserviceaccount.com" \
	--properties dataproc:dataproc.logging.stackdriver.job.driver.enable=true
```    

Copy Jar file and Submit a Job to the dataproc cluster
```
gsutil cp target/netsuite-orders-1.0-SNAPSHOT.jar gs://wm_dataproc/

gcloud dataproc jobs submit spark \
    --cluster=sparkles \
    --class=streamer.OrderStreamer \
    --jars=gs://wm_dataproc/netsuite-orders-1.0-SNAPSHOT.jar \
	--region=us-central1  \
	--properties spark.jars.packages=org.apache.spark:spark-sql_2.12:2.4.6
```

Spark DataFrame schema for the order object
```
root
 |-- id: double (nullable = false)
 |-- orderNumber: string (nullable = true)
 |-- totals: struct (nullable = true)
 |    |-- merchandiseCostTotal: double (nullable = false)
 |    |-- merchandiseTotal: double (nullable = false)
 |    |-- merchandiseTaxTotal: double (nullable = false)
 |    |-- shippingTotal: double (nullable = false)
 |    |-- shippingTaxTotal: double (nullable = false)
 |    |-- discountTotal: double (nullable = false)
 |    |-- serviceTotal: double (nullable = false)
 |    |-- serviceTaxTotal: double (nullable = false)
 |    |-- total: double (nullable = false)
 |-- dates: struct (nullable = true)
 |    |-- placedOn: string (nullable = true)
 |    |-- createdOn: string (nullable = true)
 |    |-- updatedOn: string (nullable = true)
 |-- attributes: struct (nullable = true)
 |    |-- webOrderNumber: string (nullable = true)
 |    |-- webOrderId: double (nullable = true)
 |    |-- subsidiary: string (nullable = true)
 |    |-- channel: string (nullable = true)
 |    |-- source: string (nullable = true)
 |    |-- school: string (nullable = true)
 |-- customer: struct (nullable = true)
 |    |-- email: string (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- name: string (nullable = true)
 |-- billing: struct (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- addr1: string (nullable = true)
 |    |-- addr2: string (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- state: string (nullable = true)
 |    |-- zip: string (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- phone: string (nullable = true)
 |-- fees: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- extPrice: double (nullable = false)
 |    |    |-- quantity: string (nullable = true)
 |    |    |-- lob: string (nullable = true)
 |    |    |-- cost: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- unitPrice: double (nullable = false)
 |    |    |-- shipment: string (nullable = true)
 |    |    |-- itemTitle: string (nullable = true)
 |    |    |-- itemSku: string (nullable = true)
 |    |    |-- itemId: double (nullable = false)
 |    |    |-- isDropship: boolean (nullable = false)
 |    |    |-- isCancelled: boolean (nullable = false)
 |    |    |-- tax: double (nullable = false)
 |-- shipments: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- lines: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- extPrice: double (nullable = false)
 |    |    |    |    |-- quantity: string (nullable = true)
 |    |    |    |    |-- lob: string (nullable = true)
 |    |    |    |    |-- cost: string (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |    |-- unitPrice: double (nullable = false)
 |    |    |    |    |-- shipment: string (nullable = true)
 |    |    |    |    |-- itemTitle: string (nullable = true)
 |    |    |    |    |-- itemSku: string (nullable = true)
 |    |    |    |    |-- itemId: double (nullable = false)
 |    |    |    |    |-- isDropship: boolean (nullable = false)
 |    |    |    |    |-- isCancelled: boolean (nullable = false)
 |    |    |    |    |-- tax: double (nullable = false)
 |    |    |-- addr1: string (nullable = true)
 |    |    |-- addr2: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- zip: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- phone: string (nullable = true)
 |    |    |-- email: string (nullable = true)
 |    |    |-- type: string (nullable = true)
```
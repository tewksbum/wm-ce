export GOOGLE_APPLICATION_CREDENTIALS="/d/work/wemade/gcp/wemade-core-192394ec89ff.json" && mvn compile exec:java -Dexec.mainClass=com.wemade.pipeline.BigQueryPipeline -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
--project=wemade-core \
--config=$(pwd)/src/main/java/config.properties
--stagingLocation=gs://wemade-dataflow-templates/staging \
--tempLocation=gs://wemade-dataflow-templates/temp \
--templateLocation=gs://wemade-dataflow-templates/templates/BQWriterPipelineCI.json \
--jobName=BQWriterPipeline
--runner=DataflowRunner"
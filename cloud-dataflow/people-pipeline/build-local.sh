export GOOGLE_APPLICATION_CREDENTIALS="/d/work/wemade/gcp/wemade-core-192394ec89ff.json" && mvn compile exec:java -Dexec.mainClass=com.wemade.pipeline.PeoplePipeline -Dexec.cleanupDaemonThreads=false -Dexec.args=" \
--project=wemade-core \
--stagingLocation=gs://wemade-dataflow-templates/staging \
--tempLocation=gs://wemade-dataflow-templates/temp \
--templateLocation=gs://wemade-dataflow-templates/templates/PeoplePipelineCI.json \
--jobName=PeoplePipeline
--runner=DataflowRunner"
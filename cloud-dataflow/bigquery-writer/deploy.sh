#!/bin/bash
export GOOGLE_APPLICATION_CREDENTIALS=../../keys/prod.json
mvn compile exec:java      -Dexec.mainClass=com.pipeline.DataflowStreamingPipeline      -Dexec.args="--project=wemade-core --config=$(pwd)/src/main/java/config.properties --jobName=bigquerywriter"

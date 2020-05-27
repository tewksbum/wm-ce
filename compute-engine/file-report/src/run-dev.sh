#!/bin/bash
export GOOGLE_APPLICATION_CREDENTIALS=/Users/jyang/Documents/work/wemade/key/gcp/wemade-core.json
export PROJECTID=wemade-core
export DSPROJECTID=fire-core-wm
export DATASTORENS=wemade-dev
export ELASTIC_SECRET=projects/180297787522/secrets/elastic/versions/2
export REPORT_ESINDEX=wemade-reports-dev
export REPORT_SUB=wm-file-report-sub-dev
export MYSQL_INSTANCE=wemade-core:us-central1:pipeline
export MYSQL_TESTING=35.194.39.63
export GCP_PROJECT=wemade-core
go run .
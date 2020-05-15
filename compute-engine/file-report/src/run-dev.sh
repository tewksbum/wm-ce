#!/bin/bash
export GOOGLE_APPLICATION_CREDENTIALS=/Users/jyang/Documents/work/wemade/key/gcp/wemade-core.json
export PROJECTID=wemade-core
export DSPROJECTID=fire-core-wm
export DATASTORENS=wemade-dev
export ELASTIC_SECRET=projects/180297787522/secrets/elastic/versions/2
export REPORT_ESINDEX=wemade-reports-dev
export REPORT_SUB=wm-file-report-sub-dev
export GCP_PROJECT=wemade-core
go run .
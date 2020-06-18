#!/bin/bash
(export GOOGLE_APPLICATION_CREDENTIALS=/home/jyang_trellist/keys/wemade-core.json && export PROJECTID=wemade-core && export DSPROJECTID=fire-core-wm && export DATASTORENS=wemade-dev && export ELASTIC_SECRET=projects/180297787522/secrets/elastic/versions/5 && export REPORT_ESINDEX=wemade-reports-dev && export REPORT_SUB=wm-file-report-sub-dev && export MYSQL_INSTANCE=wemade-core:us-central1:pipeline && export MYSQL_HOST=10.45.160.3 && export GCP_PROJECT=wemade-core && ./dev-exception-report&)

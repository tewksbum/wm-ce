#!/bin/bash

docker build -t gcr.io/wemade-core/browser-id:11 .

docker push gcr.io/wemade-core/browser-id:11

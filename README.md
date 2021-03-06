# WeMade Data Intelligence Pipelines

## Current Release

## Branches

---

- dev
- prod

We do not use master branch

## Cloud Functions

---

## Entry Points

- cloud-functions / event-api
- cloud-functions / file-api
- cloud-functions / segment (upsert, read, delete)

## record processors

- cloud-functions / file-processor
- cloud-functions / json-processor
- cloud-functions / record-processor

## pipelines

- cloud-functions / pre-process
- cloud-functions / campaign-post
- cloud-functions / consignment-post
- cloud-functions / event-post
- cloud-functions / people-post
- cloud-functions / order-post
- cloud-functions / orderdetail-post
- cloud-functions / product-post

## 360

- cloud-functions / campaign-360
- cloud-functions / consignment-360
- cloud-functions / event-360
- cloud-functions / order-360
- cloud-functions / orderdetail-360
- cloud-functions / people-360
- cloud-functions / product-360
- cloud-functions / household-360


## CI/CD
gcloud command example to set up cloud buiild trigger 
```
   gcloud alpha builds triggers create github --description="github-household-360" --repo-name="context-engine" --repo-owner="jyang-wemade" --branch-pattern="^dev$|^prod$" --included-files="cloud-functions/household-360/**" --build-config="cloud-functions/household-360/cloudbuild.yaml" 
```
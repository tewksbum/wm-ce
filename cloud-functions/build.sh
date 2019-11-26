#!/bin/sh -e
IGNORE_FILES=$(ls -p | grep -v /)
TRACKING_BUCKET="$COMMIT_STATE_BUCKET/$REPO_NAME/$BRANCH_NAME"

detect_changed_folders() {
    gsutil cp gs://$TRACKING_BUCKET/LAST_COMMIT . &> /dev/null || true
    last_commit_sha=`cat LAST_COMMIT 2> /dev/null || git rev-list --max-parents=0 HEAD`
    echo "Detecting changes from last build: $last_commit_sha"
    folders=`git diff --name-only "$last_commit_sha" | sort -u | awk 'BEGIN {FS="/"} {print $1}' | uniq`
    export changed_components=$folders
}

run_builds() {
    echo `pwd`
    echo $(ls)
    for component in $changed_components
    do
        if ! [[ " ${IGNORE_FILES[@]} " =~ "$component" ]]; then
            echo -e "\nBuilding $component:"
            if ! [[ -f "$component/cloudbuild.yaml" ]]; then
                echo 'Skipping... cloudbuild.yaml not found'
            else
                (cd "$component" && gcloud builds submit --config=cloudbuild.yaml --async)
            fi
        fi
    done
    echo '-------------------------------------------------'
}

update_last_commit() {
    echo "Updating latest build commit"
    echo -n "$COMMIT_SHA" > LAST_COMMIT
    gsutil cp LAST_COMMIT gs://$TRACKING_BUCKET/LAST_COMMIT
}

detect_changed_folders
run_builds
update_last_commit

#! /bin/bash

export readonly PIPELINE='sw-2'
export readonly PROJECT='playground-s-11-6658c0'
export readonly BUCKET='playground-s-11-6658c'

function venv(){
    virtualenv -p python2.7 venv
    source venv/bin/activate
    pip install -r requirements.txt
}

function run(){
    source venv/bin/activate
    gcloud beta emulators datastore start &
    sleep 4
    $(gcloud beta emulators datastore env-init)
    python pipeline.py --input ../dataset/people.json --kind people --dataset ${PROJECT}
}

function template(){
    source venv/bin/activate
    python pipeline.py --runner DataflowRunner --project ${PROJECT} --staging_location "gs://${BUCKET}/staging" --temp_location "gs://${BUCKET}/temp" --template_location "gs://${BUCKET}/templates/${PIPELINE}-template"
}

function job(){
    source venv/bin/activate
    python pipeline.py --runner DataflowRunner --project ${PROJECT} --staging_location "gs://${BUCKET}/staging" --temp_location "gs://${BUCKET}/temp" --input "gs://${BUCKET}/dataset/people.json" --output "gs://${BUCKET}/output/names"
}

$@

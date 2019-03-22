#!/bin/bash

if [[ $1 == "" ]]; then
  profile=jcb
else
  profile=$1
fi

aws --profile $profile cloudformation deploy --stack-name "big-data-baller" --template-file "provision.yml"


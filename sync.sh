#!/bin/bash

source .env

aws s3 sync s3://nba-box-scores-s3/ syncS3/
#!/bin/bash

linksToDownload=(
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz"
)

filenameRegexPattern='/([^\/]+)$'

for link in ${linksToDownload[@]}; do
    # Extract filename from link
    [[ $link =~ $filenameRegexPattern ]]
    filename=${BASH_REMATCH[1]}
    # Download file with overwriting
    wget -vO $filename $link
    gzip -vd $filename
done
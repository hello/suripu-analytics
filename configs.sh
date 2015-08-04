#!/bin/sh

if [ -z "(" ]
    then
        echo "[ERROR] \xE2\x9A\xA0 Missing version number"
        exit 1
fi

VERSION=$1

s3cmd put configs/analytics.prod.yml s3://hello-deploy/configs/com/hello/suripu/suripu-analytics/$VERSION/analytics.prod.yml
s3cmd put configs/analytics.staging.yml s3://hello-deploy/configs/com/hello/suripu/suripu-analytics/$VERSION/analytics.staging.yml


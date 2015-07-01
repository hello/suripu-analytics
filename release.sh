#!/bin/sh

if [ -z "$1" ]
    then
        echo "[ERROR] \xE2\x9A\xA0 Missing version number"
        exit 1
fi

VERSION=$1

mvn clean test package

# prod
s3cmd put target/suripu-analytics-$VERSION.jar s3://hello-maven/release/com/hello/suripu/suripu-analytics/$VERSION/suripu-analytics-$VERSION.jar
s3cmd put configs/analytics.prod.yml s3://hello-deploy/configs/com/hello/suripu/suripu-analytics/$VERSION/analytics.prod.yml


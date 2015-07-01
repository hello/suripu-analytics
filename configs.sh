#!/bin/sh

if [ -z "(" ]
    then
        echo "[ERROR] \xE2\x9A\xA0 Missing version number"
        exit 1
fi

VERSION=$1

# pardon the copy pasta but I don't know shit about bash

# prod

s3cmd put configs/analytics.prod.yml s3://hello-deploy/configs/com/hello/suripu/suripu-analytics/$VERSION/analytics.prod.yml


#!/usr/bin/env bash
git checkout master
git pull
mvn release:clean release:prepare

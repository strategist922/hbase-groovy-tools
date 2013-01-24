#!/bin/bash

export HBASE_CLASSPATH=groovy-1.8.8/lib/*
export HBASE_OPTS="-XX:+UseConcMarkSweepGC"
/usr/bin/env hbase groovy.ui.GroovyMain $@

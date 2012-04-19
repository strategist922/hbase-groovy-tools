#!/bin/bash

export HBASE_CLASSPATH=/usr/share/groovy/lib/*
/usr/bin/env hbase groovy.ui.GroovyMain $@

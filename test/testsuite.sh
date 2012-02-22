#!/bin/sh

node test/rebuild.js
node node_modules/nodeunit/bin/nodeunit test/test-casio.js $@
#!/bin/bash

ansible all --become -m shell -a "find /mnt/local-cache/tempdir/ -mindepth 1 -delete"

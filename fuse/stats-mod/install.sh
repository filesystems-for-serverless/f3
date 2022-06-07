#!/bin/bash

ansible all -m copy -a "src=fuse-stats.ko dest=/usr/local/bin/fuse-stats.ko" --become

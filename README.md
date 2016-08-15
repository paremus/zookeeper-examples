# The Paremus Service Fabric ZooKeeper examples

This repository contains example applications for the Paremus Service Fabric. All examples 
can be built locally, or release versions are available from https://nexus.paremus.com. Instructions for running these examples on the Paremus Service Fabric may be found at https://docs.paremus.com/display/SF113/Tutorials.

All sources in this repository are provided under the Apache License Version 2.0

## The `distributed-lock-1.13.x` branch

The `Distributed Lock` application is targetted for version 1.13.x of the Paremus Service Fabric

The `Distributed Lock` application uses the Apache Curator framework to talk to an Apache ZooKeeper ensemble. The application contains multiple workers that use ZooKeeper as a distributed lock, ensuring that only one worker operates at a time. The actions of the workers are sent to a JavaScript UI using Web Sockets.

See: https://docs.paremus.com/display/SF113/Ensembles+and+the+Fabric+Ensemble+Manager


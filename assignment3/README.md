# Student Information
Author: Gilles Kepnang 
Email: gkepnan1@jhu.edu

# Programming Assignment 3 - HDFS Java API
In this assignment you will modify a maven project by adding code to implement file system functions using the HDFS Java API.

## Maven Aspects
For this project, I have added the hadoop dependencies to your pom and I have also included a plugin for building a "fat" jar. This will be needed as you add dependencies in your code. 

# Build and Run specification
```
$ mvn clean package
For local filesystem
$ java -cp target/assignment3-1.0-SNAPSHOT-jar-with-dependencies.jar edu.jhu.bdpuh.App -lsr .
To use hdfs
$ hadoop jar target/assignment3-1.0-SNAPSHOT.jar edu.jhu.bdpuh.App -lsr .
```

# About this exercise
To introduce you to the HDFS Java API, I am asking you to implement a command line interface. 

# Instructions
1. Fill out the Student Information section above with your Name and jhu email id.
1. Validate this project by following the build and run steps shown above (the ls api is already implemented). There should be no errors when you build and run.
1. Modify App.java to implement the cli described in the CLI Specification section
1. Submit your assignment:
   1. Push your changes to gitlab.
   1. Download the tar.gz archive of your project
   1. Rename the archive using your jhu username (e.g. my submittal would be pwilso12.tar.gz). 
   1. submit to blackboard

# Command Line Interface Specification
Implement a few of the commands from the HDFS shell. Try to match the functionality provided. For example, the -ls command shows the file permissions and your should show those too. You should handle error conditions, especially user errors. If there are API challenges implementing the full spec, please make note of this in your comments.

Command | Notes
----------|---------------
\-ls \[\-d\] \[\-h\] \[\-R\] \[\<path\> ...\] | You have a partially working version. Improve it to support the options
\-cat \[\-ignoreCrc\] \<src\> | Simply cat the file. What is that ignorecrc flag about? 
   The ignoreCrc flag skips the checksum verification, while comparing the file to stdout. 
   I added code to check for "hdfs://" prefix for hdfs object.
\-get \[\-p\] \[\-ignoreCrc\] \[\-crc\] \<src\> ... \<localdst\>| Get files from hdfs and save them locally
\-rm \[\-f\] \[\-r&#124;\-R\] \[\-skipTrash\] \<src\> ...| Remove files from hdfs

# Student Observations
This homework was very insightful. I enjoyed learning about Hadoop FileSystem, FileChecksum, FileStatus, etc. 
With this Java API, I see how Hadoop filesystem operations are performed. I also see that we have the power to customize Hadoop operations and perform the operations we deem necessary on Hadoop Filesystem. 
My only hang-up was the set up before using HDFS. Implmenting the commands of this CLI specification was easy and straightforward. 


## Problems Encountered / how you resolved them
I launched 5 different virtual machines and they each crashed. The issue was that I needed more RAM for the virtual machine. Instead of 10GB, I added 40GB.
I had issues with the internet connection was not set up properly. So I found YouTube videos on virtualBox network settings.
I also had issues with Cloudera vm, as it has different permissions within the CentOS image. It did not allow me to setup Apache Hadoop as I wanted. So instead, I created a VDI with 40GB.
Now in the implementation, I got all the functionality going properly. That was the easy part of this project!! 
But for me, the setup was full of trials and challenges for me. The actual implmementation only took two days.

As consolation, I created the mkdir functionality to make up for my delay in submission. May Professor consider this a proof of understanding these concepts.

## Resources you found helpful
Google 

## Describe any help you received
Apache Hadoop website
Emails to Chance
YouTube videos on network reconnection of CentOS VM on VirtualBox

## Make recommendations for improvement
Have a FAQ section to address common HDFS issues (e.g. DataStreamer Exception, temp files for namenode and datanode, etc.)

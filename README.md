# Spark Search Pipeline

This is an Assessed Coursework for the [Big Data](https://www.gla.ac.uk/postgraduate/taught/computingsciencemsc/?card=course&code=COMPSCI5088) postgraduate course at the [University of Glasgow](https://www.gla.ac.uk) during the academic year 2022-2023.

## Coursework Specification

### Summary

The goal of this exercise is to familiarize yourselves with the design, implementation and performance testing of Big Data analysis tasks using Apache Spark. You will be required to design and implement a single reasonably complex Spark application. You will then test the running of this application locally on a data of various sizes. Finally, you will write a short report describing your design, design decisions, and where appropriate critique your design. You will be evaluated based on code functionality (does it produce the expected outcome), code quality (is it well designed and follows good software engineering practices) and efficiency (how fast is it and does it use resources efficiently), as well as your submitted report.

### Task Description

You are to develop a batch-based text search and filtering pipeline in Apache Spark. The core goal of this pipeline is to take in a large set of text documents and a set of user defined queries, then for each query, rank the text documents by relevance for that query, as well as filter out any overly similar documents in the final ranking. The top 10 documents for each query should be returned as output. Each document and query should be processed to remove stopwords (words with little discriminative value, e.g. ‘the’) and apply stemming (which converts each word into its ‘stem’, a shorter version that helps with term mismatch between documents and queries). Documents should be scored using the [DPH ranking model](https://github.com/terrier-org/terrier-core/blob/5.x/modules/core/src/main/java/org/terrier/matching/models/DPH.java). As a final stage, the ranking of documents for each query should be analysed to remove unneeded redundancy (near duplicate documents), if any pairs of documents are found where their titles have a textual distance (using a comparison function provided) less than 0.5 then you should only keep the most relevant of them (based on the DPH score). Note that there should be 10 documents returned for each query, even after redundancy filtering.

## Results

### Efficiency

The execution of the full dataset (~5GB) and with a set of ten queries using four local executors took ~69 seconds.

## Note

* The [dataset](https://trec.nist.gov/data/wapost/) was too large to include in the project, but it can be accessed from [here](https://ir-datasets.com/wapo.html).

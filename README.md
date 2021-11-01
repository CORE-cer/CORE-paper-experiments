# CORE-experiments

This repository contains the code needed to reproduce the experiments found in the paper.

# How to run

## Requirements

- Python >= 3.8
- Java >= 11

## Building the systems

Included is the code we used to run ESPER, FLINK and SASE.

### ESPER

Esper source contains a maven project. Just package the distribution with dependencies and move ``` espertest-1.0-SNAPSHOT-jar-with-dependencies.jar ``` to the ``` jars ``` folder, and rename it ``` esper8.jar ```.

### FLINK

Flink contains a gradle project. Because of a library that flink uses, it is unable to be compiled normally into a fat jar. So, run the gradle command ``` distZip ``` and unzip the contents in ``` flink-1.0-SNAPSHOT``` into the ```jars``` folder.

### SASE

For SASE, just compile manually an artifact with all the dependencies included, move it to the ```jars``` folder and rename it ```sase.jar```

### CORE

The CORE code that was used in the experiments can be found in the CORE repository under the ```experiments``` branch. After compiling it, move it to the ``` jars ``` folder and rename it ```core.main.jar```

## Running the experiments

The experiments scripts are located in the ```scripts``` folder. There's 6 included experiments:
 - Sequence: Asks for a simple sequence in a stream. For example A then B then C.
 - Kleene: Asks for a sequence with kleene terms in between. For example A, then one or more B, then C.
 - Sequence no last event: Asks for a simple sequence, but the last event is not present in the stream.
 - Sequence sel: Asks for a simple sequence, but using different selection strategies.
 - Disjunction: Asks for a sequence with disjunctions in between. For example A, then either B or C, then D.
 - Stock Markets: Asks multiple queries regarding the stock market data.

All these experiments have many variables that can be changed to change the behavior of the experiments:

 - TESTS: A list that can contain either ```'Time'``` or ```'Memory'```. Note that memory tests cannot be run without first running a time test, as the end condition of the memory test depends on the time test.
 - STREAM_EVENTS: A list containing integers. It controls how many events will appear on the query.
 - STREAM_LENGTH: An integer. Controls the length of the stream.
 - WIN_LENGTH: A list containing integers. Controls the length of the windows used, measured in events.
 - NOISE_EVENTS: A list containing integers. Controls how many events will appear on the stream that are not part of the query.
 - ITERATIONS: An integer. How many iterations of each test will be ran.
 - TIMEOUTS: A list containing how many (real) seconds the scripts will give each system to process the data stream. Note that this does not include the time pre-processing the stream (i.e. converting the text file into ```Event``` objects and storing them in a list)
 - SYSTEMS: A list containing which systems the experiment shall use.
 - TEST_NAME: Name of the folder that will be created in the ```results``` folder.
 - CONSUME: Should not be changed
 - NUM_EVENT_DICT: Should not be changed

After running the experiments, the data will be stored in the ```results``` folder. To make it more human readable, there's also two other scripts, ``` make_csv.py``` and ```make_csv_stock.py``` which will take the data from the ```results``` folder and convert it into a csv for easier reading. Note that when running there scripts you will have to change the variable ```TEST_NAMES``` to the folder name of the tests that were ran.

Note that all the scripts are made to be run from the folder this README is in as cwd.


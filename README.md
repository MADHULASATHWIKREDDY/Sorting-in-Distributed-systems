
# Sorting in Distributed Systems

What if you have a large data file that needs to be sorted but you can't fit it into one server memory? To solve this problem, I have implemented a networked sort algorithm using basic socket level programming on TCP/IP. Unique features include

- concurrency control methods using go routines or threads.
- Data partition algorithm

    values are mapped to servers using the first n bits if we have 2^n servers. Eg: 110101011 is mapped to Server 3 if we have 4 servers. 001101010 is mapped to server 0.

- Network protocol

    Will send 100 bytes along with a stream complete bit. After sending all data, send an additional datapoint with Stream complete byte=1. It is send additionally because if one server has no data, then it can still send the Extra data point is say it has completed all the transfer.

- low overhead (uses socket level)
## Usage

```bash
  netsort <serverId> <inputFilePath> <outputFilePath> <configFilePath>
```

- serverId: integer-valued id starting from 0 that specifies which server YOU are. For example, if there are 4 servers, valid values of this field would be {0, 1, 2, 3}.
- inputFilePath: input file
- outputFilePath: output file
- configFilePath: path to the config file

## Testing and Dependencies

While we could run each server on its own physical machine (or virtual machine), it is easier to simply run multiple server processes, each with its own TCP port and separate directory to store input/output files. This can tested using the src/run-demo.sh. To run, 

```bash
  sh run-demo.sh
```
It is advised to navigate to "src" directory before running the shell file as it automatically installs all the dependencies

## Verifying  sort implementation

### Setup
Concatenate all the input files into a file called INPUT

```
$ cp input-0.dat INPUT
$ cat input-1.dat >> INPUT
$ cat input-2.dat >> INPUT
$ cat input-3.dat >> INPUT
```

Concatenate all the output files into a file called OUTPUT
```
$ cp output-0.dat OUTPUT
$ cat output-1.dat >> OUTPUT
$ cat output-2.dat >> OUTPUT
$ cat output-3.dat >> OUTPUT
```
```
$ bin/showsort INPUT | sort > REF_OUTPUT
$ bin/showsort OUTPUT > my_output
$ diff REF_OUTPUT my_output
```



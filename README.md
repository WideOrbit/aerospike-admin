# Aerospike Admin
## Description
Aerospike Admin provides an interface for Aerospike users to view the stat
of their Aerospike Cluster by fetching information from running cluster (Cluster mode) or logs (Log-analyser mode).
Start the tool with *asadm* and run the *help* command to get started.

## Installing Aerospike Admin
```
make
sudo make install
```

## Running Aerospike Admin in Cluster Mode
asadm -h <Aerospike Server Address>
Admin> help

## Running Aerospike Admin in Log-analyser Mode
asadm -l [-f <location of logs>]
Admin> help

## Dependencies
- python 2.6+ (< 3)

## Tests
### Dependencies
- unittest2: 0.5.1
- Mock: 1.0.1

### Running Tests
./run_tests.sh or unit2 discover

## Profiling
### Dependencies
- yappi: 0.92

### Run Profiler
asadm --profile
Do not exit with 'ctrl+c' exit with the *exit* command

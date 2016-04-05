## Generating a configuration file

`mkconfig.py` accepts the following options

#### `--reporting-threshold`

Signals, in theory, report each minute. In practice this is not the
case---either the sensor is down, or our collection device is down.
Thus, average reporting rates are slightly greater than 60 seconds.

This value denotes the upper limit (in seconds) on a signals average
reporting time, for that signal to be considered "valid." That is, for
a signal to be included in the set of signals for which predictions
are made. Sensors with reporting rates greater-than the value
specified via this parameter will not be members of that set.

Caveats:

1. During prediction, neighboring signals may be used when building
   feature vectors. The neighbor set is made irrespective of of signal
   reporting times. Thus, this value is only pertinent to which
   signals are choosen for prediction.

2. The desired reporting threshold actually augments the view of the
   database that processes see. Thus, a "run" of the system can only
   be over a single reporting-threshold.

#### `--output-directory`

The top-level directory into which configuration files will be written

#### `--skeleton-file`

An existing INI file on which to base this configuration. What is
produced by `mkconfig.py` is essentially concatenated to this. This
file will not be overwritten.

####  `--parallel` or `--sequential`

Whether this configuration file is for a parallel run or a sequential
run. A parallel run is one in which Python handles farming out signal
IDs to processes; during a sequential run signal IDs are specified in
the configuration file, and Python runs the predictor for just that
ID.

####  --verbose or --no-verbose

Produce configuration files quietly, or provide feedback. In either
case, errors are reported.

## Running the system

`main.py` handles running the prediction engine. If this is a parallel
run (see `--parallel` or `--sequential` in the previous section), then
per-signal prediction is farmed out across locally available
processors (processors available on the current core). If this is a
sequential run, then multi-processing is disabled, and prediction is
run for the single signal specified in the configuration file.

`main.py` accepts the following options

#### `--config`

Location of the configuration file

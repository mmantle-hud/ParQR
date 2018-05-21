# ParQR (Parallel Qualitative Reasoner)

ParQR is an Apache Spark application written using scala. ParQR accepts 4 input arguments:

* The location of the input qualitative constraint network
* The location of configuration file that specifies a binary qualitative calculus
* An integer that specifes the level of parallelism i.e. the number of tasks to run in parallel
* The strategy used for joining relations. This needs to be 'linear' or 'smart'. The default is 'smart'.

Examples of calculi specification and input QCNs can be found in the calculi and input folders.

# mpiqueue: queuing lots of small jobs via MPI #

mpiqueue is a small program designed to help computational scientists
that run hundreds or thousands of small jobs work efficiently on
clusters that only allow the user to submit a small number of jobs.

The policies of the cluster I use were written with large multicore jobs
in mind.  On this cluster, a single user may only have five jobs running
and a total of ten jobs queued.  This works well for computational jobs
that use a large number of cores and run for a long time.  Long-running
molecular dynamics simulations are quite at home in this environment. 
My work is not.  My work typically involves running a large number of
short single-core jobs that typically perform stochastic.  Each job can
run for only a few minutes or hours, but we need a large number of them
to collect statistics.  Each job is also completely independent, and
requires no communication with any other job.

The simplest way to queue these jobs either individually or as a job
array.  The queuing system can then schedule and process each job (or
task) separately.  This is not possible on clusters that only allow ten
submitted jobs.  mpiqueue presents a solution to this problem by
allowing you to queue many small jobs masqueraded as a single large job.
 It uses MPI to present a single queue to the user.

## Usage

mpiqueue is invoked from `mpirun`.  The options to give `mpirun` depend
on the installation and batch queuing system.  Under the 
[Moab cluster suite][moab-docs], you can use the following template:

		#! /bin/bash
		#MOAB -l nodes=<NNODES>:ppn=<PROC_PER_NODE>
		#MOAB -E
		#MOAB -V

		cd <LOG_DIR>
		/path/to/mpirun /path/to/mpiqueue <PREFIX> /path/to/sim <TASKLIST>...

Where:

* `<NNODES>` is the number of nodes
* `<PROC_PER_NODE>` is the number of processors to use on each node
* `<LOG_DIR>` is the directory where the log files should be written
* `<PREFIX>` is the job log prefix
* `/path/to/sim` is a path to a command or script that takes the task
  number as a single argument
* <TASKDESC>...` are one or more task list arguments

mpiqueue will then run one process one each requested processor 
(`NNODES * PROC_PER_NODE` total processors).  One of the processes is
appointed queue master.  The queue master constructs a task list from
the command line and then dispatches tasks to itself and the other
processes.  When an mpiqueue process receives a task, it spawns the
simulation `/path/to/sim` and passes it a single argument: a task
number.  When the simulation command finishes, another will be spawned
until all of the tasks have been processed.  The command `/path/to/sim`
should return an error code based on whether the simulation completed:
zero if successful and non-zero if unsuccessful.

Each mpiqueue process logs queuing activity to`<PREFIX>_<RANK>.metalog`, 
where `<RANK>` is the rank of the mpi process.  Job stdout and stderr are 
logged to `<PREFIX>_<TASK>.log`.

## Task lists

Currently, two kinds of task lists are supported: ranges and files.  A
range task list is given as an inclusive range of two non-negative
numbers:

    `range:M-N`

Here `M` and `N` are non-negative integers.  This will add all tasks
from `M` to `N`, including `M` and `N`, to the task list.

File tasks is given as a path:

    `file:/path/to/tasklist.txt`
    
mpiqueue parses each line of the file as an integer and adds that
integer to the task list.  The integer must be non-negative.  The
numbers in the file may be padded with whitespace, and the file may have
empty lines.  Comments begin with the `#` character and continue until
the end of the line.

## Assumptions

mpiqueue assumes the following cluster environment:

1. The cluster has [MPI-2][mpi2-link].  Note that the code has only been
   tested against Open-MPI.
2. A POSIX.1-compliant operating system.  mpiqueue currently uses fork/exec.
3. A shared file system.  mpiqueue runs subscripts and writes log files
   assuming that the filesystem will be shared between nodes.

## Implementation details

The queue master is appointed by its rank.  The process with MPI rank of
zero is appointed to be queue master.  It will parse the task lists and
signal the other mpiqueue processes to begin.  Then queue master then
executes a loop:

1. Checks for messages from another mpiqueue process.  A message either
requests a first task or reports completion of a task.  In both cases, a
new task is dispatched to the mpiqueue process.

	* If a task is completed, logs whether it was successful or not.
	* Dispatch a new task to the process.  If no new task is available,
	  tell the mpiqueue process to shut down.

2. When no messages are available, the queue master checks if it has
spawned a simulation subprocess.

	* If a subprocess has finished, the queue master logs whether it was
	  successful or not.
	* If no subprocess is running, the queue master spawns a subprocess
	  to run in parallel.

3. Sleep for 5000 microseconds.

4. Stop if the queue is empty and no tasks are currently dispatched to either another process or to a subprocess.

Each non-master mpiqueue process is called a runner, and does the following:

1. Wait for the signal from the queue master to begin.
2. Request the first task from the queue master.
3. Run the task.
4. Report the result to the queue master.
5. Request a new task from the queue master.
6. Stop if no tasks are available.  Otherwise, go to step 3.

## License ##

mpiqueue is Copyright (c) 2014 by Shannon F. Stewman and licensed under the [MIT license][mit-license].  See the file LICENSE for details.

## References

  [mpi2-link]: http://www.mpi-forum.org/
  [moab-docs]: http://docs.adaptivecomputing.com/
  [mit-license]: http://opensource.org/licenses/MIT

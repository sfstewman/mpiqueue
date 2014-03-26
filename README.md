# mpishunt #

## Problem ##

Situation:

1. Users are restricted to submitting a small number of jobs

2. Jobs may use many processors

3. Many of our jobs are single core but require hundreds or thousands of
   runs to collect statistics.

The queuing system on the UIC Extreme cluster is designed for
multiprocessor jobs.  This means that the administrators restrict the
number of jobs each user can submit

The scheduler can essentially only be used in a coarse way.

## Solution ##

Use the scheduler to allocate a block of cores and then use MPI to queue
jobs to each core.

## Description ##

Uses MPI to queue tasks.

<EXPAND>

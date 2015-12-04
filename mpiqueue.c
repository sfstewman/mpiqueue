/* mpiqueue: running small, single-core jobs on a cluster biased against them
 *
 * Copyright 2014, Shannon F. Stewman
 *
 * Released under the MIT License.  See LICENSE for details.
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>

#define VERSION "0.2.1"

#if !defined(VERSION)
#  define VERSION "unknown"
#endif /* !defined(VERSION) */

#if !defined(REV_SHA)
#  define REV_SHA "unknown"
#endif /* !defined(REV_SHA) */

/* warning!  BSD but not POSIX */
#include <sys/wait.h>
#include <mpi.h>

int master_runs_jobs = 1;

enum { DEBUG_FLAG = 0 };

typedef struct taskinfo taskinfo;
struct taskinfo {
  int taskid;
  int runner;
  int errcode;
};

typedef struct tasklist tasklist;
struct tasklist {
  int num;
  int max;

  int *taskids;
  // int *runners;
  // int *errcodes;
  // taskinfo *tasks;
};

typedef struct dispatcher dispatcher;
struct dispatcher {
  tasklist *tasks;
  int numdispatched;
  int currtask;

  int info;
  // taskinfo *info;
  pid_t child;
};

static int everything_die(const char* fmt,...)
{
  va_list args;
  va_start(args,fmt);
  vfprintf(stderr,fmt,args);
  va_end(args);
  MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
  return 0;
}

enum {
  TASKLIST_SIZE0    =  1024,
  TASKLIST_DBLSIZE  =  8192,
  TASKLIST_HALFINCR = 65536,
};

static void tasklist_resize_if_at_capacity(tasklist *list, int minsize)
{
  int new_max;
  int *tmp;

  if (list->num < list->max) { return; }

  if (list->max == 0) {
    new_max = TASKLIST_SIZE0;
  } else if (list->max < TASKLIST_DBLSIZE) {
    new_max = 2*list->max;
  } else if (list->max < TASKLIST_HALFINCR) {
    /* increase by 50% */
    new_max = list->max + list->max/2;
  } else {
    /* increase by 25% */
    new_max = list->max + list->max/4;
  }

  if (new_max < minsize) {
    new_max = minsize;
  }

  tmp = realloc(list->taskids, 3*new_max*sizeof(list->taskids[0]));
  if (tmp == NULL) {
    everything_die("could not resize task list to %d items\n",new_max);
  }

  list->max = new_max;
  list->taskids  = tmp;  tmp += new_max;
  // list->runners  = tmp;  tmp += new_max;
  // list->errcodes = tmp;
}

tasklist *tasklist_new(int minsize)
{
  tasklist *list = malloc(sizeof(*list));

  if (list == NULL) {
    everything_die("could not allocate task list\n");
  }

  list->num = 0;
  list->max = 0;
  list->taskids = NULL;
  // list->runners = list->errcodes = NULL;

  /* allocate initial elements */
  tasklist_resize_if_at_capacity(list,minsize);

  return list;
}

void tasklist_add_task(tasklist *list, int taskid)
{
  tasklist_resize_if_at_capacity(list, TASKLIST_SIZE0);

  list->taskids[list->num++] = taskid;
}

/* some useful globals (NB: should be read-only after initialized) */
static char hostname[256];
static const char *prefix;
static const char *script;

/* output files */
static int echo_metalog = 1;
static FILE *_metalog = NULL;

static void metalog(const char* fmt, ...)
{
  va_list args;
  va_list args2;
  va_start(args,fmt);
  if (echo_metalog) {
    va_copy(args2,args);
    vfprintf(stdout,fmt,args2);
    fflush(stdout);
  }
  vfprintf(_metalog,fmt,args);
  va_end(args);
  fflush(_metalog);
}

static inline const char* parserange_number(const char *spec, int* n)
{
  if ((*spec < '0') || (*spec > '9')) { return NULL; }

  *n = 0;
  while(*spec) {
    if ((*spec >= '0') && (*spec <= '9')) {
      *n = 10 * (*n) + (*spec - '0');
      spec++;
    }
    else if (*spec == '-') {
      return spec;
    }
    else {
      return NULL;
    }
  }

  return spec;
}

#define RANGE_PREFIX "range:"
#define RANGE_PREFIX_LEN (sizeof(RANGE_PREFIX)-1)
static int parserange(const char *desc, tasklist *list)
{
  const char *spec;
  int i, ind0, ind1;

  if (strstr(desc,RANGE_PREFIX)!=desc) { return 0; }
  spec = desc + RANGE_PREFIX_LEN;

  spec = parserange_number(spec, &ind0);
  if (!spec || (*spec != '-')) {
    everything_die("invalid range specification: \"%s\"\n",desc);
  }

  spec++;  // skip over '-'
  spec = parserange_number(spec, &ind1);
  if (!spec || *spec) {
    everything_die("invalid range specification: \"%s\"\n",desc);
  }

  if ((ind0 < 0) || (ind1 < ind0)) {
    everything_die("invalid range specification: \"%s\"\n",desc);
  }

  for (i=ind0; i <= ind1; i++) {
    tasklist_add_task(list, i);
  }

  return 1;
}
#undef RANGE_PREFIX
#undef RANGE_PREFIX_LEN

#define FILE_PREFIX "file:"
#define FILE_PREFIX_LEN (sizeof(FILE_PREFIX)-1)
static int parsefile(const char *desc, tasklist *list)
{
  static char buf[2048];
  const char *path;
  char *beg, *end;
  FILE* f;
  int line;
  long ind;

  if (strstr(desc,"file:") != desc) { return 0; }
  path = desc + FILE_PREFIX_LEN;

  metalog("parsing task list from file %s\n", path);

  if ((f = fopen(path,"r")) == NULL) {
    everything_die("error opening task file %s: %s\n", path,strerror(errno));
  }

  line = 0;
  while(fgets(buf,sizeof(buf),f) != NULL) {
    line++;

    /* remove leading whitespace */
    beg = buf;
    while(isspace(*beg)) { beg++; }
    if (*beg == '\0') {
      continue;
    }

    /* remove comments */
    if ((end = strchr(beg,'#'))) {
      *end = '\0';
    }

    /* remove trailing whitespace */
    end = beg + strlen(beg) - 1;
    while (isspace(*end)) {
      *end-- = '\0';
	  }

    if (beg[0] == '\0') { continue; }

    metalog("line %d, parsing: '%s'\n", line,beg);

    /* convert to a number */
    end = NULL;
    ind = strtol(beg, &end, 0);
    if (*end != '\0') {
      return everything_die("line %d in file %s is not a number\n", line, path);
    }

    if ((ind < 0) || (ind > INT_MAX)) {
      return everything_die("line %d in file %s is not a valid task index\n", line, path);
    }

    tasklist_add_task(list, (int)ind);
  }

  if (ferror(f)) {
    return everything_die("error reading tasks at line %d of file %s:\n",
        line, path, strerror(errno));
  }

  return 1;
}
#undef FILE_PREFIX
#undef FILE_PREFIX_LEN

static tasklist *parsetasks(int csize, int ndesc, char **taskdesc)
{
  int i;
  tasklist *list = tasklist_new(csize);

  for(i=0; i < ndesc; i++) {
    metalog("parsing '%s'\n", taskdesc[i]);
    if (parserange(taskdesc[i],list)) { continue; }
    if (parsefile(taskdesc[i],list))  { continue; }

    everything_die("ERROR: unknown task description: %s\n", taskdesc[i]);
  }

  /* If csize > num tasks, then fill in the remaining items with -1
   * so the runners will shut down after the scatter
   */
  for (i=list->num; i < csize; i++) {
    list->taskids[i] = -1;
  }

  /*
  for (i=0; i < list->num; i++) {
    list->runners[i] = -1;
    list->errcodes[i] = 0;
  }
  */

  return list;
}

static void taskinfo_report(int rank, taskinfo *info)
{
  const char *pfx ="";
  if ((rank == 0) && (info->runner != 0)) { pfx = "MASTER: "; }

  if (info->taskid == -1) {
    metalog("%sRUNNER %d READY\n", pfx, info->runner);
  }
  else {
    if (info->errcode == EXIT_SUCCESS) {
      metalog("%sRUNNER %d FINISHED TASKID %d\n",
          pfx, info->runner, info->taskid);
    } else {
      metalog("%sRUNNER %d ERROR in TASKID %d CODE = %d\n",
          pfx, info->runner, info->taskid, info->errcode);
    }
  }
}

static int taskinfo_is_ready(int tag, int *srcp, int *tagp)
{
  MPI_Status status;
  int has_mesg = 0;
  MPI_Iprobe(MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &has_mesg, &status);
  if (has_mesg) {
   if (srcp) { *srcp = status.MPI_SOURCE; }
   if (tagp) { *tagp = status.MPI_TAG;    }
  }
  return has_mesg;
}

static void taskinfo_receive(int src, int tag, taskinfo *info)
{
  int comm_buf[2] = { 0, 0 };
  MPI_Status status;

  memset(&status,0,sizeof(status));
  MPI_Recv(comm_buf+0, 2, MPI_INT, src, tag, MPI_COMM_WORLD, &status);

  info->runner  = status.MPI_SOURCE;
  info->taskid  = comm_buf[0];
  info->errcode = comm_buf[1];
}

static void taskinfo_send(int src, int tag, taskinfo *info)
{
  int comm_buf[2];

  if (info) {
    comm_buf[0] = info->taskid;
    comm_buf[1] = info->errcode;
    if (src != 0) {
      metalog("MASTER DISPATCHING TASKID %d TO RUNNER %d\n", comm_buf[0], src);
    }
  } else {
    comm_buf[0] = -1;
    comm_buf[1] =  0;
    if (src != 0) {
      metalog("MASTER SENDING SHUTDOWN TO RUNNER %d\n", src);
    }
  }

  MPI_Send(comm_buf+0, 2, MPI_INT, src, tag, MPI_COMM_WORLD);
}

static inline int dispatcher_num_tasks(dispatcher *disp)
{
  return disp->tasks->num - disp->currtask;
}

static inline int dispatcher_has_tasks(dispatcher *disp)
{
  return dispatcher_num_tasks(disp) > 0;
}

static int dispatcher_next_task(dispatcher *disp)
{
  return dispatcher_has_tasks(disp) ? disp->tasks->taskids[disp->currtask++] : -1;
}

/* each runner sends a message with two ints:
 * comm_buf[0] -- taskid (-1 if no current task)
 * comm_buf[1] -- error code (EXIT_SUCCESS or EXIT_FAILURE)
 *
 * master sends a return reply to dispatch a new task:
 * comm_buf[0] -- new task id (-1 if none available)
 */
enum { SHUNT_TAG = 1 };

static void dispatcher_handle_runners(dispatcher *disp)
{
  int src, tag;

  src = tag = 0;
  while (taskinfo_is_ready(SHUNT_TAG, &src, &tag)) {
    taskinfo info;
    int next, runner;

    metalog("MASTER: message tag=%d from %d\n", tag, src);

    memset(&info,0,sizeof(info));
    taskinfo_receive(src, tag, &info);
    taskinfo_report(0, &info);

    if (info.taskid != -1) { disp->numdispatched--; }

    runner = info.runner;
    next = dispatcher_next_task(disp);
    info.runner  = -1;
    info.errcode = 0;
    info.taskid  = next;

    if (next >= 0) {
      taskinfo_send(runner, SHUNT_TAG, &info);
      disp->numdispatched++;
    } else {
      taskinfo_send(runner, SHUNT_TAG, NULL);
      metalog("MASTER: %3d runners dispatched\n",disp->numdispatched);
    }
  }
}

static void taskinfo_fork_and_exec(const char *log_filename, int taskid)
{
  char arg[64];
  FILE* log;
  int fd;

  log = fopen(log_filename,"a");
  fd = fileno(log);

  if (fd < 0) {
    metalog("ERROR opening log file (%d): %s\n", 
        errno, strerror(errno));
    goto on_error;
  }

  if (dup2(fd,1) < 0) { 
    metalog("ERROR redirecting log file to stdout (%d): %s\n",
        errno, strerror(errno));
    goto on_error;
  }

  if (dup2(fd,2) < 0) { 
    metalog("ERROR redirecting log file to stderr (%d): %s\n",
        errno, strerror(errno));
    goto on_error;
  }

  snprintf(arg, sizeof(arg), "%d", taskid);
  execlp(script, script, arg, NULL);
  /* execlp only returns on an error */
  metalog("ERROR in exec of \"%s\" (%d): %s\n",
      script, errno, strerror(errno));

on_error:
  if (fd >= 0) { fclose(log); }
  exit(1);
}

enum { DISPATCH_WAIT = 0, DISPATCH_NOWAIT = 1 };
static int taskinfo_dispatch(int rank, taskinfo *info, int nowait)
{
  /* open log file */
  int taskid;
  pid_t child;
  char log_filename[FILENAME_MAX+1];

  taskid = info->taskid;
  snprintf(log_filename,sizeof(log_filename), "%s_%d.log",prefix,taskid);
  metalog("RUNNER %d TASKID %d LOG %s\n", rank, taskid, log_filename);

  child = fork();
  if (child == -1) {
    metalog("ERROR in fork() %d: %s\n", errno, strerror(errno));
    return -1;
  } else if (child > 0) {
    /* parent process */
    if (nowait) {
      return (int)child;
    } else {
      int stat = 0;
      waitpid(child,&stat,0);
      info->errcode = WIFEXITED(stat) ? WEXITSTATUS(stat) : -1;
      return info->errcode;
    }
  } else {
    taskinfo_fork_and_exec(log_filename, taskid);
    return 0;
  }
}

/* dispatcher_handle_child
 *
 * Checks whether the task of the root process (rank=0) has completed.
 * If the task has completed, then tries to dispatch another.
 *
 * Inputs:
 *   rank         MPI rank of process (should always be zero)
 *   currtask     current task index
 *   childp       pointer to child pid
 *
 * Output: new current task index
 */
static void dispatcher_handle_child(dispatcher *dispatcher, int task)
{
  int rank, stat;
  taskinfo info;

  rank = 0;

  /* ... check if we're done ... */
  if ((task < 0)
      && (dispatcher->child == 0)
      && !dispatcher_has_tasks(dispatcher)) {
    return;
  }

  if (dispatcher->child > 0) {
    /* check if child is still working ... */
    stat = 0;
    if (waitpid(dispatcher->child,&stat,WNOHANG) == 0) { return; }

    dispatcher->numdispatched--;
  }

  dispatcher->child = 0;

  /* otherwise, try to dispatch new child */
  if (task < 0) {
    if (!dispatcher_has_tasks(dispatcher)) {
      metalog("RUNNER %d DONE\n", rank);
      return;
    }

    dispatcher->info = dispatcher->currtask++;
    task = dispatcher->tasks->taskids[dispatcher->info];
  }

  info.taskid = task;
  info.runner = rank;
  info.errcode = 0;

  dispatcher->child = taskinfo_dispatch(rank, &info, DISPATCH_NOWAIT);
  if (dispatcher->child < 0) { return; }

  dispatcher->numdispatched++;
}

enum {
  WAIT_SECS  =       0,
  WAIT_MSECS =    5000,
  NS_PER_MS  = 1000000,
};

/* Sleeps for a while to allow other processes to work */
static inline void dispatcher_yield(void)
{
  struct timespec wait = {
    .tv_sec = WAIT_SECS,
    .tv_nsec = (long long)WAIT_MSECS*NS_PER_MS
  };
  nanosleep(&wait,NULL);
}

int sync_processes(int rank, int size, dispatcher *disp)
{
  int first,*buf;

  // MPI_Barrier(MPI_COMM_WORLD);

  first = -1;

  buf = disp ? disp->tasks->taskids : NULL;
  MPI_Scatter(buf, 1, MPI_INT, &first, 1, MPI_INT, 0, MPI_COMM_WORLD);

  if (disp) {
    int ntasks = size;
    if (ntasks > disp->tasks->num) { ntasks = disp->tasks->num; }

    disp->currtask += ntasks;
    disp->numdispatched = ntasks-1;
  }

  if (rank == 0) {
    metalog("MASTER GO (first = %d)\n", first);
  } else {
    metalog("RUNNER %d GO (first = %d)\n",rank, first);
  }

  return first;
}

void master_main(int rank, int size, tasklist *list)
{
  dispatcher disp;
  int first;

  memset(&disp,0,sizeof(disp));
  disp.tasks = list;

  metalog("STARTING MASTER on %s\n",hostname);
  metalog("MPIQUEUE VERSION is %s, revision SHA1 is %s\n",
      VERSION, REV_SHA);
  metalog("NUMTASKS is %d\n", list->num);

  /* broacast GO */
  first = sync_processes(rank, size, &disp);

  if (!master_runs_jobs && first >= 0) {
    metalog("MASTER does not run jobs: job %d pushed onto the queue\n",
        first);
    tasklist_add_task(disp.tasks, first);
    first = -1;
  }

  while(dispatcher_has_tasks(&disp) || (disp.numdispatched > 0)) {
    static int last_nd = 0, last_ntasks = 0;
    int ntasks = list->num - disp.currtask;
    if (DEBUG_FLAG || (last_nd != disp.numdispatched) || (last_ntasks != ntasks)) {
      fprintf(stderr, "MASTER: %d dispatched, %d tasks, checking for messages\n",
          disp.numdispatched, list->num - disp.currtask);
      last_nd = disp.numdispatched;
      last_ntasks = ntasks;
    }

    /* dispatch other runners */
    dispatcher_handle_runners(&disp);

    if (master_runs_jobs) {
      /* check on (and dispatch) our task */
      dispatcher_handle_child(&disp,first);
      first = -1;
    }

    dispatcher_yield();
  }

  metalog("MASTER DONE\n");
}

void runner_main(int rank, int size)
{
  taskinfo info;
  metalog("STARTING RUNNER %d on %s\n",rank,hostname);

  /* wait for GO */
  memset(&info,0,sizeof(info));
  info.taskid = sync_processes(rank,size,NULL);

  metalog("RUNNER %d FIRST TASK %d\n", rank, info.taskid);
  for (;;) {
    if (info.taskid != -1) {
      info.runner = rank;
      taskinfo_dispatch(rank, &info, DISPATCH_WAIT);
      taskinfo_report(rank, &info);
    }

    metalog("RUNNER %d REQUEST TASK\n", rank);
    taskinfo_send(0, SHUNT_TAG, &info);

    memset(&info,0,sizeof(info));
    taskinfo_receive(MPI_ANY_SOURCE, SHUNT_TAG, &info);
    metalog("RUNNER %d RECEIVED TASK %d\n", rank, info.taskid);

    if (info.taskid == -1) { break; }
  }

  metalog("RUNNER %d DONE\n", rank);
}

void print_usage_and_exit(const char *prog, int errcode)
{
  fprintf(stderr, "usage: %s [MPI args] prefix script tasklist...\n",prog);
  fprintf(stderr, "   [MPI args] are any arguments passed to MPI (ignored by %s)\n",prog);
  fprintf(stderr, "   prefix is the prefix for log files\n"
      "     each runner is given a log file named <prefix>_<runnerid>.metalog\n"
      "     each task   is given a log file named <prefix>_<taskid>.log\n"
      "\n"
      "   script is the path to an executable script to run each task\n"
      "          the script must take one argument (the task number) and\n"
      "          return whether it succeeded or failed\n"
      "   tasklist is a list of tasks that can take these forms:\n"
      "     range:M-N    a range of numbers\n"
      "     file:<path>  filename to read numbers (one per line)\n");
  exit(1);
}

void open_metalog(int rank)
{
  char metalog_filename[FILENAME_MAX+1];
  snprintf(metalog_filename,sizeof(metalog_filename), "%s_%d.metalog",prefix,rank);

  printf("RANK %d PREFIX %s METALOG  %s\n", rank, prefix, metalog_filename);

  _metalog = fopen(metalog_filename,"a");
  if (_metalog == NULL) {
    perror("ERROR opening metalog: %s");
    if (rank == 0) {
      MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    } else {
      exit(EXIT_FAILURE);
    }
  }
}

void print_announcement(const char *prog,int rank,int size)
{
  int saved_echo;
  pid_t pid;

  pid = getpid();

  saved_echo = echo_metalog;
  echo_metalog = 1;

  metalog(
      "STARTING %s (pid %d) with rank %d of size %d\n"
      "HOSTNAME %s\n"
      "PREFIX   %s\n"
      "SCRIPT   %s\n", prog, (int)pid, rank, size, hostname, prefix,script);
  /*
  if (rank == 0) {
    metalog("-------------> MASTER pid is %d\n",(int)pid);
  }
  */

  echo_metalog = saved_echo;
}

int main(int argc, char **argv)
{
  int rank, size, opt; 
  tasklist *list;

  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank); 
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  optind = 1;
  while (opt = getopt(argc,argv,"n"), opt != -1) {
    switch (opt) {
      case 'n':
        master_runs_jobs = 0;
        break;
      default:
        print_usage_and_exit(argv[0], EXIT_FAILURE);
    }
  }

  if (argc-optind < 3) {
    print_usage_and_exit(argv[0], EXIT_FAILURE);
  }

  prefix = strdup(argv[optind+0]);
  script = strdup(argv[optind+1]);

  gethostname(hostname,sizeof(hostname));

  open_metalog(rank);

  print_announcement(argv[0],rank,size);

  echo_metalog = echo_metalog || (rank == 0);

  /* invoke main loops */
  if (rank == 0) {
    metalog("parsing task list...\n");
    list = parsetasks(size,argc-optind-2,argv+optind+2);
    master_main(rank,size,list);
  } else {
    runner_main(rank,size);
  }

  MPI_Finalize();  /* EXIT MPI */

  return EXIT_SUCCESS;
}



/* Repeatedly run a program until it exits cleanly.
 *
 * Use exponential backoff between runs if the time lived since restart is
 * short.
 *
 * Whenever command fails, a message it logged to logfile (or stderr).  When
 * command completes successfully, exits.
 *
 * Usage:
 *   simple-respawn [-l logfile] command args ...
 */

#include "common.h"
#include "plugin.h"
#include "utils_time.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static const char *progname;
static FILE *log_fp;

void plugin_log(int level, char const *format, ...)
{
  char buffer[1024];
  va_list ap;
  const char *level_str;
  char timestamp_str[64];

  cdtime_to_iso8601(timestamp_str, sizeof(timestamp_str), cdtime());

  va_start(ap, format);
  vsnprintf(buffer, sizeof(buffer), format, ap);
  va_end(ap);

  switch(level) {
    case LOG_ERR:
      level_str = "[error] ";
      break;
    case LOG_WARNING:
      level_str = "[warning] ";
      break;
    case LOG_NOTICE:
      level_str = "[notice] ";
      break;
    case LOG_INFO:
      level_str = "[info] ";
      break;
    default:
      level_str = "[] ";
  }

  fprintf(log_fp, "[%s] %s%s\n", timestamp_str, level_str, buffer);
}

gauge_t *uc_get_rate (const data_set_t *ds, const value_list_t *vl)
{
  return (NULL);
}

static void usage()
{
  fprintf(stderr, "Usage: %s [-l logfile] command args...\n",
          progname);
}

static int try_run(char *argv[])
{
  int status;
  pid_t pid;

  pid = fork();
  if (pid < 0) {
    ERROR("fork(): %s", strerror(errno));
    return -1;
  } else if (pid == 0) {
    /* Child: Start the new process */
    execvp(argv[0], argv);
    /* Still here? An error occurred! */
    ERROR("execvp %s: %s", argv[0], strerror(errno));
    exit(-1);
  }
  waitpid(pid, &status, 0);
  return status;
}

int main(int argc, char *argv[])
{
  int rc;
  cdtime_t current_sleep;
  double interval_secs;

  log_fp = stderr;
  progname = argv[0];

  while (argc > 1 && *argv[1] == '-') {
    if (strcmp(argv[1], "-l") == 0) {
      if (log_fp != stderr) {
        ERROR("%s: Trying to set more than one logfile; opening (only) %s",
              progname, argv[2]);
        fclose(log_fp);
      }
      if (!(log_fp = fopen(argv[2], "a"))) {
        ERROR("%s: Opening \"%s\" for append: %s",
              progname, argv[2], strerror(errno));
        return -1;
      }
      argv++, argc--;
    } else {
      fprintf(stderr, "%s: Unknown switch %s\n", progname, argv[1]);
      usage();
      return -2;
    }
    argv++, argc--;
  }

  argv++, argc--;
  if (argc <= 0) {
    fprintf(stderr, "Missing command\n");
    usage();
    return -3;
  }

  current_sleep = DOUBLE_TO_CDTIME_T(1.0);
  for (;;) {  /* Exit in loop */
    cdtime_t start, stop;
    start = cdtime();
    rc = try_run(argv);
    stop = cdtime();
    interval_secs = CDTIME_T_TO_DOUBLE(stop - start);
    if (WIFEXITED(rc) && WEXITSTATUS(rc) == 0) {
      INFO("Successful termination after %fs", interval_secs);
      exit(0);
    }

    if (WIFEXITED(rc))
      INFO("Exit after %fs with code %d", interval_secs, WEXITSTATUS(rc));
    else if (WIFSIGNALED(rc))
      INFO("Exit after %fs on signal %d (%s)",
           interval_secs, WTERMSIG(rc), strsignal(WTERMSIG(rc)));
    else
      ERROR("Exit after %fs with Unrecognized exit status type %d",
            interval_secs, rc);

    if (interval_secs < 3) {
      current_sleep *= 1.5;
      if (current_sleep > DOUBLE_TO_CDTIME_T(10.0))
        current_sleep = DOUBLE_TO_CDTIME_T(10.0);
      INFO("Sleeping %fs before next respawn",
           CDTIME_T_TO_DOUBLE(current_sleep));
      usleep(CDTIME_T_TO_US(current_sleep));
    } else {
      current_sleep = DOUBLE_TO_CDTIME_T(1.0);
      INFO("No sleeping before next respawn, and reset sleep");
    }
  }

  return 0;  /* UNREACHED */
}

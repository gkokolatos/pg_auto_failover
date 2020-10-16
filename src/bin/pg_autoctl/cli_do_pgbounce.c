/*
 * src/bin/pg_autoctl/XXXX.c
 */

#include "cli_root.h"
#include "cli_common.h"
#include "commandline.h"
#include "defaults.h"
#include "env_utils.h"
#include "ini_file.h"
#include "keeper_config.h"
#include "keeper.h"
#include "monitor.h"
#include "monitor_config.h"
#include "string_utils.h"

#include "pidfile.h"
#include "runprogram.h"
#include "supervisor.h"

static int cli_do_pgbouncer_getopts(int argc, char **argv);
static void cli_do_pgbouncer_start(int argc, char **argv);
static void cli_do_pgbouncer_stop(int argc, char **argv);
static void cli_do_pgbouncer_restart(int argc, char **argv);
static void cli_do_pgbouncer_status(int argc, char **argv);

/*
 * Add the subcommands to the array
 * NULL sentinel
 */
CommandLine *do_pgbouncer_subcommands[] = {
	&do_pgbouncer_start_command,
	&do_pgbouncer_stop_command,
	&do_pgbouncer_restart_command,
	&do_pgbouncer_status_command,
	NULL,
};

/*
 * The set of the commands under pg_autoctl
 * pg_autoctl pgbouncer ...
 */
CommandLine do_pgbouncer_commands =
	make_command_set("pgbouncer",							/* Command name */
					 "Set up pgbouncer for connections",		/* Help message */
					 NULL,									/* Subcommands help */
					 NULL,									/* Detailed help */
					 NULL,									/* Options set */
					 do_pgbouncer_subcommands);				/* Subcommmands */

/*
 * Each of the subcommands is described here
 */
CommandLine do_pgbouncer_start_command =
	make_command("start",
				 "set up pgbouncer for connections", 
				 "[ -- help ]",
				 "  --help       show this message \n",
				 cli_do_pgbouncer_getopts,
				 cli_do_pgbouncer_start);

CommandLine do_pgbouncer_stop_command =
	make_command("stop",
				 "set up pgbouncer for connections", 
				 "[ -- help ]",
				 "  --help       show this message \n",
				 cli_do_pgbouncer_getopts,
				 cli_do_pgbouncer_stop);

CommandLine do_pgbouncer_restart_command =
	make_command("restart",
				 "set up pgbouncer for connections", 
				 "[ -- help ]",
				 "  --help       show this message \n",
				 cli_do_pgbouncer_getopts,
				 cli_do_pgbouncer_restart);

CommandLine do_pgbouncer_status_command =
	make_command("status",
				 "set up pgbouncer for connections", 
				 "[ -- help ]",
				 "  --help       show this message \n",
				 cli_do_pgbouncer_getopts,
				 cli_do_pgbouncer_status);

static int
cli_do_pgbouncer_getopts(int argc, char **argv)
{
	KeeperConfig options = { 0 };
	int c, option_index = 0, errors = 0;

	static struct option long_options[] = {
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
	};

	/* set default values for our options, when we have some */
	optind = 0;

	while ((c = getopt_long(argc, argv, "h",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'h':
			{
				commandline_help(stderr);
				exit(EXIT_CODE_QUIT);
				break;
			}

			default:
			{
				/* getopt_long already wrote an error message */
				errors++;
			}
		}
	}

	if (errors > 0)
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	cli_common_get_set_pgdata_or_exit(&(options.pgSetup));

	keeperOptions = options;

	return optind;
}

/*
 * XXX: we need to make certain that we can start only when no other instance is
 * running
 */
static void
child_cli_do_pgbouncer_start(int argc, char **argv)
{
	Program program;

	char *args[4];
	int argsIndex = 0;

	char command[BUFSIZE];

	IntString semIdString = intToString(log_semaphore.semId);
	setenv(PG_AUTOCTL_DEBUG, "1", 1);
	setenv(PG_AUTOCTL_LOG_SEMAPHORE, semIdString.strValue, 1);

	args[argsIndex++] = "/postgres-build/bin/pgbouncer";
	args[argsIndex++] = "/tmp/config.ini";
	args[argsIndex] = NULL;

	/* we do not want to call setsid() when running this program. */
	program = initialize_program(args, false);

	program.capture = false;    /* redirect output, don't capture */
	program.stdOutFd = STDOUT_FILENO;
	program.stdErrFd = STDERR_FILENO;

	/* log the exact command line we're using */
	(void) snprintf_program_command_line(&program, command, BUFSIZE);

	log_info("%s", command);

	(void) execute_program(&program);
}

static void
cli_do_pgbouncer_start(int argc, char **argv)
{
	pid_t fpid;

	/* Flush stdio channels just before fork, to avoid double-output problems */
	fflush(stdout);
	fflush(stderr);

	/* time to create the node_active sub-process */
	fpid = fork();

	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork the postgres supervisor process");
			return;
		}

		case 0:
		{
			(void) set_ps_title("pgbouncer");

			log_trace("pgbouncer: EXEC postgres");

			child_cli_do_pgbouncer_start(0, NULL);

			/* unexpected */
			log_fatal("BUG: we should not have returned");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		default:
		{
			char service_pidfile[MAXPGPATH] = { 0 };
			log_info("pg_autoctl started pgbouncer in subprocess %d", fpid);

			/* XXX: how do we know it started? $ kill 0 fpid obviously */
			sleep(1);

			get_service_pidfile("/tmp/pg_auto_failover.pid",
								"PGBOUNCER",
								service_pidfile);

			create_pidfile(service_pidfile, fpid);
			log_info("XXX: created %s", service_pidfile);
		}
	}
}

static void
cli_do_pgbouncer_stop(int argc, char **argv)
{
	char service_pidfile[MAXPGPATH] = { 0 };
	long fileSize = 0L;
	char *fileContents = NULL;
	char *fileLines[1];

	pid_t pid;

	get_service_pidfile("/tmp/pg_auto_failover.pid",
						"PGBOUNCER",
						service_pidfile);


	if (!file_exists(service_pidfile))
		return;

	if (!read_file(service_pidfile, &fileContents, &fileSize))
	{
		log_debug("Failed to read the PID file \"%s\", removing it", service_pidfile);
		(void) remove_pidfile(service_pidfile);

		return;
	}

	splitLines(fileContents, fileLines, 1);
	stringToInt(fileLines[0], &pid);


	free(fileContents);

	kill(pid, SIGKILL);

	(void) remove_pidfile(service_pidfile);
}

static void
cli_do_pgbouncer_restart(int argc, char **argv)
{

}

static void
cli_do_pgbouncer_status(int argc, char **argv)
{

}

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
#include "supervisor.h"

#include "runprogram.h"

struct PgbouncerConfig {
	int		listen_port;
	char	admin_users[NAMEDATALEN];
	char	pidfile[MAXPGPATH];
};

/* Global variables for this file */
static char pgbouncerConfigFile[MAXPGPATH] = { 0 };

/* Forward declaration of commands */
static void cli_do_pgbouncer_start(int argc, char **argv);
static void cli_do_pgbouncer_stop(int argc, char **argv);
static void cli_do_pgbouncer_reboot(int argc, char **argv);
static void cli_do_pgbouncer_status(int argc, char **argv);

/* Forward declaration of command options */
static int cli_do_pgbouncer_common_getopts(int argc, char **argv);

/*
 * Add the subcommands to the array
 * NULL is sentinel
 */
CommandLine *do_pgbouncer_subcommands[] = {
	&do_pgbouncer_start_command,
	&do_pgbouncer_stop_command,
	&do_pgbouncer_reboot_command,
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
				 "Start a pg_auto_failover controlled pgbouncer instance",
				 "[ --config --help ]",
				 "  --config     pgbouncer config file (required)\n"
				 "  --help       show this message \n",
				 cli_do_pgbouncer_common_getopts,
				 cli_do_pgbouncer_start);

CommandLine do_pgbouncer_stop_command =
	make_command("stop",
				 "Stop a pg_auto_failover controlled pgbouncer instance",
				 "[ --config -- help ]",
				 "  --config     pgbouncer config file (required)\n"
				 "  --help       show this message \n",
				 cli_do_pgbouncer_common_getopts,
				 cli_do_pgbouncer_stop);

CommandLine do_pgbouncer_reboot_command =
	make_command("reboot",
				 "Reload a pg_auto_failover controlled pgbouncer instance",
				 "[ --config -- help ]",
				 "  --config     pgbouncer config file (required)\n"
				 "  --help       show this message \n",
				 cli_do_pgbouncer_common_getopts,
				 cli_do_pgbouncer_reboot);

CommandLine do_pgbouncer_status_command =
	make_command("status",
				 "Show stats of a pg_auto_failover controlled pgbouncer instance",
				 "[ --config -- help ]",
				 "  --config     pgbouncer config file (required)\n"
				 "  --help       show this message \n",
				 cli_do_pgbouncer_common_getopts,
				 cli_do_pgbouncer_status);


/*------------------------------
 * Section related with configuration parsing
 *------------------------------
 */


static char *
pgbouncer_parse_config_value(char *line)
{
	char   *val;

	if ((val = strchr(line, '=')) == NULL)
		return NULL;

	/*
	 * First ++ will move past the '='.
	 * Then proceed with trimming off any leading whitespace
	 */
	do
	{
		val++;
	}
	while (val && (*val == ' ' || *val == '\t'));

	return val;
}

/*
 * Absorb pgbouncer config contents into the struct
 */
static struct PgbouncerConfig *
pgbouncer_parse_config_or_die(void)
{
	struct	PgbouncerConfig *pgbouncerConfig = NULL;
	char   *pgbouncer_config_contents = NULL;
	char   *nl, *parsed;
	long	pgbouncer_config_contents_len = 0L;
	int		nline;

	if (pgbouncerConfigFile[0] == '\0')
	{
		log_fatal("pgbouncer config file is not set");
		exit(EXIT_CODE_BAD_CONFIG);
	}

	if (!read_file(pgbouncerConfigFile,
				   &pgbouncer_config_contents,
				   &pgbouncer_config_contents_len))
	{
		/* read_file has logged why */
		exit(EXIT_CODE_BAD_CONFIG);
	}

	if (pgbouncer_config_contents_len <= 0)
	{
		log_error("Empty pgbouncer config file %s", pgbouncerConfigFile);
		exit(EXIT_CODE_BAD_CONFIG);
	}

	pgbouncerConfig = calloc(1, sizeof(*pgbouncerConfig));
	nline = 0;
	parsed = pgbouncer_config_contents;

	/*
	 * And that we can read the important for us configurations
	 * -- listen_port
	 * -- pid file location
	 * -- auth_user, Only one will do
	 */
	do
	{
		if ((nl = strchr(parsed, '\n')) == NULL)
			break;

		*nl = '\0';
		if (!strncmp(parsed, "listen_port", strlen("listen_port")))
		{
			char *val = pgbouncer_parse_config_value(parsed);

			if (val == NULL ||
				(!stringToInt(val, &pgbouncerConfig->listen_port)) ||
				pgbouncerConfig->listen_port > 65353 ||
				pgbouncerConfig->listen_port < 1023)
			{
				log_error("Invalid value for listen_port %d:%s",
							nline,
							parsed);
				exit(EXIT_CODE_BAD_CONFIG);
			}
			log_debug("pgbouncer will be listening to %d", pgbouncerConfig->listen_port);
		}
		else if (!strncmp(parsed, "pidfile", strlen("pidfile")))
		{
			char *val = pgbouncer_parse_config_value(parsed);

			if (!val || strlen(val) >= MAXPGPATH)
			{
				log_error("Invalid value for pidfile %d:%s",
							nline,
							parsed);
				exit(EXIT_CODE_BAD_CONFIG);
			}

			memcpy(pgbouncerConfig->pidfile, val, strlen(val));
			log_debug("pgbouncer will write pid to %s", pgbouncerConfig->pidfile);
		}
		else if (!strncmp(parsed, "admin_users", strlen("admin_users")))
		{
			char *val = pgbouncer_parse_config_value(parsed);

			if (!val || strlen(val) > NAMEDATALEN)
			{
				log_error("Invalid value for admin_users %d:%s",
							nline,
							parsed);
				exit(EXIT_CODE_BAD_CONFIG);
			}

			memcpy(pgbouncerConfig->admin_users, val, strlen(val));

			/* Just pick the first if many */
			if ((val = strchr(pgbouncerConfig->admin_users, ',')) != NULL)
			{
				*val = '\0';
			}

			log_debug("pgbouncer will use admin_users %s", pgbouncerConfig->admin_users);
		}

		parsed = ++nl;
		nline++;
	} while (parsed < pgbouncer_config_contents + pgbouncer_config_contents_len);

	free(pgbouncer_config_contents);
	return pgbouncerConfig;
}

/*------------------------------
 * Section related with command options
 *------------------------------
 */

static int
cli_do_pgbouncer_common_getopts(int argc, char **argv)
{
	int	c;
	int	option_index = 0;
	int	errors = 0;

	static struct option long_options[] = {
		{ "config",	required_argument,	NULL,	'c' },
		{ "help",	no_argument,		NULL,	'h' },
		{ NULL,		0,					NULL,	0 }
	};

	/* set default values for our options, when we have some */
	optind = 0;

	while ((c = getopt_long(argc, argv, "c:h",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'c':
			{
				if (strlcpy(pgbouncerConfigFile,
							optarg,
							sizeof(pgbouncerConfigFile)) >=
					sizeof(pgbouncerConfigFile))
				{
					log_error("config file too long, greater than  %ld",
								sizeof(pgbouncerConfigFile) -1);
					exit(EXIT_CODE_BAD_ARGS);
				}

				log_trace("--config %s", pgbouncerConfigFile);
				break;
			}

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

	if (pgbouncerConfigFile[0] == '\0')
	{
		commandline_help(stderr);
		exit(EXIT_CODE_BAD_ARGS);
	}

	return optind;
}

/*
 * Attempt to read a valid pid from a pidfile
 *
 * The code assumes that the relevant pid is stored in the first line.
 * If ok_to_fail is set, then 0 is returned and is up to the caller to verify the
 * returned value before using it.
 */
static pid_t
pgbouncer_get_pid_from_pidfile(const char *pidfile, bool ok_to_fail)
{
	char					   *fileContents = NULL;
	char                       *fileLines[1];
	long						fileSize = 0L;
	pid_t						pid;

	if (!file_exists(pidfile))
	{
		log_error("Failed to find the PID file \"%s\"", pidfile);
		if (ok_to_fail)
			return 0;
		else
			exit(EXIT_CODE_BAD_CONFIG);
	}

	if (!read_file(pidfile, &fileContents, &fileSize))
	{
		/* read_file has logged why */
		(void) remove_pidfile(pidfile);

		if (ok_to_fail)
			return 0;
		else
			exit(EXIT_CODE_INTERNAL_ERROR);
	}

	splitLines(fileContents, fileLines, 1);
	if (!stringToInt(fileLines[0], &pid))
	{
		log_error("Invalid pid \"%s\"", fileLines[0]);
		if (ok_to_fail)
			return 0;
		else
			exit(EXIT_CODE_BAD_ARGS);
	}
	free(fileContents);

	return pid;
}

/*------------------------------
 * Section related with executing commands
 *------------------------------
 */

/*
 * Helper function used when in need to signal pbouncer
 */
static int
signal_pgbouncer(pid_t pid, int signal)
{
	char	errbuf[BUFSIZE];

	if (kill(pid, signal) != 0)
	{
		log_fatal("Failed to signal pgbouncer process %d, %s",
					pid,
					strerror_r(errno, errbuf, BUFSIZE));
		return 1;
	}

	return 0;
}

/*
 * TODO: we need to make certain that we can start only when no other instance is
 * running
 */
static void
child_cli_do_pgbouncer_start(struct PgbouncerConfig *config)
{
	Program	program;

	char   *args[5];
	int		argsIndex = 0;

	char command[BUFSIZE];

	IntString semIdString = intToString(log_semaphore.semId);
	setenv(PG_AUTOCTL_DEBUG, "1", 1);
	setenv(PG_AUTOCTL_LOG_SEMAPHORE, semIdString.strValue, 1);

	args[argsIndex++] = "/postgres-build/bin/pgbouncer";
	args[argsIndex++] = "-d";
	args[argsIndex++] = "-q";
	args[argsIndex++] = pgbouncerConfigFile;
	args[argsIndex] = NULL;

	/* XXX: why???  we do not want to call setsid() when running this program. */
	program = initialize_program(args, false);

	/* XXX: in the TODO, capture output and send to log file? */
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
	struct PgbouncerConfig *pgbouncerConfig;
	pid_t					fpid;
	pid_t					running_pid;

	pgbouncerConfig = pgbouncer_parse_config_or_die();
	running_pid = pgbouncer_get_pid_from_pidfile(pgbouncerConfig->pidfile, true);

	if (running_pid != 0 && signal_pgbouncer(running_pid, 0) != 1)
	{
		log_fatal("Process %d already running", running_pid);
		exit(EXIT_CODE_BAD_STATE);
	}

	/* Flush stdio channels just before fork, to avoid double-output problems */
	fflush(stdout);
	fflush(stderr);

	/* time to create the node_active sub-process */
	fpid = fork();
	switch (fpid)
	{
		case -1:
		{
			log_error("Failed to fork the pgbouncer process");
			return;
		}

		case 0:
		{
			(void) set_ps_title("pgbouncer");
			log_trace("pgbouncer: EXEC pgbouncer");
			child_cli_do_pgbouncer_start(pgbouncerConfig);

			/* unexpected */
			log_fatal("BUG: we should not have returned");
			exit(EXIT_CODE_INTERNAL_ERROR);
		}

		default:
		{
			int status;
			log_info("pg_autoctl started pgbouncer in subprocess %d", fpid);

			do {
				if (waitpid(fpid, &status, WUNTRACED) == -1)
				{
					log_fatal("XXX: Some kind of error, inspect errno");
					exit(EXIT_CODE_INTERNAL_ERROR);
				}
			} while (!WIFEXITED(status) && !WIFSIGNALED(status));

			if (status)
			{
				log_fatal("XXX: status %d", status);
				exit(EXIT_CODE_INTERNAL_ERROR);
			}
		}
	}
}

static void
cli_do_pgbouncer_stop(int argc, char **argv)
{
	struct PgbouncerConfig	   *config;
	pid_t						pid;

	config = pgbouncer_parse_config_or_die();
	pid = pgbouncer_get_pid_from_pidfile(config->pidfile, false);

	if (signal_pgbouncer(pid, SIGINT))
	{
		/* It has allready logged why */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}

	(void) remove_pidfile(config->pidfile);
}

static void
cli_do_pgbouncer_reboot(int argc, char **argv)
{
	struct PgbouncerConfig	   *config;
	pid_t						pid;

	config = pgbouncer_parse_config_or_die();
	pid = pgbouncer_get_pid_from_pidfile(config->pidfile, false);

	if (signal_pgbouncer(pid, SIGHUP))
	{
		/* It has allready logged why */
		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}

static void
cli_do_pgbouncer_status(int argc, char **argv)
{
	char * const args[] = {
		"/postgres-build/bin/psql",
		"-X",
		"-p 15001",
		"-Upgbouncer",
		"-dpgbouncer",
		"-c SHOW STATS",
		NULL,
	};
	char errbuf[BUFSIZE];

	if (execv("/postgres-build/bin/psql", args) == -1)
	{
		fprintf(stdout, "out: %s\n", strerror_r(errno, errbuf, BUFSIZE));
		fprintf(stderr, "error: %s\n", strerror_r(errno, errbuf, BUFSIZE));

		exit(EXIT_CODE_INTERNAL_ERROR);
	}
}

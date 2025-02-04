#!/bin/bash

# alpenhorn start-up script for non-interactive nodes

# This is an example of a script to run alpenhorn on nodes
# which do not allow interactive login.  This script should be
# installed as executable on the non-interactive node and it's
# path passed as the value of the command= directive in the
# alpenhorn user's authorized key list.

# Then, to interact with this script, specify a command on the inbound 
# ssh command-line when using the appropriate key:
#
# ssh <non-interactive-node> -i <robot-keyfile> { start | stop | restart }
#
# Available commands:
#
#  start    start alpenhornd in a screen if not already running
#  stop     stop alpenhornd (and the screen) if running
#  restart  forced-restart: equivalent to "stop" and then "start"

# The alpenhornd command is run in a detached screen on the non-interactive
# node in a session called "alpenhornd".  The script will log execute to
# syslog as configured below.  

# Note: it is also possible to use this script on an interactive node
# (say, for testing purposes) by manually setting SSH_ORIGINAL_COMMAND
# to the desired value during invocation:
#
# SSH_ORIGINAL_COMMAND=<COMMAND> ./alpenhorn_robot.sh
#
# (where <COMMAND> is one of: start, stop, restart.)


# CONFIGURATION
# -------------
#
# These variables need to be customised for the node which this script
# is running on

# The path to the awk(1) program
AWK=/usr/bin/awk

# The path to the GNU screen(1) program
SCREEN=/usr/bin/screen

# The command to run to start a new alpenhornd instance.  This may
# be the alpenhornd daemon itself, but is typically a command or script
# that sets up the alpenhornd environment before starting the daemon.
ALPENHORND="/usr/bin/sg alpenhorn ${HOME}/bin/start_alpenhornd"

# The log priority for message sent to the syslog.  May be numeric or
# a facility.level pair.  See the -p option in logger(1) for details.
LOGPRIO=local0.info

# The log tag for messages sent to the syslog.  See the -t option in
# logger(1) for details.
LOGTAG=alpenhorn

# END CONFIRGUATION
# -----------------
#
# The rest of this script probably doesn't need changing

THIS_SCRIPT=$(basename $0)

# NB: The inbound command ends up in $SSH_ORIGINAL_COMMAND

# Run screen(1) with logging.  Parameters are passed to screen(1)
function run_screen() {
  echo "running: screen $@"
  logger -t $LOGTAG -p $LOGPRIO "Command called by $THIS_SCRIPT for user $USER: $SCREEN $*"
  $SCREEN "$@"
}

# Part one: vet the inbound command
if [ "x$SSH_ORIGINAL_COMMAND" != "xstart" \
  -a "x$SSH_ORIGINAL_COMMAND" != "xstop" \
  -a "x$SSH_ORIGINAL_COMMAND" != "xrestart" \
  ]
then
  # Reject all unsupported input
  logger -t $LOGTAG -p $LOGPRIO "Command rejected by $THIS_SCRIPT for user $USER: $SSH_ORIGINAL_COMMAND"
  exit 1
fi



# Part two: if asked to stop or restart, stop an existing daemon
if [ "$SSH_ORIGINAL_COMMAND" = "stop" \
  -o "$SSH_ORIGINAL_COMMAND" = "restart" \
  ]
then
  echo "$0: Killing alpenhornd (if running)"

  # Kill all screens with sessions named "alpenhornd"
  run_screen -ls | $AWK '/[0-9]*.alpenhornd/ { print $1 }' | while read session; do
    run_screen -S $session -X quit
  done
  sleep 1

  # Kill all processes named "alpenhornd"
  killall -v -9 alpenhornd

  # If force-restarting, wait for termination
  if [ "$SSH_ORIGINAL_COMMAND" = "restart" ]
  then
    sleep 5
  fi
fi


# Part three: if asked to start or restart, start daemon if necessary
if [ "$SSH_ORIGINAL_COMMAND" = "start" \
  -o "$SSH_ORIGINAL_COMMAND" = "restart" \
  ]
then
  # this tests whether a screen called "alpenhornd" is running
  if ! run_screen -S alpenhornd -Q select . &>/dev/null
  then
    # spawn a new detached screen using the alpenstart start script
    echo "$0: starting alpenhornd in a screen"
    run_screen -d -m -S alpenhornd ${ALPENHORN}
  else
    echo "$0: alpenhornd already running"
  fi
fi

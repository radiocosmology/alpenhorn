INITDIR=/etc/init
SYSTEMDDIR=/lib/systemd/system
LOGDIR=/var/log/alpenhorn
CRON=/etc/cron.hourly

install: alpenhornd.conf

	# Use pip to install the package and scripts
	pip install --no-deps -U -e ../

	# Create the logdir if it doesn't exist
	mkdir -p $(LOGDIR)

	if ps -p1 | grep -q systemd; then \
		install -m 644 alpenhornd.service $(SYSTEMDDIR)/alpenhornd.service; \
	else install -m 644 alpenhornd.conf $(INITDIR)/alpenhornd.conf; fi

	if [ "x`hostname`" = "xtubular" ]; then \
		install -m 755 cron.hourly.tubular ${CRON}/alpenhorn; fi


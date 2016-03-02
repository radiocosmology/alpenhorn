INITDIR=/etc/init
LOGDIR=/var/log/alpenhorn
CRON=/etc/cron.hourly

install: alpenhornd.conf

	# Use pip to install the package and scripts
	pip install ../

	# Create the logdir if it doesn't exist
	mkdir -p $(LOGDIR)

	install -m 644 alpenhornd.conf $(INITDIR)/alpenhornd.conf
	
	if [ "x`hostname`" = "xtubular" ]; then \
		install -m 755 cron.hourly.tubular ${CRON}/alpenhorn; fi


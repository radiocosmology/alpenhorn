BINDIR=/usr/local/bin
SBINDIR=/usr/sbin
INITDIR=/etc/init
ROTATEDIR=/etc/logrotate.d
LOGDIR=/var/log/alpenhorn
CRON=/etc/cron.hourly

install: alpenhorn alpenhornd alpenhornd.conf
	if [ ! -d $(LOGDIR) ]; then mkdir $(LOGDIR); fi
	install -m 644 alpenhornd.conf $(INITDIR)/alpenhornd.conf
	install -m 644 logrotate.script $(ROTATEDIR)/alpenhornd
	if [ "x`hostname`" = "xtubular" ]; then \
      install -m 755 cron.hourly.tubular ${CRON}/alpenhorn; fi
	install -m 755 alpenhornd $(SBINDIR)/alpenhornd
	install -m 755 alpenhorn $(BINDIR)/alpenhorn

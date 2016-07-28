FROM reverbrain:trusty-dev

RUN	echo "deb http://repo.reverbrain.com/trusty/ current/amd64/" > /etc/apt/sources.list.d/reverbrain.list && \
	echo "deb http://repo.reverbrain.com/trusty/ current/all/" >> /etc/apt/sources.list.d/reverbrain.list && \
	apt-get install -y curl tzdata && \
	cp -f /usr/share/zoneinfo/posix/W-SU /etc/localtime && \
	curl http://repo.reverbrain.com/REVERBRAIN.GPG | apt-key add - && \
	apt-get update && \
	apt-get upgrade -y && \
	apt-get install -y git g++ liblz4-dev libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev libjemalloc-dev && \
	apt-get install -y cmake debhelper cdbs devscripts && \
	git config --global user.email "zbr@ioremap.net" && \
	git config --global user.name "Evgeniy Polyakov"

RUN	cd /tmp && \
	git clone https://github.com/facebook/rocksdb && \
	cd rocksdb && \
	make shared_lib && \
	make INSTALL_PATH=/usr install-shared && \
	echo "Rocksdb package has been updated and installed"

RUN	apt-get install -y libboost-system-dev libboost-filesystem-dev libboost-program-options-dev libmsgpack-dev libswarm3-dev libthevoid3-dev && \
	cd /tmp && \
	git clone https://github.com/reverbrain/greylock && \
	cd greylock && \
	git branch -v && \
	dpkg-buildpackage -b && \
	dpkg -i ../greylock_*.deb ../greylock-dev_*.deb && \
	echo "Greylock package has been updated and installed" && \
    	rm -rf /var/lib/apt/lists/*

EXPOSE 8080 8181 8111

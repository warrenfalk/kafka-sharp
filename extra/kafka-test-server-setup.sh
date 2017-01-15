#!/bin/bash

VERSION=0.10.1.1
APPDIR=kafka_2.11-${VERSION}
ARCHIVE=$APPDIR.tgz

cd $(dirname $0)
mkdir -p kafka-test-server
cd kafka-test-server
if [ ! -d $APPDIR ]; then
	if [ ! -f $ARCHIVE ]; then
		echo "Downloading kafka"
		wget "http://www-us.apache.org/dist/kafka/0.10.1.1/$ARCHIVE"
	fi
	echo "Extracting archive"
	tar -xzf $ARCHIVE
fi

if [ ! -d kafka ]; then
	echo "Symlinking kafka directory"
	ln -s $APPDIR kafka
fi

tmux attach -t kafka 2>/dev/null || {
	tmux\
		new-session -d -s kafka -n kafka 'bash --init-file <(echo ". \"$HOME/.bashrc\"; ./start-zk")' \; \
		split-window 'bash --init-file <(echo ". \"$HOME/.bashrc\"; ./start-kafka")' \; \
		attach-session -t kafka;
}
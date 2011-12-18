HADOOP_VERSION=0.18.0
HADOOP_INSTALL=/home/radu/Desktop/ATDS/wiki_stats

NAME=EditViewBalance
JAR=$(NAME).jar
JAVA=$(NAME).java

all:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/user src/$(JAVA)
	jar -cvf $(JAR) -C bin/user .

realclean: clean
	rm -fr output*

clean:
	rm -fr bin/user/*
	rm -fr bin/page/*
	rm -fr $(JAR)


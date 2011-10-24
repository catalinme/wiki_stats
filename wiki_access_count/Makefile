
HADOOP_VERSION=0.20.2
HADOOP_INSTALL=/home/hadoop/hadoop-$(HADOOP_VERSION)/

JAR=WikiAccesscount.jar

all:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WikiAccessCount.java
	jar -cvf WikiAccesscount.jar -C bin/ .

run:
	hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input/ output/
	
realclean: clean
	rm -fr output*

clean:
	rm -fr bin/*
	rm -fr WikiAccesscount.jar




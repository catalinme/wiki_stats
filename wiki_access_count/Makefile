
HADOOP_VERSION=0.20.2
HADOOP_INSTALL=/home/hadoop/hadoop-$(HADOOP_VERSION)/

JAR=WikiAccesscount.jar

all: wiki_trends

wiki_acc_count:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WikiAccessCount.java
	jar -cvf WikiAccesscount.jar -C bin/ .

wiki_trends:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WikiTrend.java src/Entry.java
	jar -cvf WikiTrend.jar -C bin/ .

run-count:
	hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input/ output/

run-trend:
	#hadoop jar WikiTrend.jar org.myorg.WikiTrend input/ output/
	hadoop jar WikiTrend.jar org.myorg.WikiTrend input_gz/ output/
	
run-big:
	#hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input_big/ output_big/
	hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input_gz/ output_gz/

realclean: clean

clean:
	rm -fr output/
	rm -fr bin/*
	rm -fr WikiAccesscount.jar




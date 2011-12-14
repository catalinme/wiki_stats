
HADOOP_VERSION=0.20.2
HADOOP_INSTALL=/home/hadoop/hadoop-$(HADOOP_VERSION)/

JAR=WikiAccesscount.jar

all: wiki_trends

wiki_acc_count:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WikiAccessCount.java
	jar -cvf WikiAccesscount.jar -C bin/ .

wiki_trends:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WikiTrend.java
	jar -cvf WikiTrend.jar -C bin/ .

run-count:
	hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input/ output/

run-trend:
	hadoop jar WikiTrend.jar org.myorg.WikiTrend input/ output/
	#java -jar WikiTrend.jar org.myorg.WikiTrend input/ output/
	
run-big:
	#hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input_big/ output_big/
	hadoop jar WikiAccesscount.jar org.myorg.WikiAccessCount input_gz/ output_gz/

realclean: clean
	rm -fr output*

clean:
	rm -fr bin/*
	rm -fr WikiAccesscount.jar




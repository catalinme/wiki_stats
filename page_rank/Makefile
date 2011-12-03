
HADOOP_VERSION=0.18.0
HADOOP_INSTALL=/home/radu/Desktop/ATDS/wiki_stats

JAR=WikiPageRank.jar

all:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WikiPageRank.java
	jar -cvf $(JAR) -C bin/ .

run:
	hadoop jar $(JAR) org.myorg.WikiPageRank input/ output/
	
realclean: clean
	rm -fr output*

clean:
	rm -fr bin/*
	rm -fr $(JAR)



HADOOP_VERSION=0.20.2
HADOOP_INSTALL=/home/hadoop/hadoop-$(HADOOP_VERSION)/

JAR=wordcount.jar

all:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/ src/WordCount.java
	jar -cvf wordcount.jar -C bin/ .

run:
	hadoop jar wordcount.jar org.myorg.WordCount input/file1 output_1/
	hadoop jar wordcount.jar org.myorg.WordCount input/file2 output_2/
	
realclean: clean
	rm -fr output*

clean:
	rm -fr bin/*
	rm -fr wordcount.jar





HADOOP_VERSION=0.18.0
HADOOP_INSTALL=/home/radu/Desktop/ATDS/wiki_stats

JAR1=WikiPageEditUser.jar
JAR2=WikiPageEditPage.jar

all:
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/user src/WikiPageEditUser.java
	jar -cvf $(JAR1) -C bin/user .
	javac -classpath $(HADOOP_INSTALL)/hadoop-$(HADOOP_VERSION)-core.jar -d bin/page src/WikiPageEditPage.java
	jar -cvf $(JAR2) -C bin/page .


run1:
	hadoop jar $(JAR1) org.myorg.WikiPageEditUser input/ output/

run2:
	hadoop jar $(JAR2) org.myorg.WikiPageEditPage input/ output/
	
realclean: clean
	rm -fr output*

clean:
	rm -fr bin/user/*
	rm -fr bin/page/*
	rm -fr $(JAR1)
	rm -fr $(JAR2)


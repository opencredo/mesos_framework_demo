Hello,

This is a simple example framework accompanying a blog post that can be found here: http://www.opencredo.com/2015/02/16/write-mesos-framework/

Build
-----

```
mvn clean package
```

This will produce the artefact: target/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar


Run
---


Run the jar:

```
java -Djava.library.path=/usr/local/lib -jar target/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar zk://localhost:2181/mesos
```

*You'll need to point java to the mesos shared library, which is usually found in /usr/local/lib* 

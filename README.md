Hello,

This is a simple example framework accompanying a blog post that can be found here: http://www.example.com/

Build
-----

```
mvn clean package
```

This will produce the artefact: target/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar


Run
---

*You'll need the mesos shared library in your library path* 

Run the jar:

```
java -jar target/example-framework-1.0-SNAPSHOT-jar-with-dependencies.jar
```

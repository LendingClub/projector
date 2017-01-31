![Projector](https://raw.githubusercontent.com/LendingClub/projector/master/.assets/noun_7987_sm.png) 

# projector

Projector is a set of tools project a graph-model of physical, virtual and cloud infrastructure.

It is intended to be used as a library inside of other services and tools.



# Usage

## Core Configuration

The [Projector](https://github.com/LendingClub/projector/blob/master/projector-core/src/main/java/org/lendingclub/projector/core/Projector.java) class is the cornerstone of the
Projector project.  It exposes configuration and a REST client for interacting with Neo4j.

Projector doesn't use Spring, Dagger, Guice or any other DI framework.  That is your choice.  Projector is intended to be simple and straightorward to use.

To create a Projector instance that connects to Neo4j at http://localhost:7474 with no username or password:

```java
Projector projector = new BasicProjector();
```

The following configuration options can be passed through the ```Projector(Map config)``` constructor:

| Property Name | Description | Default Value |
|---------------|-------------|---------------|
| neo4j.url     |  URL for connecting to Neo4j REST API | http://localhost:7474 |
| neo4j.username|  Username for authentication | N/A |
| neo4j.password|  Password for authentication | N/A |

Usage is fairly self-explanatory:

```java
Map<String,String> config = new HashMap<>();
config.put("neo4j.url","https://localhost:7473");
config.put("neo4j.username","myusername");
config.put("neo4j.username","mypassword");

Projector projector = new BasicProjector(config);
```


## AWS


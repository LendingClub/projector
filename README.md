![Projector](https://raw.githubusercontent.com/LendingClub/projector/master/.assets/noun_7987_sm.png) 

# projector

Projector is a set of tools project a graph-model of physical, virtual and cloud infrastructure.

It is intended to be used as a library inside of other services and tools.



# Usage

## Core Configuration

[Projector](https://github.com/LendingClub/projector/blob/master/projector-core/src/main/java/org/lendingclub/projector/core/Projector.java) is the cornerstone of the
Projector project.  It exposes configuration and a REST client for interacting with Neo4j.

Instantiating a Projector instance is straigtforward:

```java
Projector projector = new BasicProjector();
```

By default, this will communicate with Neo4j at http://localhost:7474 with no username and password.

The following configuration options are supported:

| Property Name | Description | Default Value |
|---------------|-------------|---------------|
| neo4j.url     |  URL for connecting to Neo4j REST API | http://localhost:7474 |
| neo4j.username|  Username for authentication | N/A |
| neo4j.password|  Password for authentication | N/A |

```java
Map<String,String> config = new HashMap<>();
config.put("neo4j.url","https://localhost:7473");
config.put("neo4j.username","myusername");
config.put("neo4j.username","mypassword");

Projector projector = new BasicProjector(config);
```


## AWS


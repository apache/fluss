# Fluss IDEA Run Configs (Coordinator + Tablet)

## 1) CoordinatorServer (Application)

- Main class:
`org.apache.fluss.server.coordinator.CoordinatorServer`

- Use classpath of module:
`fluss-dist`

- Program arguments:
```text
--configDir /Users/litiliu/IdeaProjects/fluss/fluss-dist/src/main/resources-coordinator
```

- VM options:
```text
-Dlog.file=/tmp/fluss-coordinator-idea.log -Dconsole.log.level=INFO -Dlog4j.configurationFile=file:/Users/litiliu/IdeaProjects/fluss/fluss-dist/src/main/resources/conf/log4j-console.properties
```

- Working directory:
`/Users/litiliu/IdeaProjects/fluss`

- Environment variables:
```text
ROOT_LOG_LEVEL=INFO
```

---

## 2) TabletServer (Application)

- Main class:
`org.apache.fluss.server.tablet.TabletServer`

- Use classpath of module:
`fluss-dist`

- Program arguments:
```text
--configDir /Users/litiliu/IdeaProjects/fluss/fluss-dist/src/main/resources-tablet
```

- VM options:
```text
-Dlog.file=/tmp/fluss-tablet-idea.log -Dconsole.log.level=INFO -Dlog4j.configurationFile=file:/Users/litiliu/IdeaProjects/fluss/fluss-dist/src/main/resources/conf/log4j-console.properties
```

- Working directory:
`/Users/litiliu/IdeaProjects/fluss`

- Environment variables:
```text
ROOT_LOG_LEVEL=INFO
```

---

## Related config dirs

- Coordinator config:
`/Users/litiliu/IdeaProjects/fluss/fluss-dist/src/main/resources-coordinator/server.yaml`

- Tablet config:
`/Users/litiliu/IdeaProjects/fluss/fluss-dist/src/main/resources-tablet/server.yaml`

---

## 3) Flink SQL test (in Docker)

### Start SQL Client container
```bash
docker run -it apache/fluss-quickstart-flink:1.20-0.9.1-incubating bash
```

### Enter Flink SQL Client
```bash
bash sql-client
```

### Create Fluss catalog
```sql
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'host.docker.internal:9123'
);
```

### Optional quick connectivity check inside container
```bash
curl -v telnet://host.docker.internal:9123
```

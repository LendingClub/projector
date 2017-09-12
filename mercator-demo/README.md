
```
docker run \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -p 7474:7474 \
  -p 7687:7687 \
  mercator-demo
````
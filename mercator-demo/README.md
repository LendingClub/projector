
```
docker run \
  -v ~/.aws:/root/.aws
  -v /var/run/docker.sock:/var/run/docker.sock \
  -p 7474:7474 \
  -p 7687:7687 \
  lendingclub/mercator-demo
````
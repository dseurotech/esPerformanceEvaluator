This simple project's goal is to evaluate the performance of the elasticsearch client, both low-level and high-level versions (testing against version 7.8.1)

The goal is to understand whether it's better to have few or a lot of threads per client, and if having a client pool is beneficial or not (and how much).

An elasticseach instance is needed as a target, one can easily be obtained using docker:

```
docker run -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:7.8.1
```

Once that's up, just run the main method of the EsPErformanceEvalutator
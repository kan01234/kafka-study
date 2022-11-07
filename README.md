# kafka study
study log for [course](https://www.udemy.com/course/apache-kafka/) from Conduktor

### setup
execute zookeeper
```
zookeeper-server-start /usr/local/etc/zookeeper/zoo.cfg
```

execute kafka server
```
kafka-server-start /usr/local/etc/kafka/server.properties
```

### code
#### basic
basic contains basic java kafka producer and consumer

#### wiki-producer
read event from [wiki recent changes](https://stream.wikimedia.org/v2/stream/recentchange) and produce to topic

#### opensearch-consumer
consumer event from wiki recent change topic and send to open search


### Arhitecture
![basic arhitecture](./drawio/basic.svg)

#### Video system
![video system arhitecture](./drawio/video-system.svg)

#### taxi system
![taxi system arhitecture](./drawio/taxi.svg)

#### social media system
![social media system arhitecture](./drawio/social-media.svg)

#### big data system
![big data system arhitecture](./drawio/big-data.svg)

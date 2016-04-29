#Jodis - Java client for codis

[![Build Status](https://travis-ci.org/CodisLabs/jodis.svg)](https://travis-ci.org/CodisLabs/jodis)

Jodis is a java client for codis based on [Jedis](https://github.com/xetorthio/jedis) and [Curator](http://curator.apache.org/).

##Features
- Use a round robin policy to balance load to multiple codis proxies.
- Detect proxy online and offline automatically.

##How to use
Add this to your pom.xml. We deploy jodis to https://oss.sonatype.org.
```xml
<dependency>
  <groupId>io.codis.jodis</groupId>
  <artifactId>jodis</artifactId>
  <version>0.3.1</version>
</dependency>
```
To use it
```java
JedisResourcePool jedisPool = RoundRobinJedisPool.create()
        .curatorClient("zkserver:2181", 30000).zkProxyDir("/zk/codis/db_xxx/proxy").build();
try (Jedis jedis = jedisPool.getResource()) {
    jedis.set("foo", "bar");
    String value = jedis.get("foo");
    System.out.println(value);
}
```
Note: JDK7 is required to build and use jodis. If you want to use jodis with JDK6, you can copy the source files to your project, replace ThreadLocalRandom in BoundedExponentialBackoffRetryUntilElapsed and JDK7 specified grammar(maybe, not sure) , and then compile with JDK6.

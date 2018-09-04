# Jodis - Java client for codis

[![Build Status](https://travis-ci.org/CodisLabs/jodis.svg)](https://travis-ci.org/CodisLabs/jodis)

Jodis is a java client for codis based on [Jedis](https://github.com/xetorthio/jedis) and [Curator](http://curator.apache.org/).

# Features
- Use a round robin policy to balance load to multiple codis proxies.
- Detect proxy online and offline automatically.

# How to use
Add this to your pom.xml. We deploy jodis to https://oss.sonatype.org.
```xml
<dependency>
  <groupId>io.codis.jodis</groupId>
  <artifactId>jodis</artifactId>
  <version>0.5.1</version>
</dependency>
```
To use it for Codis2.x:
```java
JedisResourcePool jedisPool = RoundRobinJedisPool.create()
        .curatorClient("zkserver:2181", 30000).zkProxyDir("/zk/codis/db_xxx/proxy").build();
try (Jedis jedis = jedisPool.getResource()) {
    jedis.set("foo", "bar");
    String value = jedis.get("foo");
    System.out.println(value);
}
```
Or for Codis3.x with `jodis_compatible=false`:
```java
JedisResourcePool jedisPool = RoundRobinJedisPool.create()
        .curatorClient("zkserver:2181", 30000).zkProxyDir("/jodis/xxx").build();
try (Jedis jedis = jedisPool.getResource()) {
    jedis.set("foo", "bar");
    String value = jedis.get("foo");
    System.out.println(value);
}
```
Note: JDK8 is required to use and build jodis, as JDK7 has been EOL since May 2015.

/**
 * @(#)RoundRobinJedisPool.java, 2014-11-30.
 * 
 * Copyright (c) 2014 Wandoujia Inc.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.wandoulabs.jodis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

/**
 * A round robin connection pool for connecting multiple codis proxies based on Jedis and Curator.
 * 
 * @author Apache9
 * @see https://github.com/xetorthio/jedis
 * @see http://curator.apache.org/
 */
public class RoundRobinJedisPool implements JedisResourcePool {

    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinJedisPool.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CODIS_PROXY_STATE_ONLINE = "online";

    private static final int CURATOR_RETRY_BASE_SLEEP_MS = 100;

    private static final int CURATOR_RETRY_MAX_SLEEP_MS = 30 * 1000;

    private static final int JEDIS_POOL_TIMEOUT_UNSET = -1;

    private static final ImmutableSet<PathChildrenCacheEvent.Type> RESET_TYPES = Sets
            .immutableEnumSet(PathChildrenCacheEvent.Type.CHILD_ADDED,
                    PathChildrenCacheEvent.Type.CHILD_UPDATED,
                    PathChildrenCacheEvent.Type.CHILD_REMOVED);

    private final CuratorFramework curatorClient;

    private final boolean closeCurator;

    private final PathChildrenCache watcher;

    private static final class PooledObject {
        public final String addr;

        public final JedisPool pool;

        public PooledObject(String addr, JedisPool pool) {
            this.addr = addr;
            this.pool = pool;
        }

    }

    private volatile ImmutableList<PooledObject> pools = ImmutableList.of();

    private final AtomicInteger nextIdx = new AtomicInteger(-1);

    private final JedisPoolConfig poolConfig;

    private final int connectionTimeoutMs;

    private final int soTimeoutMs;

    private final String password;

    private final int database;

    private final String clientName;

    /**
     * Create a RoundRobinJedisPool with default timeout.
     * <p>
     * We create a CuratorFramework with infinite retry number. If you do not like the behavior, use
     * the other constructor that allow you pass a CuratorFramework created by yourself.
     * 
     * @param zkAddr
     *            ZooKeeper connect string. e.g., "zk1:2181"
     * @param zkSessionTimeoutMs
     *            ZooKeeper session timeout in ms
     * @param zkPath
     *            the codis proxy dir on ZooKeeper. e.g., "/zk/codis/db_xxx/proxy"
     * @param poolConfig
     *            same as JedisPool
     * @see #RoundRobinJedisPool(String, int, String, JedisPoolConfig, int)
     */
    @Deprecated
    public RoundRobinJedisPool(String zkAddr, int zkSessionTimeoutMs, String zkPath,
            JedisPoolConfig poolConfig) {
        this(zkAddr, zkSessionTimeoutMs, zkPath, poolConfig, JEDIS_POOL_TIMEOUT_UNSET);
    }

    /**
     * Create a RoundRobinJedisPool.
     * <p>
     * We create a CuratorFramework with infinite retry number. If you do not like the behavior, use
     * the other constructor that allow you pass a CuratorFramework created by yourself.
     * 
     * @param zkAddr
     *            ZooKeeper connect string. e.g., "zk1:2181"
     * @param zkSessionTimeoutMs
     *            ZooKeeper session timeout in ms
     * @param zkPath
     *            the codis proxy dir on ZooKeeper. e.g., "/zk/codis/db_xxx/proxy"
     * @param poolConfig
     *            same as JedisPool
     * @param timeout
     *            timeout of JedisPool
     * @see #RoundRobinJedisPool(CuratorFramework, boolean, String, JedisPoolConfig, int)
     */
    @Deprecated
    public RoundRobinJedisPool(String zkAddr, int zkSessionTimeoutMs, String zkPath,
            JedisPoolConfig poolConfig, int timeout) {
        this(CuratorFrameworkFactory.builder().connectString(zkAddr)
                .sessionTimeoutMs(zkSessionTimeoutMs)
                .retryPolicy(new BoundedExponentialBackoffRetryUntilElapsed(
                        CURATOR_RETRY_BASE_SLEEP_MS, CURATOR_RETRY_MAX_SLEEP_MS, -1L))
                .build(), true, zkPath, poolConfig, timeout);
    }

    /**
     * Create a RoundRobinJedisPool with default timeout.
     * 
     * @param curatorClient
     *            We will start it if it has not started yet.
     * @param closeCurator
     *            Whether to close the curatorClient passed in when close.
     * @param zkPath
     *            the codis proxy dir on ZooKeeper. e.g. "/zk/codis/db_xxx/proxy"
     * @param poolConfig
     *            same as JedisPool
     */
    @Deprecated
    public RoundRobinJedisPool(CuratorFramework curatorClient, boolean closeCurator, String zkPath,
            JedisPoolConfig poolConfig) {
        this(curatorClient, closeCurator, zkPath, poolConfig, JEDIS_POOL_TIMEOUT_UNSET);
    }

    /**
     * Create a RoundRobinJedisPool.
     * 
     * @param curatorClient
     *            We will start it if it has not started yet.
     * @param closeCurator
     *            Whether to close the curatorClient passed in when close.
     * @param zkPath
     *            the codis proxy dir on ZooKeeper. e.g. "/zk/codis/db_xxx/proxy"
     * @param poolConfig
     *            same as JedisPool
     * @param timeout
     *            timeout of JedisPool
     */
    @Deprecated
    public RoundRobinJedisPool(CuratorFramework curatorClient, boolean closeCurator, String zkPath,
            JedisPoolConfig poolConfig, int timeout) {
        this(curatorClient, closeCurator, zkPath, poolConfig, timeout, timeout, null,
                Protocol.DEFAULT_DATABASE, null);
    }

    private RoundRobinJedisPool(CuratorFramework curatorClient, boolean closeCurator,
            String zkProxyDir, JedisPoolConfig poolConfig, int connectionTimeoutMs, int soTimeoutMs,
            String password, int database, String clientName) {
        this.poolConfig = poolConfig;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.soTimeoutMs = soTimeoutMs;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.curatorClient = curatorClient;
        this.closeCurator = closeCurator;
        watcher = new PathChildrenCache(curatorClient, zkProxyDir, true);
        watcher.getListenable().addListener(new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                    throws Exception {
                StringBuilder sb = new StringBuilder("zookeeper event received: type=")
                        .append(event.getType());
                if (event.getData() != null) {
                    ChildData data = event.getData();
                    sb.append(", path=").append(data.getPath()).append(", stat=")
                            .append(data.getStat());
                }
                LOG.info(sb.toString());
                if (RESET_TYPES.contains(event.getType())) {
                    resetPools();
                }
            }
        });
        // we need to get the initial data so client must be started
        if (curatorClient.getState() == CuratorFrameworkState.LATENT) {
            curatorClient.start();
        }
        try {
            watcher.start(StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            throw new JedisException(e);
        }
        resetPools();
    }

    private void resetPools() {
        ImmutableList<PooledObject> pools = this.pools;
        Map<String, PooledObject> addr2Pool = Maps.newHashMapWithExpectedSize(pools.size());
        for (PooledObject pool: pools) {
            addr2Pool.put(pool.addr, pool);
        }
        ImmutableList.Builder<PooledObject> builder = ImmutableList.builder();
        for (ChildData childData: watcher.getCurrentData()) {
            try {
                CodisProxyInfo proxyInfo = MAPPER.readValue(childData.getData(),
                        CodisProxyInfo.class);
                if (!CODIS_PROXY_STATE_ONLINE.equals(proxyInfo.getState())) {
                    continue;
                }
                String addr = proxyInfo.getAddr();
                PooledObject pool = addr2Pool.remove(addr);
                if (pool == null) {
                    LOG.info("Add new proxy: " + addr);
                    String[] hostAndPort = addr.split(":");
                    String host = hostAndPort[0];
                    int port = Integer.parseInt(hostAndPort[1]);
                    pool = new PooledObject(addr, new JedisPool(poolConfig, host, port,
                            connectionTimeoutMs, soTimeoutMs, password, database, clientName));
                }
                builder.add(pool);
            } catch (Throwable t) {
                LOG.warn("parse " + childData.getPath() + " failed", t);
            }
        }
        this.pools = builder.build();
        for (PooledObject pool: addr2Pool.values()) {
            LOG.info("Remove proxy: " + pool.addr);
            pool.pool.close();
        }
    }

    @Override
    public Jedis getResource() {
        ImmutableList<PooledObject> pools = this.pools;
        if (pools.isEmpty()) {
            throw new JedisException("Proxy list empty");
        }
        for (;;) {
            int current = nextIdx.get();
            int next = current >= pools.size() - 1 ? 0 : current + 1;
            if (nextIdx.compareAndSet(current, next)) {
                return pools.get(next).pool.getResource();
            }
        }
    }

    @Override
    public void close() {
        try {
            Closeables.close(watcher, true);
        } catch (IOException e) {
            throw new AssertionError("IOException should not have been thrown", e);
        }
        if (closeCurator) {
            curatorClient.close();
        }
        List<PooledObject> pools = this.pools;
        this.pools = ImmutableList.of();
        for (PooledObject pool: pools) {
            pool.pool.close();
        }
    }

    /**
     * Create a {@link RoundRobinJedisPool} using the fluent style api.
     * 
     * @return
     */
    public static Builder create() {
        return new Builder();
    }

    public static final class Builder {

        private CuratorFramework curatorClient;

        private boolean closeCurator;

        private String zkProxyDir;

        private String zkAddr;

        private int zkSessionTimeoutMs;

        private JedisPoolConfig poolConfig;

        private int connectionTimeoutMs = Protocol.DEFAULT_TIMEOUT;

        private int soTimeoutMs = Protocol.DEFAULT_TIMEOUT;

        private String password;

        private int database = Protocol.DEFAULT_DATABASE;

        private String clientName;

        private Builder() {}

        /**
         * Set curator client.
         * 
         * @param curatorClient
         *            the client to be used
         * @param closeCurator
         *            whether to close curator client while closing pool
         */
        public Builder curatorClient(CuratorFramework curatorClient, boolean closeCurator) {
            this.curatorClient = curatorClient;
            this.closeCurator = closeCurator;
            return this;
        }

        /**
         * Set codis proxy path on zk.
         * 
         * @param zkProxyDir
         *            the codis proxy dir on ZooKeeper. e.g., "/zk/codis/db_xxx/proxy"
         */
        public Builder zkProxyDir(String zkProxyDir) {
            this.zkProxyDir = zkProxyDir;
            return this;
        }

        /**
         * Set curator client.
         * <p>
         * We will create curator client based on these parameters and close it while closing pool.
         * 
         * @param zkAddr
         *            ZooKeeper connect string. e.g., "zk1:2181"
         * @param zkSessionTimeoutMs
         *            ZooKeeper session timeout in milliseconds
         */
        public Builder curatorClient(String zkAddr, int zkSessionTimeoutMs) {
            this.zkAddr = zkAddr;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            return this;
        }

        /**
         * Set jedis pool config.
         */
        public Builder poolConfig(JedisPoolConfig poolConfig) {
            this.poolConfig = poolConfig;
            return this;
        }

        /**
         * Set jedis pool timeout in milliseconds.
         * <p>
         * We will set connectionTimeoutMs and soTimeoutMs both.
         * 
         * @param timeoutMs
         *            timeout is milliseconds
         */
        public Builder timeoutMs(int timeoutMs) {
            this.connectionTimeoutMs = this.soTimeoutMs = timeoutMs;
            return this;
        }

        /**
         * Set jedis pool connection timeout in milliseconds.
         * 
         * @param connectionTimeoutMs
         *            timeout is milliseconds
         */
        public Builder connectionTimeoutMs(int connectionTimeoutMs) {
            this.connectionTimeoutMs = connectionTimeoutMs;
            return this;
        }

        /**
         * Set jedis pool connection soTimeout in milliseconds.
         * 
         * @param soTimeoutMs
         *            timeout is milliseconds
         */
        public Builder soTimeoutMs(int soTimeoutMs) {
            this.soTimeoutMs = soTimeoutMs;
            return this;
        }

        /**
         * Set password.
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * Set redis database.
         */
        public Builder database(int database) {
            this.database = database;
            return this;
        }

        /**
         * Set redis client name.
         */
        public Builder clientName(String clientName) {
            this.clientName = clientName;
            return this;
        }

        private void validate() {
            Preconditions.checkNotNull(zkProxyDir, "zkProxyDir can not be null");
            if (curatorClient == null) {
                Preconditions.checkNotNull(zkAddr, "zk client can not be null");
                curatorClient = CuratorFrameworkFactory.builder().connectString(zkAddr)
                        .sessionTimeoutMs(zkSessionTimeoutMs)
                        .retryPolicy(new BoundedExponentialBackoffRetryUntilElapsed(
                                CURATOR_RETRY_BASE_SLEEP_MS, CURATOR_RETRY_MAX_SLEEP_MS, -1L))
                        .build();
                curatorClient.start();
                closeCurator = true;
            }
            if (poolConfig == null) {
                poolConfig = new JedisPoolConfig();
            }
        }

        /**
         * Create the {@link RoundRobinJedisPool}.
         */
        public RoundRobinJedisPool build() {
            validate();
            return new RoundRobinJedisPool(curatorClient, closeCurator, zkProxyDir, poolConfig,
                    connectionTimeoutMs, soTimeoutMs, password, database, clientName);
        }
    }
}

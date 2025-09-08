package org.crazycake.shiro;

import io.valkey.*;
import io.valkey.params.ScanParams;
import io.valkey.resps.ScanResult;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RedisClusterManager implements IRedisManager {

    private static final int DEFAULT_COUNT = 100;
    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final String DEFAULT_HOST = "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002";
    private String host = DEFAULT_HOST;

    // timeout for jedis try to connect to redis server, not expire time! In
    // milliseconds
    private int timeout = Protocol.DEFAULT_TIMEOUT;

    // timeout for jedis try to read data from redis server
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;

    private String password;

    private int database = Protocol.DEFAULT_DATABASE;

    private int count = DEFAULT_COUNT;

    private int maxAttempts = DEFAULT_MAX_ATTEMPTS;

    /**
     * JedisPoolConfig used to initialize JedisPool.
     */
    private JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

    private volatile JedisCluster jedisCluster = null;

    private void init() {
        if (jedisCluster == null) {
            synchronized (RedisClusterManager.class) {
                if (jedisCluster == null) {
                    GenericObjectPoolConfig<Connection> connectionPoolConfig = new GenericObjectPoolConfig<>();
                    connectionPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
                    connectionPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
                    connectionPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());
                    connectionPoolConfig.setLifo(jedisPoolConfig.getLifo());
                    connectionPoolConfig.setFairness(jedisPoolConfig.getFairness());
                    connectionPoolConfig.setMaxWait(jedisPoolConfig.getMaxWaitDuration());
                    connectionPoolConfig.setMinEvictableIdleTime(jedisPoolConfig.getMinEvictableIdleDuration());
                    connectionPoolConfig.setSoftMinEvictableIdleTime(jedisPoolConfig.getSoftMinEvictableIdleDuration());
                    connectionPoolConfig.setNumTestsPerEvictionRun(jedisPoolConfig.getNumTestsPerEvictionRun());
                    connectionPoolConfig.setEvictionPolicyClassName(jedisPoolConfig.getEvictionPolicyClassName());
                    connectionPoolConfig.setTestOnCreate(jedisPoolConfig.getTestOnCreate());
                    connectionPoolConfig.setTestOnBorrow(jedisPoolConfig.getTestOnBorrow());
                    connectionPoolConfig.setTestOnReturn(jedisPoolConfig.getTestOnReturn());
                    connectionPoolConfig.setTestWhileIdle(jedisPoolConfig.getTestWhileIdle());
                    connectionPoolConfig.setTimeBetweenEvictionRuns(jedisPoolConfig.getTimeBetweenEvictionRuns());
                    connectionPoolConfig.setBlockWhenExhausted(jedisPoolConfig.getBlockWhenExhausted());
                    connectionPoolConfig.setJmxEnabled(jedisPoolConfig.getJmxEnabled());
                    connectionPoolConfig.setJmxNamePrefix(jedisPoolConfig.getJmxNamePrefix());
                    connectionPoolConfig.setJmxNameBase(jedisPoolConfig.getJmxNameBase());
                    jedisCluster = new JedisCluster(getHostAndPortSet(), timeout, soTimeout, maxAttempts, password,
                            connectionPoolConfig);
                }
            }
        }
    }

    private Set<HostAndPort> getHostAndPortSet() {
        String[] hostAndPortArr = host.split(",");
        Set<HostAndPort> hostAndPorts = new HashSet<HostAndPort>();
        for (String hostAndPortStr : hostAndPortArr) {
            String[] hostAndPort = hostAndPortStr.split(":");
            hostAndPorts.add(new HostAndPort(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        return hostAndPorts;
    }

    protected JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            init();
        }
        return jedisCluster;
    }

    @Override
    public byte[] get(byte[] key) {
        if (key == null) {
            return null;
        }
        return getJedisCluster().get(key);
    }

    @Override
    public byte[] set(byte[] key, byte[] value, int expireTime) {
        if (key == null) {
            return null;
        }
        getJedisCluster().set(key, value);
        if (expireTime >= 0) {
            getJedisCluster().expire(key, expireTime);
        }
        return value;
    }

    @Override
    public void del(byte[] key) {
        if (key == null) {
            return;
        }
        getJedisCluster().del(key);
    }

    @Override
    public Long dbSize(byte[] pattern) {
        long dbSize = 0L;
        Map<String, ConnectionPool> clusterNodes = getJedisCluster().getClusterNodes();
        for (Map.Entry<String, ConnectionPool> node : clusterNodes.entrySet()) {
            String[] hostAndPort = node.getKey().split(":");
            try (JedisPool jedisPool = new JedisPool(getJedisPoolConfig(), hostAndPort[0],
                    Integer.parseInt(hostAndPort[1]), timeout, password)) {
                long nodeDbSize = getDbSizeFromClusterNode(jedisPool, pattern);
                if (nodeDbSize == 0L) {
                    continue;
                }
                dbSize += nodeDbSize;
            }
        }
        return dbSize;
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        Set<byte[]> keys = new HashSet<byte[]>();
        Map<String, ConnectionPool> clusterNodes = getJedisCluster().getClusterNodes();
        for (Map.Entry<String, ConnectionPool> node : clusterNodes.entrySet()) {
            String[] hostAndPort = node.getKey().split(":");
            try (JedisPool jedisPool = new JedisPool(getJedisPoolConfig(), hostAndPort[0],
                    Integer.parseInt(hostAndPort[1]), timeout, password)) {
                Set<byte[]> nodeKeys = getKeysFromClusterNode(jedisPool, pattern);
                if (nodeKeys.isEmpty()) {
                    continue;
                }
                keys.addAll(nodeKeys);
            }
        }

        return keys;
    }

    private Set<byte[]> getKeysFromClusterNode(JedisPool jedisPool, byte[] pattern) {
        Set<byte[]> keys = new HashSet<byte[]>();
        Jedis jedis = jedisPool.getResource();

        try {
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(pattern);
            byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
            ScanResult<byte[]> scanResult;
            do {
                scanResult = jedis.scan(cursor, params);
                keys.addAll(scanResult.getResult());
                cursor = scanResult.getCursorAsBytes();
            } while (scanResult.getCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);
        } finally {
            jedis.close();
        }
        return keys;
    }

    private long getDbSizeFromClusterNode(JedisPool jedisPool, byte[] pattern) {
        long dbSize = 0L;
        Jedis jedis = jedisPool.getResource();

        try {
            ScanParams params = new ScanParams();
            params.count(count);
            params.match(pattern);
            byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
            ScanResult<byte[]> scanResult;
            do {
                scanResult = jedis.scan(cursor, params);
                dbSize++;
                cursor = scanResult.getCursorAsBytes();
            } while (scanResult.getCursor().compareTo(ScanParams.SCAN_POINTER_START) > 0);
        } finally {
            jedis.close();
        }
        return dbSize;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    public JedisPoolConfig getJedisPoolConfig() {
        return jedisPoolConfig;
    }

    public void setJedisPoolConfig(JedisPoolConfig jedisPoolConfig) {
        this.jedisPoolConfig = jedisPoolConfig;
    }
}

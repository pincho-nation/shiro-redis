package org.crazycake.shiro;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.valkey.JedisCluster;

public class RedisClusterManagerTest {

    @Mock
    private JedisCluster jedisCluster;
    @InjectMocks
    private RedisClusterManager redisClusterManager;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        redisClusterManager.setJedisCluster(jedisCluster);
    }

    @Test
    public void get() {
        byte[] value = redisClusterManager.get(null);
        assertThat(value, is(nullValue()));
        byte[] testKey = "123".getBytes();
        byte[] expectValue = "abc".getBytes();
        when(jedisCluster.get(testKey)).thenReturn(expectValue);
        byte[] testValue = redisClusterManager.get(testKey);
        assertThat(testValue, is(expectValue));
    }

    @Test
    public void set() {
        redisClusterManager.set(null, null, -1);
        verify(jedisCluster, times(0)).set(any(byte[].class), any(byte[].class));
        redisClusterManager.set(new byte[0], new byte[0], -1);
        verify(jedisCluster, times(1)).set(any(byte[].class), any(byte[].class));
        verify(jedisCluster, times(0)).expire(any(byte[].class), anyLong());
        redisClusterManager.set(new byte[0], new byte[0], 0);
        verify(jedisCluster, times(1)).expire(any(byte[].class), anyLong());
        redisClusterManager.set(new byte[0], new byte[0], 1);
        verify(jedisCluster, times(2)).expire(any(byte[].class), anyLong());
    }
}
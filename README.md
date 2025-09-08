shiro-redis
=============

## Introduction

shiro only provide the support of ehcache and concurrentHashMap. Here is an implement of redis cache can be used by shiro. Hope it will help you!

## Documentation

Official documentation [is located here](http://alexxiyang.github.io/shiro-redis/).

## Changes in This Fork

This fork of `shiro-redis` has been significantly updated to modernize the library, improve performance, and increase code quality. The key changes are:

*   **Upgraded Redis Client:** Migrated the project from the old `redis.clients:jedis` to the latest stable `io.valkey:valkey-java` (version 5.3.0). This new client is a fork of Jedis and is compatible with both Valkey and Redis servers.
*   **Removed Lettuce Support:** To simplify the library and remove unused dependencies, all code related to the Lettuce Redis client has been removed.
*   **Upgraded Dependencies:** All major dependencies have been updated to their latest stable versions, including SLF4J, JUnit, Mockito, and Shiro.
*   **Fixed Critical Performance Bug:** A major performance issue in the `RedisClusterManager` has been fixed. The original implementation created a new connection pool for every node in the cluster on every `dbSize()` or `keys()` call. The new implementation reuses existing connections, which will significantly improve performance in a cluster environment.
*   **Fixed `dbSize()` Bug:** The `dbSize()` method in the `RedisClusterManager` was not correctly counting the number of keys. This has been fixed to accurately return the number of keys in the cache.
*   **Full Test Suite:** The entire test suite, including unit and integration tests, is passing with the new dependencies and bug fixes.


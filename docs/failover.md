# Jedis Failover

Jedis supports failover for your Redis deployments. This is useful when:
1. You have more than one Redis deployment. This might include two independent Redis servers or two or more Redis databases replicated across multiple [active-active Redis Enterprise](https://docs.redis.com/latest/rs/databases/active-active/) clusters.
2. You want your application to connect to and use one deployment at a time.
3. You want your application to fail over to the next available deployment if the current deployment becomes unavailable.

Jedis will fail over to a subsequent Redis deployment after reaching a configurable failure threshold.
This failure threshold is implemented using a [circuit breaker pattern](https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern).

You can also configure Jedis to retry failed calls to Redis.
Once a maximum number of retries have been exhausted, the circuit breaker will record a failure.
When the circuit breaker reaches its failure threshold, a failover is triggered.

The remainder of this guide describes:

* A basic failover configuration
* Supported retry and circuit breaker settings
* Considerations for failing back

We recommend that you read this guide carefully and understand the configuration settings before enabling Jedis failover
in production.

## Basic usage

To configure Jedis for failover, you specify an ordered list of Redis databases to connect to.
By default, Jedis will connect to the first Redis database in the list. If the first database becomes unavailable,
Jedis will attempt to connect to the next database in the list, and so on.

Suppose you run two Redis deployments.
We'll call them `redis-east` and `redis-west`.
You want your application to first connect to `redis-east`.
If `redis-east` becomes unavailable, you want your application to connect to `redis-west`.

Let's look at one way of configuring Jedis for this scenario.

First create an array of `ClusterJedisClientConfig` objects, one for each Redis database.

```java
JedisClientConfig config = DefaultJedisClientConfig.builder().user("cache").password("secret").build();

ClusterJedisClientConfig[] clientConfigs = new ClusterJedisClientConfig[2];
clientConfigs[0] = new ClusterJedisClientConfig(new HostAndPort("redis-east.example.com", 14000), config);
clientConfigs[1] = new ClusterJedisClientConfig(new HostAndPort("redis-west.example.com", 14000), config);
```

The configuration above represents your two Redis deployments: `redis-east` and `redis-west`.
You'll use this array of configuration objects to create a connection provider that supports failover.

Use the `MultiClusterJedisClientConfig` builder to set your preferred retry and failover configuration, passing in the client configs you just created.
Then build a `MultiClusterPooledConnectionProvider`.

```java
MultiClusterJedisClientConfig.Builder builder = new MultiClusterJedisClientConfig.Builder(clientConfigs);
builder.circuitBreakerSlidingWindowSize(10);
builder.circuitBreakerSlidingWindowMinCalls(1);
builder.circuitBreakerFailureRateThreshold(50.0f);

MultiClusterPooledConnectionProvider provider = new MultiClusterPooledConnectionProvider(builder.build());
```

Internally, the connection provider uses a [highly configurable circuit breaker and retry implementation](https://resilience4j.readme.io/docs/circuitbreaker) to determine when to fail over.
In the configuration here, we've set a sliding window size of 10 and a failure rate threshold of 50%.
This means that a failover will be triggered if 5 out of any 10 calls to Redis fail.

Once you've configured and created a `MultiClusterPooledConnectionProvider`, instantiate a `UnifiedJedis` instance for your application, passing in the provider you just created:

```java
UnifiedJedis jedis = new UnifiedJedis(provider);
```

## Configuration options

Under the hood, Jedis' failover support relies on [resilience4j](https://resilience4j.readme.io/docs/getting-started),
a fault-tolerance library that implements [retry](https://resilience4j.readme.io/docs/retry) and [circuit breakers](https://resilience4j.readme.io/docs/circuitbreaker).

Once you configure Jedis for failover using the `MultiClusterPooledConnectionProvider`, each call to Redis is decorated with resilience4j retry and circuit breaker.

By default, any call that throws a `JedisConnectionException` will be retried up to 3 times.
If the call continues to fail after the max number of retry attempts, then the circuit breaker will record a failure.
The circuit breaker maintains a record of failures in a sliding window data structure.
If the failure rate reaches a configured threshold (e.g., over 50% of the last 10 calls have failed),
then the circuit breaker's state transitions from `CLOSED` to `OPEN`.
When this occurs, Jedis will attempt to connect to the next Redis database in its client configuration list.

The supported retry and circuit breaker settings, and their default values, are described below.
You can configure any of these settings using the `MultiClusterJedisClientConfig.Builder` builder.
Refer the basic usage above for an example of this.

### Retry configuration

Jedis uses the following retry settings:

| Setting                          | Default value              | Description                                                                                                                                                                                      |
|----------------------------------|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Max retry attempts               | 3                          | Maximum number of retry attempts (including the initial call)                                                                                                                                    |
| Retry wait duration              | 500 ms                     | Number of milliseconds to wait between retry attempts                                                                                                                                            |
| Wait duration backoff multiplier | 2                          | Exponential backoff factor against wait duration between retries. For example, with a wait duration of 1s and a multiplier of 2, the retries would be done after 1s, 2s, 4s, 8s, 16s, and so on. |
| Retry included exception list    | `JedisConnectionException` | A list of `Throwable` classes that count as failures and should be retried.                                                                                                                      |
| Retry ignored exception list     | Empty list                 | A list of `Throwable` classes to explicitly ignore for the purposes of retry.                                                                                                                    |

To disable retry, set `maxRetryAttempts` to 1.

### Circuit breaker configuration

Jedis uses the following circuit breaker settings:

| Setting                                 | Default value              | Description                                                                                                                                                                                      |
|-----------------------------------------|----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Sliding window type                     | `COUNT_BASED`              | The type of sliding window used to record the outcome of calls. Options are `COUNT_BASED` and `TIME_BASED`.                                                                                      |
| Sliding window size                     | 100                        | The size of the sliding window. Units depend on sliding window type. When `COUNT_BASED`, the size represents number of calls. When `TIME_BASED`, the size represents seconds.                    |
| Sliding window min calls                | 100                        | Minimum number of calls required (per sliding window period) before the CircuitBreaker can calculate the error rate or slow call rate.                                                           |                                                          | 
| Wait duration backoff multiplier        | 2                          | Exponential backoff factor against wait duration between retries. For example, with a wait duration of 1s and a multiplier of 2, the retries would be done after 1s, 2s, 4s, 8s, 16s, and so on. |
| Failure rate threshold                  | `50.0f`                    | Percentage of calls within the sliding window that must fail before the circuit breaker transitions to the `OPEN` state.                                                                         |
| Slow call duration threshold            | 60000 ms                   | Duration threshold above which calls are considered as slow and increase the rate of slow calls                                                                                                  |                                                                                              |
| Slow call rate threshold                | `100.0f`                   | Percentage of calls within the sliding window that exceed the slow call duration threshold before circuit breaker transitions to the `OPEN` state.                                               |
| Circuit breaker included exception list | `JedisConnectionException` | A list of `Throwable` classes that count as failures and add the failure rate.                                                                                                                   |
| Circuit breaker ignored exception list  | Empty list                 | A list of `Throwable` classes to explicitly ignore for failure rate calcuations.                                                                                                                 |                                                                                                               |

## Considerations for failing back

When a failover is triggered, Jedis will attempt to connect to the next Redis server in the list of server configurations
you provide at setup.

Recall the `redis-east` and `redis-west` deployments from the basic usage example above.
Jedis will attempt to connect to `redis-east` first.
If `redis-east` becomes unavailable (and the circuit breaker transitions), then Jedis will attempt to use `redis-west`.

Now suppose that `redis-east` eventually comes back online.
You will likely want to fail your application back to `redis-east`.
However, Jedis will not fail back to `redis-east` automatically.
In this case, we recommend that you first ensure that your `redis-east` deployment is healthy before you fail back your application.

Once you're determined that `redis-east` is healthy, you have two ways to fail back your application.



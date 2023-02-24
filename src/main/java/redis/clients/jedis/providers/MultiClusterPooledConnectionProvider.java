package redis.clients.jedis.providers;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.MultiClusterJedisClientConfig.ClusterJedisClientConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisValidationException;
import redis.clients.jedis.util.Pool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Allen Terleto (aterleto)
 * <p>
 * ConnectionProvider which supports multiple cluster/database endpoints each with their own isolated connection pool.
 * With this ConnectionProvider users can seamlessly failover to Disaster Recovery (DR), Backup, and Active-Active cluster(s)
 * by using simple configuration which is passed through from Resilience4j - https://resilience4j.readme.io/docs
 * <p>
 * Support for manunal failback is provided by way of {@link #setActiveMultiClusterIndex(int)}
 * <p>
 */
public class MultiClusterPooledConnectionProvider implements ConnectionProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<Integer, Cluster> clusterMap = new ConcurrentHashMap<>();
    private volatile Integer activeMultiClusterIndex = 1; // Current active cluster to which all traffic will be routed


    public MultiClusterPooledConnectionProvider(MultiClusterJedisClientConfig multiClusterJedisClientConfig) {

        if (multiClusterJedisClientConfig == null)
            throw new JedisValidationException("MultiClusterJedisClientConfig must not be NULL for MultiClusterPooledConnectionProvider");

        ////////////// Configure Retry ////////////////////

        RetryConfig.Builder retryConfigBuilder = RetryConfig.custom();
        retryConfigBuilder.maxAttempts(multiClusterJedisClientConfig.getRetryMaxAttempts());
        retryConfigBuilder.intervalFunction(IntervalFunction.ofExponentialBackoff(multiClusterJedisClientConfig.getRetryWaitDuration(),
                multiClusterJedisClientConfig.getRetryWaitDurationExponentialBackoffMultiplier()));
        retryConfigBuilder.failAfterMaxAttempts(false); // JedisConnectionException will be thrown
        retryConfigBuilder.retryExceptions(multiClusterJedisClientConfig.getRetryIncludedExceptionList().stream().toArray(Class[]::new));

        List<Class> retryIgnoreExceptionList = multiClusterJedisClientConfig.getRetryIgnoreExceptionList();
        if (retryIgnoreExceptionList != null && !retryIgnoreExceptionList.isEmpty())
            retryConfigBuilder.ignoreExceptions(retryIgnoreExceptionList.stream().toArray(Class[]::new));

        RetryConfig retryConfig = retryConfigBuilder.build();

        ////////////// Configure Circuit Breaker ////////////////////

        CircuitBreakerConfig.Builder circuitBreakerConfigBuilder = CircuitBreakerConfig.custom();
        circuitBreakerConfigBuilder.failureRateThreshold(multiClusterJedisClientConfig.getCircuitBreakerFailureRateThreshold());
        circuitBreakerConfigBuilder.slowCallRateThreshold(multiClusterJedisClientConfig.getCircuitBreakerSlowCallRateThreshold());
        circuitBreakerConfigBuilder.slowCallDurationThreshold(multiClusterJedisClientConfig.getCircuitBreakerSlowCallDurationThreshold());
        circuitBreakerConfigBuilder.minimumNumberOfCalls(multiClusterJedisClientConfig.getCircuitBreakerSlidingWindowMinCalls());
        circuitBreakerConfigBuilder.slidingWindowType(multiClusterJedisClientConfig.getCircuitBreakerSlidingWindowType());
        circuitBreakerConfigBuilder.slidingWindowSize(multiClusterJedisClientConfig.getCircuitBreakerSlidingWindowSize());
        circuitBreakerConfigBuilder.recordExceptions(multiClusterJedisClientConfig.getCircuitBreakerIncludedExceptionList().stream().toArray(Class[]::new));
        circuitBreakerConfigBuilder.automaticTransitionFromOpenToHalfOpenEnabled(false); // State transitions are forced. No half open states are used

        List<Class> circuitBreakerIgnoreExceptionList = multiClusterJedisClientConfig.getCircuitBreakerIgnoreExceptionList();
        if (circuitBreakerIgnoreExceptionList != null && !circuitBreakerIgnoreExceptionList.isEmpty())
            circuitBreakerConfigBuilder.ignoreExceptions(circuitBreakerIgnoreExceptionList.stream().toArray(Class[]::new));

        CircuitBreakerConfig circuitBreakerConfig = circuitBreakerConfigBuilder.build();

        ////////////// Configure Cluster Map ////////////////////

        ClusterJedisClientConfig[] clusterConfigs = multiClusterJedisClientConfig.getClusterJedisClientConfigs();
        for (ClusterJedisClientConfig config : clusterConfigs) {

            String clusterId = "cluster:" + config.getPriority() + ":" + config.getHostAndPort();

            Retry retry = RetryRegistry.of(retryConfig).retry(clusterId);


            Retry.EventPublisher retryPublisher = retry.getEventPublisher();
            retryPublisher.onRetry(event -> log.warn(String.valueOf(event)));
            retryPublisher.onError(event -> log.error(String.valueOf(event)));

            CircuitBreaker circuitBreaker = CircuitBreakerRegistry.of(circuitBreakerConfig).circuitBreaker(clusterId);

            CircuitBreaker.EventPublisher circuitBreakerEventPublisher = circuitBreaker.getEventPublisher();
            circuitBreakerEventPublisher.onCallNotPermitted(event -> log.error(String.valueOf(event)));
            circuitBreakerEventPublisher.onError(event -> log.error(String.valueOf(event)));
            circuitBreakerEventPublisher.onFailureRateExceeded(event -> log.error(String.valueOf(event)));
            circuitBreakerEventPublisher.onSlowCallRateExceeded(event -> log.error(String.valueOf(event)));
            circuitBreakerEventPublisher.onStateTransition(event -> log.warn(String.valueOf(event)));

            clusterMap.put(config.getPriority(), new Cluster(new ConnectionPool(config.getHostAndPort(),
                                                                                config.getJedisClientConfig()),
                                                                                retry, circuitBreaker));
        }
    }

    /**
     * Increments the index used by all interactions with this provider as a representation
     * of the currently active connection through which all commands should flow through.
     *
     * Only indexes within the preconfigured range (static) are supported
     * otherwise an exception will be thrown.
     *
     * In the event that the next prioritized connection has a forced open state,
     * the method will recursively increment the index as to avoid a failed command.
     */
    public void incrementActiveMultiClusterIndex() {

        synchronized (activeMultiClusterIndex) {
            if (++activeMultiClusterIndex > clusterMap.size())
                throw new JedisConnectionException(getClusterCircuitBreaker(--activeMultiClusterIndex).getName() +
                        " failed to connect. No additional clusters were configured for failover");
        }

        // Handles edge-case in which the user resets the activeMultiClusterIndex to a higher priority prematurely
        // which forces a failover to the next prioritized cluster that has potentially not yet recovered
        if (CircuitBreaker.State.FORCED_OPEN.equals(getClusterCircuitBreaker().getState()))
            incrementActiveMultiClusterIndex();
    }

    /**
     * Design decision was made to defer responsibility for cross-replication validation to the user.
     *
     * Alternatively there was discussion to handle cross-cluster replication validation by
     * setting a key/value pair per hashslot in the active connection (with a TTL) and
     * subsequently reading it from the target connection.
     */
    public void validateTargetConnection(int multiClusterIndex) {

        CircuitBreaker circuitBreaker = getClusterCircuitBreaker(multiClusterIndex);

        // Synchronization on the circuit breaker is used to avoid any mutation in
        // between forced transitioning from closed and opened if unsuccessful
        synchronized (circuitBreaker) {

            State originalState = circuitBreaker.getState();

            Connection targetConnection = null;
            try {
                // Transitions the state machine to a CLOSED state, allowing state transition, metrics and event publishing
                circuitBreaker.transitionToClosedState();

                targetConnection = getConnection(multiClusterIndex);
                targetConnection.ping();
            }
            catch (Exception e) {

                // If the original state was FORCED_OPEN, then transition it back which stops state transition, metrics and event publishing
                if (CircuitBreaker.State.FORCED_OPEN.equals(originalState))
                    circuitBreaker.transitionToForcedOpenState();

                throw new JedisValidationException(circuitBreaker.getName() + " failed to connect. Please check configuration and try again. " + e);
            }
            finally {
                if (targetConnection != null)
                    targetConnection.close();
            }
        }
    }

    /**
     * Used to manually failback to any pre-configured cluster.
     *
     * Special care should be taken to confirm cluster/database availability AND
     * potentially cross-cluster replication BEFORE using this capability.
     */
    public void setActiveMultiClusterIndex(int multiClusterIndex) {

        if (multiClusterIndex < 1 || multiClusterIndex > clusterMap.size())
            throw new JedisValidationException("MultiClusterPriority: " + multiClusterIndex + " is not within the configured range");

        validateTargetConnection(multiClusterIndex);

        synchronized (activeMultiClusterIndex) {
            activeMultiClusterIndex = multiClusterIndex;
        }
    }

    @Override
    public void close() {
        clusterMap.get(activeMultiClusterIndex).getConnection().close();
    }

    @Override
    public Connection getConnection() {
        return clusterMap.get(activeMultiClusterIndex).getConnection().getResource();
    }

    public Connection getConnection(int multiClusterIndex) {
        return clusterMap.get(multiClusterIndex).getConnection().getResource();
    }

    @Override
    public Connection getConnection(CommandArguments args) {
        return clusterMap.get(activeMultiClusterIndex).getConnection().getResource();
    }

    @Override
    public Map<?, Pool<Connection>> getConnectionMap() {
        ConnectionPool connectionPool = clusterMap.get(activeMultiClusterIndex).getConnection();
        return Collections.singletonMap(connectionPool.getFactory(), connectionPool);
    }

    public Retry getClusterRetry() {
        return clusterMap.get(activeMultiClusterIndex).getRetry();
    }

    public CircuitBreaker getClusterCircuitBreaker() {
        return clusterMap.get(activeMultiClusterIndex).getCircuitBreaker();
    }

    public CircuitBreaker getClusterCircuitBreaker(int multiClusterIndex) {
        return clusterMap.get(multiClusterIndex).getCircuitBreaker();
    }

    static class Cluster {

        private final ConnectionPool connection;
        private final Retry retry;
        private final CircuitBreaker circuitBreaker;

        public Cluster(ConnectionPool connection, Retry retry, CircuitBreaker circuitBreaker) {
            this.connection = connection;
            this.retry = retry;
            this.circuitBreaker = circuitBreaker;
        }

        public ConnectionPool getConnection() {
            return connection;
        }

        public Retry getRetry() {
            return retry;
        }

        public CircuitBreaker getCircuitBreaker() {
            return circuitBreaker;
        }
    }

}
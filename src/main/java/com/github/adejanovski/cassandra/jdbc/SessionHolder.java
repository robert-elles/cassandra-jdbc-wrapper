/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.github.adejanovski.cassandra.jdbc;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.github.adejanovski.cassandra.jdbc.codec.BigDecimalToBigintCodec;
import com.github.adejanovski.cassandra.jdbc.codec.DoubleToDecimalCodec;
import com.github.adejanovski.cassandra.jdbc.codec.DoubleToFloatCodec;
import com.github.adejanovski.cassandra.jdbc.codec.IntToLongCodec;
import com.github.adejanovski.cassandra.jdbc.codec.ListCustomTypeCodec;
import com.github.adejanovski.cassandra.jdbc.codec.LongToIntCodec;
import com.github.adejanovski.cassandra.jdbc.codec.UdtTypeCodec;
import com.github.adejanovski.cassandra.jdbc.codec.TimestampToLongCodec;
import com.google.common.cache.LoadingCache;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.adejanovski.cassandra.jdbc.Utils.*;

/**
 * Holds a {@link Session} shared among multiple {@link CassandraConnection} objects.
 * <p>
 * This class uses reference counting to track if active CassandraConnections still use the
 * Session. When the last CassandraConnection has closed, the Session gets closed.
 */
class SessionHolder {

    private static final Logger logger = LoggerFactory.getLogger(SessionHolder.class);
    final Session session;
    final Properties properties;
    private final LoadingCache<Map<String, String>, SessionHolder> parentCache;
    private final Map<String, String> cacheKey;
    private final AtomicInteger references = new AtomicInteger();
    static final String URL_KEY = "jdbcUrl";

    SessionHolder(Map<String, String> params,
            LoadingCache<Map<String, String>, SessionHolder> parentCache) throws SQLException {
        this.cacheKey = params;
        this.parentCache = parentCache;

        String url = params.get(URL_KEY);

        // parse the URL into a set of Properties
        // replace " by ' to handle the fact that " is not a valid character in URIs
        properties = Utils.parseURL(url.replace("\"", "'"));

        // other properties in params come from the initial call to connect(), they take priority
        for (String key : params.keySet())
            if (!URL_KEY.equals(key))
                properties.put(key, params.get(key));

        if (logger.isDebugEnabled())
            logger.debug("Final Properties to Connection: {}", properties);

        session = createSession(properties);
    }

    /**
     * Indicates that a CassandraConnection has closed and stopped using this object.
     */
    void release() {
        int newRef;
        while (true) {
            int ref = references.get();
            // We set to -1 after the last release, to distinguish it from the initial state
            newRef = (ref == 1) ? -1 : ref - 1;
            if (references.compareAndSet(ref, newRef))
                break;
        }
        if (newRef == -1) {
            logger.debug("Released last reference to {}, closing Session", cacheKey.get(URL_KEY));
            dispose();
        } else {
            logger.debug("Released reference to {}, new count = {}", cacheKey.get(URL_KEY), newRef);
        }
    }

    /**
     * Called when a CassandraConnection tries to acquire a reference to this object.
     *
     * @return whether the reference was acquired successfully
     */
    boolean acquire() {
        while (true) {
            int ref = references.get();
            if (ref < 0) {
                // We raced with the release of the last reference, the caller will need to create
                // a new session
                logger.debug("Failed to acquire reference to {}", cacheKey.get(URL_KEY));
                return false;
            }
            if (references.compareAndSet(ref, ref + 1)) {
                logger.debug("Acquired reference to {}, new count = {}", cacheKey.get(URL_KEY),
                        ref + 1);
                return true;
            }
        }
    }

    @SuppressWarnings("resource")
    private Session createSession(Properties properties) throws SQLException {
        String hosts = properties.getProperty(TAG_SERVER_NAME);
        int port = Integer.parseInt(properties.getProperty(TAG_PORT_NUMBER));
        String keyspace = properties.getProperty(TAG_DATABASE_NAME);
        String username = properties.getProperty(TAG_USER, "");
        String password = properties.getProperty(TAG_PASSWORD, "");
        String loadBalancingPolicy = properties.getProperty(TAG_LOADBALANCING_POLICY, "");
        String retryPolicy = properties.getProperty(TAG_RETRY_POLICY, "");
        String reconnectPolicy = properties.getProperty(TAG_RECONNECT_POLICY, "");
        boolean debugMode = properties.getProperty(TAG_DEBUG, "").equals("true");
        // SSL Options
        String sslEnabledOption = properties.getProperty(TAG_SSL_ENABLED, "false");
        boolean sslEnabled = isTrue(sslEnabledOption);
        String verifyServerCertificateOption = properties.getProperty(TAG_VERIFY_SERVER_CERTIFICATE.
                toLowerCase(), "false");
        boolean verifyServerCertificate = isTrue(verifyServerCertificateOption);

        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints(hosts.split("--")).withPort(port);
        if (sslEnabled && verifyServerCertificate) {
            String keyStorePassword = properties.getProperty(KEY_STORE_PASSWORD, "");
            String keyStoreUrl = properties.getProperty(KEY_STORE_URL, "");
            String keyStoreFactory = properties.getProperty(KEY_STORE_FACTORY, "");
            String trustStoreUrl = properties.getProperty(TRUST_STORE_URL, "");
            String privateKeyAlias = properties.getProperty(KEY_ALIAS, "");
            String privateKeyPassPhrase = properties.getProperty(PRIVATE_KEY_PASSPHRASE, "");
            KeyStore keyStore = null;
            KeyStore trustStore = null;
            //Condition to check for custom Key Store Factory to Generate Key/Trust stores
            if (!StringUtils.isEmpty(keyStoreFactory)) {
                try {
                    Class factory = Class.forName(keyStoreFactory);
                    Method getKeyStoreMethod = factory.getDeclaredMethod(
                            "getKeyStore", Properties.class);
                    keyStore = (KeyStore) getKeyStoreMethod.invoke(null, properties);

                    Method getTrustStoreMethod = factory.getDeclaredMethod(
                            "getTrustStore", Properties.class);
                    trustStore = (KeyStore) getTrustStoreMethod.invoke(null, properties);
                } catch (ClassNotFoundException | NoSuchMethodException |
                        IllegalAccessException | InvocationTargetException e) {
                    //ignore this and proceed
                    logger.warn("Error while creating Key/Trust store ", e);
                    keyStore = null;
                    trustStore = null;
                }
            }
            SSLContext context = null;
            if (keyStore != null && trustStore != null) {
                context = SSLUtil.getSSLContextFromKeyStore(keyStore, trustStore, privateKeyAlias,
                        privateKeyPassPhrase);
            } else {
                // check keyStoreUrl
                if (!StringUtils.isEmpty(keyStoreUrl)) {
                    try {
                        new URL(keyStoreUrl);
                    } catch (MalformedURLException e) {
                        keyStoreUrl = "file:" + keyStoreUrl;
                    }
                }
                context = SSLUtil.getTrustEverybodySSLContext(keyStoreUrl, privateKeyAlias,
                        privateKeyPassPhrase, keyStorePassword, trustStoreUrl);
            }

            SSLOptions options = RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(context)
                    .build();
            builder.withSSL(options);

        } else {
            builder.withSocketOptions(new SocketOptions().setKeepAlive(true));
        }
        // Set credentials when applicable
        if (username.length() > 0) {
            builder.withCredentials(username, password);
        }

        if (loadBalancingPolicy.length() > 0) {
            // if load balancing policy has been given in the JDBC URL, parse it and add it to the
            // cluster builder
            try {
                builder.withLoadBalancingPolicy(parseLbPolicy(loadBalancingPolicy));
            } catch (Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                logger.warn("Error occurred while parsing load balancing policy :" + e.getMessage()
                        + " / Forcing to TokenAwarePolicy...");
                builder.withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
            }
        }

        if (retryPolicy.length() > 0) {
            // if retry policy has been given in the JDBC URL, parse it and add it to the cluster
            // builder
            try {
                builder.withRetryPolicy(parseRetryPolicy(retryPolicy));
            } catch (Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                logger.warn("Error occured while parsing retry policy :" + e.getMessage()
                        + " / skipping...");
            }
        }

        if (reconnectPolicy.length() > 0) {
            // if reconnection policy has been given in the JDBC URL, parse it and add it to the
            // cluster builder
            try {
                builder.withReconnectionPolicy(parseReconnectionPolicy(reconnectPolicy));
            } catch (Exception e) {
                if (debugMode) {
                    throw new SQLNonTransientConnectionException(e);
                }
                logger.warn("Error occured while parsing reconnection policy :" + e.getMessage()
                        + " / skipping...");
            }
        }

        // The codecs below were defined in the original code. We probably
        // don't need all of them, however, a couple are necessary to run the
        // tests, specifically these:
        //
        // NumericTypesUnitTest.testDecimalType [bigint <-> java.lang.Integer]
        // JdbcRegressionUnitTest.testTimestampToLongCodec [timestamp <-> java.lang.Long]

        // Declare and register codecs
        List<TypeCodec<?>> codecs = new ArrayList<TypeCodec<?>>();
        CodecRegistry customizedRegistry = new CodecRegistry();

        codecs.add(new TimestampToLongCodec(Long.class));
        codecs.add(new LongToIntCodec(Integer.class));
        codecs.add(new IntToLongCodec(Long.class));
        codecs.add(new BigDecimalToBigintCodec(BigDecimal.class));
        codecs.add(new DoubleToDecimalCodec(Double.class));
        codecs.add(new DoubleToFloatCodec(Double.class));
        codecs.add(new UdtTypeCodec());
        codecs.add(TypeCodec.list(new UdtTypeCodec()));
        codecs.add(new ListCustomTypeCodec());

        customizedRegistry.register(codecs);
        builder.withCodecRegistry(customizedRegistry);
        // end of codec register

        Cluster cluster = null;
        try {
            cluster = builder.build();
            return cluster.connect(keyspace);
        } catch (DriverException e) {
            if (cluster != null)
                cluster.close();
            throw new SQLNonTransientConnectionException(e);
        }
    }

    private void dispose() {
        // No one else has a reference to the parent Cluster, and only one Session was created from
        // it:
        session.getCluster().close();
        parentCache.invalidate(cacheKey);
    }

    private boolean isTrue(String value) {
        return value != null && (value.equals("1") || value.toLowerCase(Locale.ENGLISH)
                .equals("true"));
    }
}

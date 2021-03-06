package com.inovaworkscc.quartz.cassandra.db;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
/**
 *
 * @author bm
 */
public class CassandraConnectionManager {

    private static final Logger LOG = Logger.getLogger(CassandraConnectionManager.class.getName());
        
    //public static HazelcastInstance hzInstance;

    public static String CASSANDRA_CLUSTER_NAME;
    public static ConsistencyLevel CONSISTENCY_LEVEL;
    public static Integer CONN_POOLING_LOCAL_CORE;
    public static Integer CONN_POOLING_LOCAL_MAX;
    public static Integer CONN_POOLING_REMOTE_CORE;
    public static Integer CONN_POOLING_REMOTE_MAX;

    public static String CONTACT_POINT = null;
    public static String PORT = null;
    public static String KEYSPACE_NAME = null;
    
    private Cluster cluster;
    private CqlSession session;

    private static final Map<String, String> statementSpecsMap = new HashMap<>();
    private final Map<String, PreparedStatement> statementsMap = new HashMap<>();

    private volatile static CassandraConnectionManager instance;
    
    private static final Boolean LOCK_GET_STATEMENT=Boolean.TRUE;

    /**
     * Call before getInstance to set CONTACT_POINT, PORT and KEYSPACE_NAME
     * @param contactPoint
     * @param port
     * @param keyspaceName
     * @return 
     */
    public static String getMapValue(String key) {
    	return statementSpecsMap.get(key);
    }
    
    public static void setProperties(String contactPoint, String port, String keyspaceName) {
        
       CONTACT_POINT = contactPoint;
       PORT = port;
       KEYSPACE_NAME = keyspaceName;
    }
    
    public static CassandraConnectionManager getInstance() {
        
        CassandraConnectionManager localInstance = CassandraConnectionManager.instance;
        if (localInstance == null) {
            synchronized (CassandraConnectionManager.class) {
                localInstance = CassandraConnectionManager.instance;
                if (localInstance == null) {
                    CassandraConnectionManager.instance = localInstance = new CassandraConnectionManager();
                    LOG.warning("Creating new Cassandra CONNECTION");
                }
            }
        }
        return localInstance;
    }

    private CassandraConnectionManager() {

        if (CONSISTENCY_LEVEL == null) {
            CONSISTENCY_LEVEL = ConsistencyLevel.valueOf("ONE");
        }
        if (CONTACT_POINT == null) {
            CONTACT_POINT = "127.0.0.1";
        }

        if(CASSANDRA_CLUSTER_NAME==null) {
            CASSANDRA_CLUSTER_NAME = "quartz_nosql_cluster";
        }

        if (KEYSPACE_NAME == null) {
            KEYSPACE_NAME = "quartz_nosql";
        }
        if (PORT == null) {
            PORT = "9042";
        }

        if (CONN_POOLING_LOCAL_CORE==null){
            CONN_POOLING_LOCAL_CORE = 4;
        }
        if (CONN_POOLING_LOCAL_MAX==null){
            CONN_POOLING_LOCAL_MAX = 10;
        }
        if (CONN_POOLING_REMOTE_CORE==null){
             CONN_POOLING_REMOTE_CORE = 2;
        }
        if (CONN_POOLING_REMOTE_MAX==null){
             CONN_POOLING_REMOTE_MAX = 2;
        }
        
        try {
            this.restartDatabaseConnection();
        } catch (CassandraDatabaseException e) {
            LOG.log(Level.SEVERE, "Database connection not initialized. Will be retried uppon next invocation. Msg:"+ e.getMessage(),e);
        }
    }

    public CqlSession getSession() {
        return session;
    }

//    public Session.State getState() {
//        if (session == null) {
//            return null;
//        } else {
//            return session.getState();
//        }
//    }

    private void restartDatabaseConnection() throws CassandraDatabaseException {
        if (session != null || cluster != null) {
            this.shutdownConnection();
        }
        if (cluster == null || session == null) {
            try {
                synchronized (CassandraConnectionManager.class) {
                    if (cluster == null || session == null) {
                        PoolingOptions poolingOptions = new PoolingOptions();

                        poolingOptions
                                .setConnectionsPerHost(HostDistance.LOCAL, CONN_POOLING_LOCAL_CORE, CONN_POOLING_LOCAL_MAX)
                                .setConnectionsPerHost(HostDistance.REMOTE, CONN_POOLING_REMOTE_CORE, CONN_POOLING_REMOTE_MAX);

                        String[] contactPoints = CONTACT_POINT.split(",");
                        
//                        cluster = Cluster.builder().addContactPoints(contactPoints).withPort(Integer.parseInt(PORT))
//                                .withPoolingOptions(poolingOptions)
//                                .withQueryOptions(new QueryOptions().setConsistencyLevel(CONSISTENCY_LEVEL)).withClusterName(CASSANDRA_CLUSTER_NAME).build();
//                        session = cluster.connect(KEYSPACE_NAME);
                        
                        DriverConfigLoader loader =
            				    DriverConfigLoader.programmaticBuilder()
            				        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30)).withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(30))
            				        .startProfile("slow")
            				        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            				        .endProfile()
            				        .build();
            			session = CqlSession.builder().withConfigLoader(loader)
            			           .withCloudSecureConnectBundle(Paths.get("C:\\D_Folder\\Workspace\\Outsource\\Sunil\\demo\\src\\main\\resources\\secure-connect-mydatabase.zip"))
            			           .withAuthCredentials("PLpIsTWrapYOYDbQtUAthjLr","B8NmkG63T68FwcooevogylqxDgD9s88H_,-w0TgJ9,8n8zLQ,ki7R7schRWzbIeoMW-s_eK4R_Kt,IEx7d09JpZkYlbDdSv-BwuWnk,cdL,EzQP30A4pLboCht_rBFxp")
            			           .build();
            			
                        
//                        ClientNetworkConfig clientConfig = new ClientNetworkConfig();
//                        clientConfig.addAddress("10.10.0.178:5701");
//                        ClientConfig cf = new ClientConfig();
//                        cf.setNetworkConfig(clientConfig);
//                        hzInstance = HazelcastClient.newHazelcastClient(cf); 
                        
                        for (String key : statementSpecsMap.keySet()) {
                            try {
                                PreparedStatement statement = this.createStatement(key);
                                statementsMap.put(key, statement);

                            } catch (Exception e) {
                                //ignore error while trying to register the preparedstatement
                            }
                        }
                    }
                }
            } catch (NoHostAvailableException e) {
                
                Map<EndPoint, Throwable> errors = e.getErrors();
                
                for (Entry<EndPoint, Throwable> entry : errors.entrySet()) {
                    EndPoint key = entry.getKey();
                    Throwable value = entry.getValue();
                    
                     LOG.log(Level.SEVERE, "NoHostAvailableException. Error: " + value.getMessage(), value);
                }
                
                LOG.log(Level.SEVERE, "NoHostAvailableException. Msg:" + e.getErrors(), e);
                throw new CassandraDatabaseException("Unable to create database connection. " + e.getMessage());
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Unable to create database connection. Msg:" + e.getMessage(), e);
                throw new CassandraDatabaseException("Unable to create database connection. " + e.getMessage());
            }

        }
    }
 
    /**
     *
     * @param key
     * @param statement
     */
    public static String registerStatement(String key, String statement) {
        statementSpecsMap.put(key, statement);
//        try {
//            statementsMap.put(key, this.createStatement(key));
//        } catch (Exception e) {
//            LOG.log(Level.WARNING, "Statement creation failed during the registration. Will be automaticaly retried at next usage. {0}", e.getMessage());
//        }
        return key;
    }


    private PreparedStatement createStatement(String key) throws CassandraDatabaseException {
        String statementSpecs = this.statementSpecsMap.get(key);

        if (statementSpecs == null) {
            throw new CassandraDatabaseException(key + " not found in the registered statements.");
        } else {

            try {

                if (session == null) {
                    // no active session to the database exists
                    // if the connection fails an exception is thrown and the statement id not created
                    this.restartDatabaseConnection();
                }

                return (PreparedStatement) session.prepare(statementSpecs);

            } catch (CassandraDatabaseException e) {
                throw e;
            } catch (NoHostAvailableException ne) {
                throw new CassandraDatabaseException(ne);
            }

        }

    }

    /**
     *
     * @param key
     * @return
     * @throws InvalidStatementException
     */
    public PreparedStatement getStatement(String key) throws CassandraDatabaseException {
        PreparedStatement statement = statementsMap.get(key);
        if (statement == null) {
            synchronized (LOCK_GET_STATEMENT) {
                statement = statementsMap.get(key);
                if (statement == null) {
                    // statement not yet created
                    statement = this.createStatement(key);
                    statementsMap.put(key, statement);
                }
            }
        }

        return statement;
    }

//    public ResultSet execute(Statement stmt) {
//        return this.execute(stmt, false);
//    }

    /**
     *
     * @param stmt
     * @param retryOnceOnNoHostAvailable
     * @return
     */
//    public ResultSet execute(Statement stmt, boolean retryOnceOnNoHostAvailable) {
//        try {
//            if (session == null) {
//                restartDatabaseConnection();
//            }
//
//            return session.execute(stmt);
//        } catch (NoHostAvailableException ne) {
//            restartDatabaseConnection();
//            if (retryOnceOnNoHostAvailable) {
//                try {
//                    return session.execute(stmt);
//                } catch (NoHostAvailableException e) {
//                    throw new CassandraDatabaseException(ne);
//                }
//            } else {
//                throw new CassandraDatabaseException(ne);
//            }
//
//        } catch (com.datastax.driver.core.exceptions.InvalidQueryException iq) {
//            if (iq.getMessage() != null && iq.getMessage().contains("You may have used a PreparedStatement that was created with another Cluster instance")) {
//                // special case where a new connection has being created but the prepared statemtn was already registed in a previous cluster instance.
//                // but because in this method the key statemtn specs is not present, all prepared statements will be re-registered.
//                for (String key : statementSpecsMap.keySet()) {
//                    try {
//                        PreparedStatement statement = this.createStatement(key);
//                        statementsMap.put(key, statement);
//
//                    } catch (Exception e) {
//                        //ignore error while trying to register the preparedstatement
//                        throw new CassandraDatabaseException("Error registering prepared statement");
//                    }
//                }
//
//                // at the end the query is retried
//                return session.execute(stmt);
//            } else {
//                throw new CassandraDatabaseException(iq);
//            }
//
//        } catch (CassandraDatabaseException ge) {
//            throw ge;
//        }
//
//    }

    public ResultSet execute(String query) {
        return this.execute(query, false);
    }

    /**
     *
     * @param query
     * @param retryOnceOnNoHostAvailable
     * @return
     */
    public ResultSet execute(String query, boolean retryOnceOnNoHostAvailable) {
        try {
            if (session == null) {
                restartDatabaseConnection();
            }
            return session.execute(query);
        } catch (NoHostAvailableException ne) {
            restartDatabaseConnection();
            if (retryOnceOnNoHostAvailable) {
                try {
                    return session.execute(query);
                } catch (NoHostAvailableException e) {
                    throw new CassandraDatabaseException(ne);
                }
            } else {
                throw new CassandraDatabaseException(ne);
            }

        } catch (com.datastax.driver.core.exceptions.InvalidQueryException iq) {
            if (iq.getMessage() != null && iq.getMessage().contains("You may have used a PreparedStatement that was created with another Cluster instance")) {
                // special case where a new connection has being created but the prepared statemtn was already registed in a previous cluster instance.
                // but because in this method the key statemtn specs is not present, all prepared statements will be re-registered.
                for (String key : statementSpecsMap.keySet()) {
                    try {
                        PreparedStatement statement = this.createStatement(key);
                        statementsMap.put(key, statement);

                    } catch (Exception e) {
                        //ignore error while trying to register the preparedstatement
                    }
                }

                // at the end the query is retried
                return session.execute(query);
            } else {
                throw new CassandraDatabaseException(iq);
            }

        } catch (CassandraDatabaseException ge) {
            throw ge;
        }
    }

    /**
     *
     * @param stmt
     * @return
     */
//    public ResultSetFuture executeAsync(Statement stmt) {
//        return this.executeAsync(stmt, false);
//    }
    
    /**
     *
     * @param stmt
     * @return
     */
//    public ResultSetFuture executeAsync(String stmt) {
//        return this.executeAsync(stmt, false);
//    }

    /**
     *
     * @param stmt
     * @param retryOnceOnNoHostAvailable
     * @return
     */
//    public ResultSetFuture executeAsync(Statement stmt, boolean retryOnceOnNoHostAvailable) {
//        try {
//            if (session == null) {
//                restartDatabaseConnection();
//            }
//            return session.executeAsync(stmt);
//        } catch (NoHostAvailableException ne) {
//            restartDatabaseConnection();
//            if (retryOnceOnNoHostAvailable) {
//                try {
//                    return session.executeAsync(stmt);
//                } catch (NoHostAvailableException e) {
//                    throw new CassandraDatabaseException(ne);
//                }
//            } else {
//                throw new CassandraDatabaseException(ne);
//            }
//
//        } catch (com.datastax.driver.core.exceptions.InvalidQueryException iq) {
//            if (iq.getMessage() != null && iq.getMessage().contains("You may have used a PreparedStatement that was created with another Cluster instance")) {
//                // special case where a new connection has being created but the prepared statemtn was already registed in a previous cluster instance.
//                // but because in this method the key statemtn specs is not present, all prepared statements will be re-registered.
//                for (String key : statementSpecsMap.keySet()) {
//                    try {
//                        PreparedStatement statement = this.createStatement(key);
//                        statementsMap.put(key, statement);
//
//                    } catch (Exception e) {
//                        //ignore error while trying to register the preparedstatement
//                    }
//                }
//
//                // at the end the query is retried
//                return session.executeAsync(stmt);
//            } else {
//                throw new CassandraDatabaseException(iq);
//            }
//
//        } catch (CassandraDatabaseException ge) {
//            throw ge;
//        }
//    }
    
    /**
     *
     * @param stmt
     * @param retryOnceOnNoHostAvailable
     * @return
     */
//    public ResultSetFuture executeAsync(String stmt, boolean retryOnceOnNoHostAvailable) {
//        try {
//            if (session == null) {
//                restartDatabaseConnection();
//            }
//            return session.executeAsync(stmt);
//        } catch (NoHostAvailableException ne) {
//            restartDatabaseConnection();
//            if (retryOnceOnNoHostAvailable) {
//                try {
//                    return session.executeAsync(stmt);
//                } catch (NoHostAvailableException e) {
//                    throw new CassandraDatabaseException(ne);
//                }
//            } else {
//                throw new CassandraDatabaseException(ne);
//            }
//
//        } catch (com.datastax.driver.core.exceptions.InvalidQueryException iq) {
//            if (iq.getMessage() != null && iq.getMessage().contains("You may have used a PreparedStatement that was created with another Cluster instance")) {
//                // special case where a new connection has being created but the prepared statemtn was already registed in a previous cluster instance.
//                // but because in this method the key statemtn specs is not present, all prepared statements will be re-registered.
//                for (String key : statementSpecsMap.keySet()) {
//                    try {
//                        PreparedStatement statement = this.createStatement(key);
//                        statementsMap.put(key, statement);
//
//                    } catch (Exception e) {
//                        //ignore error while trying to register the preparedstatement
//                    }
//                }
//
//                // at the end the query is retried
//                return session.executeAsync(stmt);
//            } else {
//                throw new CassandraDatabaseException(iq);
//            }
//
//        } catch (CassandraDatabaseException ge) {
//            throw ge;
//        }
//    }

    /**
     * Closes the session and cluster internal objects. No exceptions are thrown
     * just a silent log.
     */
    public void shutdownConnection() {
        LOG.log(Level.WARNING, "Shutting down Cassandra connections!");

        this.closeSession();

        this.closeCluster();

        LOG.log(Level.WARNING, "Shutdown Cassandra connections!");

    }

    private void closeSession() {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error closing Cassandra Session {0}:", e);
        }
        session = null;
    }

    private void closeCluster() {
        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Error closing Cassandra Cluster connection: {0}", e);
        }
        cluster = null;
    }
}

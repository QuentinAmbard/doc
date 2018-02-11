import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core._
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnectorConf, LocalNodeFirstLoadBalancingPolicy, MultipleRetryPolicy}

/**
  * Created by quentin on 04/02/18.
  */
class CustomConnectionFactory  extends CassandraConnectionFactory {
  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      .withRetryPolicy(
        new MultipleRetryPolicy(conf.queryRetryCount))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
      .withLoadBalancingPolicy(
        new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
      .withCompression(conf.compression)
      .withPoolingOptions(new PoolingOptions()
        .setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
        .setMaxRequestsPerConnection(HostDistance.REMOTE, 32768)
        .setMaxConnectionsPerHost( HostDistance.LOCAL, 5)
        .setCoreConnectionsPerHost(HostDistance.LOCAL,  1)
        .setMaxConnectionsPerHost( HostDistance.REMOTE, 5)
        .setCoreConnectionsPerHost(HostDistance.REMOTE, 1))

      .withQueryOptions(
        new QueryOptions()
          .setRefreshNodeIntervalMillis(0)
          .setRefreshNodeListIntervalMillis(0)
          .setRefreshSchemaIntervalMillis(0))
    .build()

  }
}

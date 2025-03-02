package src.main.java.com.linkedin.venice.ratis;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;


public class PartitionAssignmentServer implements Closeable {
  private final RaftServer server;

  public static final String STORE_NAME = "storeName";

  public PartitionAssignmentServer(RaftPeer peer, File storageDir, String storeName, String groupName)
      throws IOException {
    // create a property object
    final RaftProperties properties = new RaftProperties();

    // set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    // set the port (different for each peer) in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    final PartitionAssignmentStateMachine partitionAssignmentStateMachine =
        new PartitionAssignmentStateMachine(properties.get(STORE_NAME));

    // build the Raft server
    this.server = RaftServer.newBuilder()
        // .setGroup(RaftGroup.emptygroup())
        .setProperties(properties)
        .setServerId(peer.getId())
        .setStateMachine(partitionAssignmentStateMachine)
        .build();

  }

  @Override
  public void close() throws IOException {

  }
}

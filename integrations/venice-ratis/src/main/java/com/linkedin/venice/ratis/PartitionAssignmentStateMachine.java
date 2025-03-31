package com.linkedin.venice.ratis;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.participant.protocol.GroupPartitionAssignment;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.JavaUtils;


public class PartitionAssignmentStateMachine extends BaseStateMachine {

  // TODO: Ratis is grpc based, so it's probably more protobuf savvy. But I'm a milk fed avro country programmer. So
  // for now we're using avro, but we should evaluate if protobuf here would be a more natural fit.
  private final RecordDeserializer<GroupPartitionAssignment> groupPartitionAssignmentDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.GROUP_PARTITION_ASSIGNMENT.getCurrentProtocolVersionSchema(),
          GroupPartitionAssignment.class);

  private final RecordSerializer<GroupPartitionAssignment> groupPartitionAssignmentSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(
          AvroProtocolDefinition.GROUP_PARTITION_ASSIGNMENT.getCurrentProtocolVersionSchema());

  private static final Logger LOGGER = LogManager.getLogger(PartitionAssignmentStateMachine.class);
  private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
  private final Map<String, List<Integer>> partitionAssignments = new ConcurrentHashMap<String, List<Integer>>();

  private final String storeName;

  public PartitionAssignmentStateMachine(String storeName) {
    this.storeName = storeName;
  }

  /**
   * Apply the transaction to the state machine. The transaction is a command that updates the partition assignment
   *
   * @param trx the transaction context
   * @return the message containing the updated counter value
   */
  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    final RaftProtos.LogEntryProto entry = trx.getLogEntry();
    PartitioningMessage partitioningMessage =
        new PartitioningMessage(entry.getStateMachineLogEntry().getLogData().toByteArray());

    // Sanity check, we should only get these for
    if (!storeName.equals(partitioningMessage.getGroupPartitionAssignment().getStoreName())) {
      return JavaUtils.completeExceptionally(
          new IllegalArgumentException(
              String.format(
                  "Store name mismatch. Expected: {}, Actual: {}",
                  storeName,
                  partitioningMessage.getGroupPartitionAssignment().getStoreName())));
    }

    // apply the assignment and update term-index
    final TermIndex termIndex = TermIndex.valueOf(entry);
    updateState(termIndex, partitioningMessage.getGroupPartitionAssignment().getAssignment());

    // if leader, log the incremented value and the term-index
    if (trx.getServerRole() == RaftProtos.RaftPeerRole.LEADER) {
      LOGGER.info(String.format("{}: Increment to {}", termIndex, partitionAssignments));
    }

    // return the new value of the assignment to the client (TODO: consider serializing instead of sending a string)
    return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(partitionAssignments)));
  }

  /**
   * Process the only query we got, which gets the assignment.
   *
   * @param request the GET request
   * @return a {@link Message} containing the current counter value as a {@link String}.
   */
  @Override
  public CompletableFuture<Message> query(Message request) {
    // We only support one kind of query, so we don't bother checking into the request.
    // TODO: Nice for humans, but not nice for deserializing. We can send back the avro serialized version, but won't
    // for now
    return CompletableFuture.completedFuture(Message.valueOf(String.valueOf(partitionAssignments)));
  }

  /** The state of the {@link CounterStateMachine}. */
  static class PartitionAssignmentState {
    private final TermIndex applied;
    private final Map<String, List<Integer>> partitionAssignments;
    private final long timestampInSeconds;

    PartitionAssignmentState(TermIndex applied, Map<String, List<Integer>> partitionAssignments) {
      this.applied = applied;
      this.partitionAssignments = partitionAssignments;
      this.timestampInSeconds = System.currentTimeMillis() / 1000;
    }

    TermIndex getApplied() {
      return applied;
    }

    Map<String, List<Integer>> getPartitionAssignments() {
      return partitionAssignments;
    }

    long getTimestampInSeconds() {
      return timestampInSeconds;
    }
  }

  /**
   * Initialize the state machine storage and then load the state. The initialize method is called at most once when
   * the server is starting up.
   *
   * @param server  the server running this state machine
   * @param groupId the id of the {@link org.apache.ratis.protocol.RaftGroup}
   * @param raftStorage the storage of the server
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
    super.initialize(server, groupId, raftStorage);
    storage.init(raftStorage);
    reinitialize();
  }

  /**
   * Loads the state from local storage.  reinitialize is called when either:
   *
   * 1. the server is resumed from the PAUSE state
   * 2. a new snapshot is installed from the leader or from an external source.
   *
   * @throws IOException if it fails to load the state.
   */
  @Override
  public void reinitialize() throws IOException {
    load(storage.getLatestSnapshot());
  }

  /** @return the current partition assignment state. */
  private synchronized PartitionAssignmentState getState() {
    return new PartitionAssignmentState(getLastAppliedTermIndex(), partitionAssignments);
  }

  private synchronized void updateState(TermIndex applied, Map<String, List<Integer>> partitionAssignments) {
    this.partitionAssignments.clear();
    this.partitionAssignments.putAll(partitionAssignments);
    updateLastAppliedTermIndex(applied);
  }

  /**
   * Store the current state as a snapshot file in the {@link #storage}.
   *
   * @return the index of the snapshot
   */
  @Override
  public long takeSnapshot() {
    // get the current state
    final PartitionAssignmentState state = getState();
    final long index = state.getApplied().getIndex();

    // create a file for the snapshot
    final File snapshotFile = storage.getSnapshotFile(state.getApplied().getTerm(), index);

    // write the assignment into the snapshot file
    try (ObjectOutputStream out =
        new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(snapshotFile.toPath())))) {

      GroupPartitionAssignment groupPartitionAssignment = new GroupPartitionAssignment();
      groupPartitionAssignment.setAssignment(state.getPartitionAssignments());
      groupPartitionAssignment.setGroupName(getGroupId().toString());
      // TODO: pass the store name to the server
      groupPartitionAssignment.setStoreName(storeName);
      groupPartitionAssignment.setTimestampInSeconds(state.getTimestampInSeconds());
      out.write(groupPartitionAssignmentSerializer.serialize(groupPartitionAssignment));
    } catch (IOException ioe) {
      LOGGER.warn("Failed to write snapshot file \"" + snapshotFile + "\", last applied index=" + state.getApplied());
    }

    // return the index of the stored snapshot (which is the last applied one)
    return index;
  }

  /**
   * Load the state of the state machine from the {@link #storage}.
   *
   * @param snapshot the information of the snapshot being loaded
   * @return the index of the snapshot or -1 if snapshot is invalid
   * @throws IOException if it failed to read from storage
   */
  private long load(SingleFileSnapshotInfo snapshot) throws IOException {

    // TODO, technically, we don't need to rely on the local state. We could probably just go blank on start and let the
    // load
    // balancer do it's work. However, loading from local state might be useful in some cases where we already had local
    // davinci state.

    // check null
    if (snapshot == null) {
      LOGGER.warn("The snapshot info is null.");
      return RaftLog.INVALID_LOG_INDEX;
    }
    // check if the snapshot file exists.
    final Path snapshotPath = snapshot.getFile().getPath();
    if (!Files.exists(snapshotPath)) {
      LOGGER.warn("The snapshot file {} does not exist for snapshot {}", snapshotPath, snapshot);
      return RaftLog.INVALID_LOG_INDEX;
    }

    // read the TermIndex from the snapshot file name
    final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotPath.toFile());

    final GroupPartitionAssignment assignment;
    File file = snapshotPath.toFile();
    byte[] bytes = new byte[(int) file.length()];
    try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(snapshotPath)))) {
      in.readFully(bytes);
      assignment = groupPartitionAssignmentDeserializer.deserialize(bytes);
    }

    // Sanity check, this persisted state should be for the same store
    if (!storeName.equals(assignment.getStoreName())) {
      throw new VeniceException(
          String.format("Store name mismatch. Expected: {}, Actual: {}", storeName, assignment.getStoreName()));
    }

    // update state
    updateState(last, assignment.getAssignment());

    return last.getIndex();
  }
}

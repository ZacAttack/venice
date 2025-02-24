import org.apache.ratis.protocol.Message;


public enum AutoPartitioningAssignmentCommands {
  /** Post a new partition assignment to participating peers*/
  REBALANCE_PARTITIONS,
  /** Get the partition assignments. */
  GET_PARTITION_ASSIGNMENTS,;

  private final Message message = Message.valueOf(name());

  public Message getMessage() {
    return message;
  }

  /** Does the given command string match this command? */
  public boolean matches(String command) {
    return name().equalsIgnoreCase(command);
  }
}

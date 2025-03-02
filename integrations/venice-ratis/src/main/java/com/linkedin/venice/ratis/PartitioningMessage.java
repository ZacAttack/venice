package src.main.java.com.linkedin.venice.ratis;

import com.linkedin.venice.participant.protocol.GroupPartitionAssignment;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;


public class PartitioningMessage implements Message {

  // TODO: Ratis is grpc based, so it's probably more protobuf savvy. But I'm a milk fed avro country programmer. So
  // for now we're using avro, but we should evaluate if protobuf here would be a more natural fit.
  // also, I want to make these static, but I seem to recall these aren't actually threadsafe. So we won't do that.
  private final RecordDeserializer<GroupPartitionAssignment> groupPartitionAssignmentDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.GROUP_PARTITION_ASSIGNMENT.getCurrentProtocolVersionSchema(),
          GroupPartitionAssignment.class);

  private final RecordSerializer<GroupPartitionAssignment> groupPartitionAssignmentSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(
          AvroProtocolDefinition.GROUP_PARTITION_ASSIGNMENT.getCurrentProtocolVersionSchema());

  private final GroupPartitionAssignment groupPartitionAssignment;

  public PartitioningMessage(GroupPartitionAssignment groupPartitionAssignment) {
    this.groupPartitionAssignment = groupPartitionAssignment;
  }

  public PartitioningMessage(byte[] bytes) {
    groupPartitionAssignment = groupPartitionAssignmentDeserializer.deserialize(bytes);
  }

  public GroupPartitionAssignment getGroupPartitionAssignment() {
    return groupPartitionAssignment;
  }

  @Override
  public ByteString getContent() {
    return ByteString.copyFrom(groupPartitionAssignmentSerializer.serialize(groupPartitionAssignment));
  }
}

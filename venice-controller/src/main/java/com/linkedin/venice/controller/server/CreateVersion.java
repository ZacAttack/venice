package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import spark.Route;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


/**
 * This class will add a new version to the given store.
 */
public class CreateVersion {
  private static final Logger logger = Logger.getLogger(CreateVersion.class);

  public static Route createVersionRoute(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, CREATE_VERSION.getParams(), admin);
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);

        // TODO we should verify the data size at first. If it exceeds the quota, controller should reject this request.
        // TODO And also we should use quota to calculate partition count to avoid this case that data size of first
        // push is very small but grow dramatically because quota of this store is very large.
        // Store size in Bytes
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        int partitionNum = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
        int replicaFactor = admin.getReplicationFactor(clusterName, storeName);
        Version version = admin.incrementVersion(clusterName, storeName, partitionNum, replicaFactor);
        // The actual partition number could be different from the one calculated here,
        // since Venice is not using dynamic partition number across different versions.
        responseObject.setPartitions(admin.getStore(clusterName, storeName).getPartitionCount());
        responseObject.setReplicas(replicaFactor);
        responseObject.setVersion(version.getNumber());
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers());
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }

  /**
   * Instead of asking Venice to create a version, pushes should ask venice which topic to write into.
   * The logic below includes the ability to respond with an existing topic for the same push, allowing requests
   * to be idempotent
   *
   * @param admin
   * @return
   */
  public static Route requestTopicForPushing(Admin admin) {
    return (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        AdminSparkServer.validateParams(request, REQUEST_TOPIC.getParams(), admin);

        //Query params
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        String pushJobId = request.queryParams(PUSH_JOB_ID);
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        String pushTypeString = request.queryParams(PUSH_TYPE);
        PushType pushType;
        try {
          pushType = PushType.valueOf(pushTypeString);
        } catch (RuntimeException e){
          throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, pushTypeString + " is an invalid " + PUSH_TYPE, e);
        }

        //looked up params
        int replicationFactor = admin.getReplicationFactor(clusterName, storeName);
        int partitionCount = admin.calculateNumberOfPartitions(clusterName, storeName, storeSize);

        responseObject.setCluster(clusterName);
        responseObject.setName(storeName);
        responseObject.setPartitions(partitionCount);
        responseObject.setReplicas(replicationFactor);
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers());

        switch(pushType) {
          case BATCH:
            Version version = admin.incrementVersionIdempotent(clusterName, storeName, pushJobId, partitionCount, replicationFactor);
            responseObject.setVersion(version.getNumber());
            responseObject.setKafkaTopic(Version.composeKafkaTopic(storeName, version.getNumber()));
            break;
          case STREAM:
            //Verify that the real-time buffer topic exists
            //create the buffer topic if it doesn't exist
            //return the topic name for the real time buffer topic
            if (1==1) {
              throw new VeniceHttpException(HttpStatus.SC_NOT_IMPLEMENTED,
                  "Venice does not yet support streaming records into a hybrid store");
            }
            break;
          default:
            throw new VeniceException(pushTypeString + " is an unrecognized " + PUSH_TYPE);
        }
      } catch (Throwable e) {
        responseObject.setError(e.getMessage());
        AdminSparkServer.handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return AdminSparkServer.mapper.writeValueAsString(responseObject);
    };
  }
}

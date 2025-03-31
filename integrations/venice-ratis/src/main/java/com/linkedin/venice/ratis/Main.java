package src.main.java.com.linkedin.venice.ratis;

public class Main {
  public static void main(String[] args) {

    // We gotta start writing this shit down cause we keep forgetting the plan.
    // Let's look at the possible ways to do it.

    // A raft group is only able to move if it's got an up and running quorum

    // Use helix for all the nodes:
    // -This isn't gonna work. We don't want our common zk to get chewed up client deployments. So fuck em!

    // So with this in mind, we need something that allows for arbitrary consumer group size.

    // In Kafka, a broker is in charge of setting up the consumer group participation and what gets committed to the
    // consumer
    // group is decided amongst broker nodes using RAFT and committed to a metadata topic in the cluster.

    // In Venice, we could use helix to select a special host to decide the partition assignment. It accepts joins and
    // registers
    // and writes into a system store the assignment. (we could even skip RT if we wanted?). Also processes heartbeats
    // of members
    // and publishes new version
    // -Cons: This is nice, but it has a few gotchas. One obvious one is that there is a natural bottleneck on the
    // server
    // which is in charge of accepting and broadcasting changes. A more subtle gotcha is that there isn't a way
    // to have direct feedback between different states of replicas. This isn't super necessary depending on the
    // guarantees
    // we want to provide. If we don't have to guarantee high availability of partitions, then this will work. But
    // if we wanted something that gave a similar set of guarantees as what would be required by an online service, we'd
    // either need a different or at least tweaked approach.

    // Raft group amongst clients in a group. Raft comes with a couple of nice things. First, members heartbeat each
    // other,
    // and there's always a leader at a given time. This is similar to the approach where we have a venice server in
    // charge
    // of assignment. Additionally, participants in the group are able to ack back and react to changes in state. The
    // advantage
    // of this is that we can give better availability guarantees for partition assignment (nodes can ack back if
    // they're able to
    // accept the partition assignment).
    // -Cons: We'll probably need something which is able to trim and grow the raft group list. This is actually maybe
    // the most
    // painful part. If nodes are truly coming and going like crazy, this will be hard.

    System.out.println("Hello world!");
  }
}

# Overview
Maintaining the service availability, reassign the Shard to
service node, remain load balancing between the service nodes,
R/W Request routing。


# Deploy Mode
## Single Gateway Mode
 Deploying service in a single node, with no guarantee of fault toleration.
 When Node failed, no load balancing happened, client will fallback into a naive mode.
  
 
## Strong Cluster Mode
   Using Raft to stay in consistency&high availability.

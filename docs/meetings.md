---
id: meetings
title: "Meetings"
---

This section contains the notes collected by ZIO Keeper's team obtained during
their design sessions. To emphasize recent activities, notes will be sorted in
a descending order.

Note that these should be used as an accompanying material of the design docs
presented in `Overview` when additional clarification of design and implementation
decision is needed.

# Meeting 2019-11-22

**Participants:**

- Maxim Schuwalow (@mschuwalow)
- Peter Rowlands
- Dejan Mijic (@mijicd)

**Agenda:**

- Discuss Peter's design proposal.

## Notes

We discussed a few things yesterday, but the high level idea we agree on is still "95% CRDTs, 5% strongly consistent mutable data".
We want to stay with our seperate pluggable layers for membership/crdts and started compiling requirements and possible designs.

### Membership

Here we agreed on the following requirements:
1. We need to suppport a (possibly hierarchical) topic model for membership and broadcast.
2. We need an algorithm that can operate in different environments depending on tunable parameters. The main axis we want to consider
   is reliability of nodes.
3. We need to support a broadcast strategy that reaches all nodes in the cluster very quickly (for example through high fanout values) and optimally
   supporting acknowledgement. This will be necessary for the consensus protocols.
4. We should support some sort of immutable facts that can be spread across the cluster such as certificate revocation.
5. For all of \{ CRDTs, strongly consistent mutable data, immutable facts \} we want to support a time to live.
6. We want to have encryption and cryptographic signing built in from the beginning. Peter started posting about that.

I believe this gives us enough requirements to begin creating a short list of possible protocol that we could use for this.
As a fist step it is probably a good idea to create a long list of all well known ones.

### CRDTs

We decided to go with operation-based CRDTs by default, but not overly specialize any parts of the code for them.
We will need to support some sort of GC for them, Peter drafted a system of how this might look like

1. We have periodic snapshots running that are signed and agreed on by the cluster using a consensus mechanism.
   These snapshots will compact the metadata of a CRDT, but not change their current value. One example could be
   removal of tombstones.
2. We defined two periods related to GC:
    1. T1 the amount of time for which a snapshot is considered 'fresh'
    2. T2 the amount of time for which a snapshot is retained, this should be significantly larger than the longest expected continous
       downtime of a node.
3. Each update to an CRDT is signed with the current snapshot. If a node sends an update one of three situations can occur:
    1. The age of the snapshot is < T1 -> It is accepted without any further measures
    2. The age of the snapshot is > T1, < T2 -> Member is required to update to a newer snapshot
    3. The age of the snapshot is > T2 -> The member is forced to discard its own state a do a full sync with another member.
       This should only happpen if a node has been down for a considerable amount of time.

### Consensus

We will require some sort of consensus mechanism for both GC and any form of strongly consistent key-value store.
Protocols like raft tend to not scale well with the number of nodes in a cluster, proposition is therefore the following:

1. We have one consensus protocol running that scales well with number of nodes and elects a smaller group.
2. This smaller group runs something like RAFT and becomes the source of truth for consensus in the cluster.
3. If one of the nodes in the inner circle goes down it will be immediately replaced by a successor.
4. Possibly, we have an active-passive setup with a group that will immediately take over leadership if the first group fails.

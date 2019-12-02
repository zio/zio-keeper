# ZIO keeper

[![CircleCI][badge-ci]][link-ci]

ZIO keeper is a purely-functional, type-safe library for managing distributed state in a strong
eventually consistent manner, backed by [Conflict-free Replicated Data Types][link-crdts-wiki] and
[ZIO][link-zio].

## Goals

- Dynamic cluster formation and management via Gossip protocol.
- Support subscribing to cluster events (e.g. node joining or leaving).
- Access and store values using their key and type. Stored data will have one of the following types: `bool`, `long`, `string`, `set`, `map`.
- Support subscribing to value updates (e.g. modified, removed).

## Background readings

The following section contains the list of papers and videos that explain the basic building blocks
of the library. For an introduction to distributed systems in general, make sure to check out
[Distributed Systems for Fun and Profit][link-dsffap].

### Conflict-free Replicated Data Types

Foundations:

- [Conflict-free Replicated Data Types][link-crdts-paper]
- [CRDTs illustrated][link-crdts-illustrated]
- [Strong Eventual Consistency and Conflict-free Replicated Data Types][link-shapiro]

Production experiences:

- [CRDTs in Practice][link-crdts-in-practice]
- [CRDTs in Production][link-crdts-in-production]

For an in-depth understanding of the topic, make sure to check out the materials listed in
[this blog post][link-cmeik-blog].

### Gossip

Foundations:

- [Epidemic Algorithms for Replicated Database Maintenance][link-gossip-intro]
- [Understanding Gossip Protocols][link-gossip-overview]
- [The Promise, and Limitations, of Gossip Protocols][link-gossip-birman]

Production experiences:

- [Lifeguard: Local Health Awareness for More Accurate Failure Detection][link-lifeguard]

[badge-ci]: https://circleci.com/gh/zio/zio-keeper/tree/master.svg?style=svg
[link-crdts-wiki]: https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type
[link-zio]: https://zio.dev
[link-ci]: https://circleci.com/gh/zio/zio-keeper/tree/master
[link-dsffap]: http://book.mixu.net/distsys/single-page.html
[link-crdts-paper]: https://hal.inria.fr/hal-00932836/file/CRDTs_SSS-2011.pdf
[link-crdts-in-practice]: https://www.youtube.com/watch?v=xxjHC3yLDqw
[link-crdts-in-production]: https://www.youtube.com/watch?v=f03FWiIfXoQ
[link-crdts-illustrated]: https://www.youtube.com/watch?v=9xFfOhasiOE
[link-shapiro]: https://www.youtube.com/watch?v=ebWVLVhiaiY
[link-cmeik-blog]: http://christophermeiklejohn.com/crdt/2014/07/22/readings-in-crdts.html
[link-gossip-intro]:http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
[link-gossip-overview]: https://www.youtube.com/watch?v=QQ2n1UX3Qwg
[link-gossip-birman]: http://www.cs.cornell.edu/Projects/Quicksilver/public_pdfs/2007PromiseAndLimitations.pdf
[link-lifeguard]: https://arxiv.org/pdf/1707.00788.pdf

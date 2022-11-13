---
id: references
title: "References"
---

This section references books, journal articles, blog posts, and videos
providing an in-depth explanation of building blocks and concepts used inside
the library. They are categorized by topic, and can be covered without any
particular order.

## Conflict-free Replicated Data Types

Foundations:

- [Conflict-free Replicated Data Types][link-crdts-paper]
- [CRDTs illustrated][link-crdts-illustrated]
- [Strong Eventual Consistency and Conflict-free Replicated Data Types][link-shapiro]

Production experiences:

- [CRDTs in Practice][link-crdts-in-practice]
- [CRDTs in Production][link-crdts-in-production]

For an in-depth understanding of the topic, make sure to check out the materials listed in
[this blog post][link-cmeik-blog].

[link-crdts-paper]: https://hal.inria.fr/hal-00932836/file/CRDTs_SSS-2011.pdf
[link-crdts-in-practice]: https://www.youtube.com/watch?v=xxjHC3yLDqw
[link-crdts-in-production]: https://www.youtube.com/watch?v=f03FWiIfXoQ
[link-crdts-illustrated]: https://www.youtube.com/watch?v=9xFfOhasiOE
[link-shapiro]: https://www.youtube.com/watch?v=ebWVLVhiaiY
[link-cmeik-blog]: http://christophermeiklejohn.com/crdt/2014/07/22/readings-in-crdts.html

## Membership Protocols

### Gossip protocols foundations:

- [Epidemic Algorithms for Replicated Database Maintenance][1]
- [Understanding Gossip Protocols][2]
- [The Promise, and Limitations, of Gossip Protocols][3]

### SWIM protocol:

- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol (paper)][4]
- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol (video)][5]
- [Serf documentation][6]

### HyParView protocol:

- [HyParView: a membership protocol for reliable gossip-based broadcast][7]

[1]: http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/CSL-89-1_Epidemic_Algorithms_for_Replicated_Database_Maintenance.pdf
[2]: https://www.youtube.com/watch?v=QQ2n1UX3Qwg
[3]: http://www.cs.cornell.edu/Projects/Quicksilver/public_pdfs/2007PromiseAndLimitations.pdf
[4]: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
[5]: https://www.youtube.com/watch?v=bkmbWsDz8LM
[6]: https://www.serf.io/docs/internals/gossip.html
[7]: http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf

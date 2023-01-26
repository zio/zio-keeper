---
id: index
title: "Introduction to ZIO Keeper"
sidebar_label: "Introduction"
---

ZIO Keeper is a purely-functional, type-safe library for building distributed systems.

@PROJECT_BADGES@

## Introduction

It provides numerous primitives for tackling the common problems in distributed computing (e.g. leader election, cluster forming etc.).

Under the hood, the library is backed by [ZIO][Link-ZIO] and [ZIO NIO][Link-NIO], profiting from their performant, type and resource-safe APIs:
- **Composable**. Design complex systems by composing the available building blocks.
- **Resilient**. Build apps with automated failure recovery.
- **Secure**. Benefit from security guarantees built into the library core.

From the high-level perspective, the library can be separated into the following
"modules":
- transport
- membership
- consensus

## Installation

In order to use this library, we need to add the following line in our `build.sbt` file:

```scala
libraryDependencies += "dev.zio" %% "zio-keeper" % "<version>"

resolvers += Resolver.sonatypeRepo("snapshots")
```

[Link-ZIO]: https://zio.dev
[Link-NIO]: https://zio.github.io/zio-nio/


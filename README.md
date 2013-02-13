#Zyni

zyni - Message based application framework on ZeroMQ


## Features
* Automatic discovery and State sharing
 * Pluggable discovery system
 * Gossip based discovery
* Peer-to-Peer massively parallel processing
 * A topology based (DAG) processing
 * Utilize a load statics
 * Guaranting processing with an Acker
 * Streaming processing with a cursor

## Planned Features
* Centralized logging
* Consistence hasing
* File sharing
* Replica processing

## Contribution Process

This project uses the [C4 process](http://rfc.zeromq.org/spec:16) for all code changes. "Everyone,
without distinction or discrimination, SHALL have an equal right to become a Contributor under the
terms of this contract."

## Usage

Add it to your Maven project's `pom.xml`:

    <!-- for the latest SNAPSHOT -->
    <dependency>
      <groupId>org.zyni</groupId>
      <artifactId>zyni</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>

Also please refer the [Wiki](https://github.com/infiniloop/zyni/wiki)

#Jini

jini - Message based application framework on JeroMQ


## Features
* Automatic discovery and State sharing
 * Gossip based discovery
 * Fixed set discovery
* Peer-to-Peer massively parallel processing
 * A topology based processing
 * Utilize a load statics
 * Guaranting processing with an Ack
 * Streaming processing with a cursor

## Contribution Process

This project uses the [C4 process](http://rfc.zeromq.org/spec:16) for all code changes. "Everyone,
without distinction or discrimination, SHALL have an equal right to become a Contributor under the
terms of this contract."

## Usage

Add it to your Maven project's `pom.xml`:

    <!-- for the latest SNAPSHOT -->
    <dependency>
      <groupId>org.jeromq</groupId>
      <artifactId>jini</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>

Also please refer the [Wiki](https://github.com/infiniloop/jini/wiki)

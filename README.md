# k2

Toy kafka implementation

```sh
go get github.com/schmichael/k2
k2

# Then send messages to it
```

https://github.com/urbanairship/pykafkap#usage is a handy way to send messages
and test the thing. Kafka's shell scripts drive me crazy, and I'd prefer not to
have it or any scala on my machine at all.

```sh
k2 -h # for options
```

## Why?

Kafka is fantastic, and I have no intention of trying to write a production
ready replacement. Here's why I'm wasting my time on k2 at all:

1. For fun. This is the only real reason. Well maybe to learn a thing or two as
   well.
1. Hopefully it's fixed now but compiling kafka in an Ubuntu encrypted homedir
   exceeded path length limits and broke. That drove me crazy.
1. Depending on what version of Kafka you want/need, you may be stuck on a
   version of the JDK you otherwise wouldn't use because specific versions of
   Kafka are tied to specific versions of Scala which are tied to specific
   versions of Java.
1. Eventually re-using some of this code for kafka related tooling might be
   nice since (as stated above) the bundled shell scripts drive me crazy.

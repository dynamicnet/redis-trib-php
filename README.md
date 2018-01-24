# redis-trib-php
A pure PHP implementation of redis-trib.rb. A tool to manage Redis clusters.

## Why ?
The original redis-trib.rb is a Ruby tool, with some dependencies that require a recent Ruby.

Sometimes, you don't have a Ruby envrironnement and don't want to setup one in order to manage a Redis cluster.

redis-trib.php aims to do the same work as the original tool, but with PHP and no dependency.

The interface is the same as the original tool shipped with Redis. So any tutorial showing examples with the Ruby tool may work with the PHP tool.


## Install manually

Pick the latest release at https://github.com/dynamicnet/redis-trib-php/releases

```console
~# wget https://github.com/dynamicnet/redis-trib-php/releases/XXXXXX
~# chmod +x ./redis-trib.php

~# ./redis-trib.php help
```

## Install via Composer


## Commands

### create
Create a cluster using a list of node. All nodes must be empty. If you have non empty nodes, you can use --force-flush to flush the redis DB, be carful with this option.

The slot allocation is automatic. The memory allocated to each node is used to balance the cluster. The more a node have memory, the more slot we allocate to it.

Create a cluster with 3 nodes
```console
~# redis-trib.php create 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381
```

### check
Performs a sanity check of the cluster.

```console
~# redis-trib.php create 127.0.0.1:6379
```


## DISCLAIMER
 :warning: **NOT PRODUCTION READY**

 :warning: Not all commands or options of redis-trib.rb are available at the moment.
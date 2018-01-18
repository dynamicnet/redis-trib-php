# redis-trib-php
A pure PHP implementation of redis-trib.rb. A tool to manage Redis cluster.

## Why ?
The original redis-trib.rb is a Ruby tool, with some dependencies that require a recent Ruby.

Sometimes, you don't have a Ruby envrironnement and don't want to setup one in order to manage a Redis cluster.

redis-trib.php aims to do the same work as the original tool, but with PHP and no dependency.

The interface is the same as the original tool shipped with Redis. So any tutorial showing examples with the Ruby tool may work with the PHP tool.


## DISCLAIMER
 :warning: **NOT PRODUCTION READY**
 :warning: Not all commands or options of redis-trib.rb are available at the moment.
Concentrator (Go)
=============

The Concentrator is used to forward network requests onto multiple backend servers. The primary use is for repeating statsd/graphite packets to multiple backend stats servers simultaneously.

A simple yaml configuration file is used to define which interfaces the server utilizes and which backend servers requests should be forwarded to.

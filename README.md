CandleProxy
===================


It's a POC of Akka IO TCP Client/Server Application
Before run you need to start ticker upstream server ( see /main/python/upstream.py )
By default  upstream.py uses 5555 port on localhost, and CandleProxy binds 5556
To run use `sbt run`
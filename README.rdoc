= datastax_rails
==============

A Ruby-on-Rails interface to Datastax Enterprise (specifically DSE Search nodes).  Replaces the majority of ActiveRecord functionality.

This gem is based heavily on the excellent CassandraObject gem (https://github.com/data-axle/cassandra_object) as well as some work I initially did in the form of DatastaxRails (https://github.com/jasonmk/datastax_rails).  We made the decision to move away from Solandra and to Datastax Enterprise, thus datastax_rails was born.

Significant changes from DatastaxRails:

* Cassandra communication is now entirely CQL-based
* Solr communication is now handled directly via RSolr

=== Usage Note

Before using this gem, you should probably take a strong look at the type of problem you are trying to solve.  Cassandra is primarily designed as a solution to Big Data problems.  This gem is not.  You will notice that it still carries a lot of relational logic with it.  We are using DSE to solve a replication problem more so than a Big Data problem.  That's not to say that this gem won't work for Big Data problems, it just might not be ideal.  You've been warned...



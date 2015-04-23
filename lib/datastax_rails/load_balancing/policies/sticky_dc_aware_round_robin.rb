require 'cassandra/load_balancing'
module DatastaxRails
  module LoadBalancing
    module Policies
      # In Datastax Enterprise, there is a small amount of time between when a record is updated
      # on the local node and when that update is available in the Solr index on other nodes within
      # the datacenter. As a result, we want to stick to a particular node for a number of requests
      # before we roll on to the next node. This minimizes the chance of data not being where we
      # expect it to be.
      class StickyDcAwareRoundRobin < Cassandra::LoadBalancing::Policies::DCAwareRoundRobin
        def initialize(max_requests,
                       datacenter = nil,
                       max_remote_hosts_to_use = nil,
                       use_remote_hosts_for_local_consistency = false)
          @max_requests = max_requests
          super(datacenter, max_remote_hosts_to_use, use_remote_hosts_for_local_consistency)
          Thread.current[:position] = 0
          Thread.current[:sticky_count] = 0
        end

        def plan(_keyspace, _statement, options)
          local = @local

          if LOCAL_CONSISTENCIES.include?(options.consistency) && !@use_remote
            remote = EMPTY_ARRAY
          else
            remote = @remote
          end

          total = local.size + remote.size

          return EMPTY_PLAN if total == 0

          Thread.current[:position] ||= rand(total)
          Thread.current[:sticky_count] ||= 0

          if Thread.current[:sticky_count] >= @max_requests
            Thread.current[:position] = (Thread.current[:position] + 1) % total
            Thread.current[:sticky_count] = 0
          else
            Thread.current[:sticky_count] += 1
          end

          Plan.new(local, remote, Thread.current[:position])
        end
      end
    end
  end
end

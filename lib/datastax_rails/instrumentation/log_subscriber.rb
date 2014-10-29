module DatastaxRails
  module Instrumentation
    # A log subscriber to attach to Datastax related events
    #
    # @see https://github.com/rails/rails/blob/master/activerecord/lib/active_record/log_subscriber.rb
    #
    class LogSubscriber < ActiveSupport::LogSubscriber
      def initialize(*args)
        super
        @odd = false
      end

      def self.solr_runtime=(value)
        Thread.current['datastax_solr_runtime'] = value
      end

      def self.solr_runtime
        Thread.current['datastax_solr_runtime'] ||= 0
      end

      def self.cql_runtime=(value)
        Thread.current['datastax_cql_runtime'] = value
      end

      def self.cql_runtime
        Thread.current['datastax_cql_runtime'] ||= 0
      end

      def self.reset_solr_runtime
        rt, self.solr_runtime = solr_runtime, 0
        rt
      end

      def self.reset_cql_runtime
        rt, self.cql_runtime = cql_runtime, 0
        rt
      end

      # Intercept `solr.datastax_rails` events, and display them in the Rails log
      def solr(event)
        self.class.solr_runtime += event.duration
        return unless logger.debug? && DatastaxRails::Base.log_solr_queries

        payload = event.payload

        name    = "#{payload[:klass]} #{payload[:name]} (#{event.duration.round(1)}ms)"
        search  = payload[:search].inspect.gsub(/:(\w+)=>/, '\1: ')

        if odd?
          name = color(name, CYAN, true)
          search = color(search, nil, true)
        else
          name = color(name, MAGENTA, true)
        end

        debug "  #{name} #{search}"
      end

      # Intercept `cql.datastax_rails` events, and display them in the Rails log
      def cql(event)
        self.class.cql_runtime += event.duration
        return unless logger.debug? && DatastaxRails::Base.log_cql_queries

        payload = event.payload

        name    = "#{payload[:klass]} #{payload[:name]} (#{event.duration.round(1)}ms)"
        cql     = payload[:cql]
        binds   = nil

        unless (payload[:binds] || []).empty?
          binds = ' ' + payload[:binds].map { |col, v| [col, v.inspect.truncate(23)] }.inspect
        end

        if odd?
          name = color(name, CYAN, true)
          cql = color(cql, nil, true)
        else
          name = color(name, MAGENTA, true)
        end

        debug "  #{name} #{cql}#{binds}"
      end

      def odd?
        @odd = !@odd
      end
    end
  end
end

DatastaxRails::Instrumentation::LogSubscriber.attach_to :datastax_rails

require 'active_support/core_ext/module/attr_internal'

module DatastaxRails
  module Instrumentation
    # Hooks into ActionController to display Solr and CQL runtime
    #
    # @see https://github.com/rails/rails/blob/master/activerecord/lib/active_record/railties/controller_runtime.rb
    #
    module ControllerRuntime
      extend ActiveSupport::Concern

      protected

      attr_internal :solr_runtime
      attr_internal :cql_runtime

      def process_action(action, *args)
        DatastaxRails::Instrumentation::LogSubscriber.reset_solr_runtime
        DatastaxRails::Instrumentation::LogSubscriber.reset_cql_runtime
        super
      end

      def cleanup_view_runtime
        solr_rt_before_render = DatastaxRails::Instrumentation::LogSubscriber.reset_solr_runtime
        cql_rt_before_render = DatastaxRails::Instrumentation::LogSubscriber.reset_cql_runtime
        self.solr_runtime = (solr_runtime || 0) + solr_rt_before_render
        self.cql_runtime = (cql_runtime || 0) + cql_rt_before_render
        runtime = super
        solr_rt_after_render = DatastaxRails::Instrumentation::LogSubscriber.reset_solr_runtime
        cql_rt_after_render = DatastaxRails::Instrumentation::LogSubscriber.reset_cql_runtime
        self.solr_runtime += solr_rt_after_render
        self.cql_runtime += cql_rt_after_render
        runtime - solr_rt_after_render - cql_rt_after_render
      end

      def append_info_to_payload(payload)
        super
        payload[:solr_runtime] = (solr_runtime || 0) + DatastaxRails::Instrumentation::LogSubscriber.reset_solr_runtime
        payload[:cql_runtime] = (cql_runtime || 0) + DatastaxRails::Instrumentation::LogSubscriber.reset_cql_runtime
      end

      module ClassMethods
        def log_process_action(payload)
          messages, solr_runtime, cql_runtime = super, payload[:solr_runtime], payload[:cql_runtime]
          messages << ('Solr: %.1fms' % solr_runtime.to_f) if solr_runtime
          messages << ('CQL: %.1fms' % cql_runtime.to_f) if cql_runtime
          messages
        end
      end
    end
  end
end

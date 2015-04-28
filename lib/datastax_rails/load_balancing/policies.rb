module DatastaxRails
  module LoadBalancing
    module Policies #:nodoc:
      extend ActiveSupport::Autoload

      autoload :StickyDcAwareRoundRobin
    end
  end
end

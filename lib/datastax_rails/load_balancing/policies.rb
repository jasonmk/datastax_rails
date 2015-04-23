module DatastaxRails
  module LoadBalancing
    module Policies
      extend ActiveSupport::Autoload

      autoload :StickyDcAwareRoundRobin
    end
  end
end

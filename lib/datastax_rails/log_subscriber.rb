module DatastaxRails
  class LogSubscriber < ActiveSupport::LogSubscriber
    def multi_get(event)
      name = '%s multi_get (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  (#{event.payload[:keys].size}) #{event.payload[:keys].join(" ")}"
    end

    def remove(event)
      name = '%s remove (%.1fms)' % [event.payload[:column_family], event.duration]

      message = "  #{name}  #{event.payload[:key]}"
      message << " #{Array(event.payload[:attributes]).inspect}" if event.payload[:attributes]

      debug message
    end

    def truncate(event)
      name = '%s truncate (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  #{event.payload[:column_family]}"
    end

    def insert(event)
      name = '%s insert (%.1fms)' % [event.payload[:column_family], event.duration]

      debug "  #{name}  #{event.payload[:key]} #{event.payload[:attributes].inspect}"
    end

    def get_range(event)
      name = '%s get_range (%.1fms)' % [event.payload[:column_family], event.duration]
      
      debug "  #{name}  (#{event.payload[:count]}) '#{event.payload[:start]}' => '#{event.payload[:finish]}'"
    end
  end
end
DatastaxRails::LogSubscriber.attach_to :datastax_rails

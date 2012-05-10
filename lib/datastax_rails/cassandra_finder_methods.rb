module DatastaxRails
  module CassandraFinderMethods
    # extend ActiveSupport::Concern
#     
    # module ClassMethods
      # def find(*keys)
        # keys = Array(keys)
        # key_strings = keys.collect{|k| k.try :to_s}
# 
        # if key_string.blank?
          # raise DatastaxRails::RecordNotFound, "Couldn't find #{self.name} with key #{key.inspect}"
        # elsif attributes = connection.get(column_family, key_string, {:count => 500}).presence
          # instantiate(key_string, attributes)
        # else
          # raise DatastaxRails::RecordNotFound
        # end
      # end
# 
      # def find_by_id(keys)
        # find(keys)
      # rescue DatastaxRails::RecordNotFound
        # nil
      # end
# 
      # def all(options = {})
        # limit = options[:limit] || 100
        # results = ActiveSupport::Notifications.instrument("get_range.datastax_rails", :column_family => column_family, :key_count => limit) do
          # connection.get_range(column_family, :key_count => limit, :consistency => thrift_read_consistency)
        # end
# 
        # results.map do |k, v|
          # v.empty? ? nil : instantiate(k, v)
        # end.compact
      # end
# 
      # def first(options = {})
        # all(options.merge(:limit => 1)).first
      # end
# 
      # def find_with_ids(*ids)
        # ids = ids.flatten
        # return ids if ids.empty?
# 
        # ids = ids.compact.map(&:to_s).uniq
# 
        # multi_get(ids).values.compact
      # end
# 
      # def count
        # connection.count_range(column_family)
      # end
# 
      # def multi_get(keys, options={})
        # attribute_results = ActiveSupport::Notifications.instrument("multi_get.datastax_rails", :column_family => column_family, :keys => keys) do
          # connection.multi_get(column_family, keys.map(&:to_s), :consistency => thrift_read_consistency)
        # end
# 
        # Hash[attribute_results.map do |key, attributes|
          # [parse_key(key), attributes.present? ? instantiate(key, attributes) : nil]
        # end]
      # end
#       
      # def multi_find(keys)
        # keys = Array(keys)
        # key_strings = keys.collect {|k| k.try :to_s}.compact
# 
        # return [] if key_strings.empty?
#         
        # results = connection.multi_get(column_family, key_strings, {:count => 5000}).presence
#         
        # return [] if results.nil?
#         
        # models = []
        # key_strings.each do |key|
          # attributes = results[key].presence
          # if attributes.blank?
            # # It wasn't found in Cassandra.  Let's remove it from Sunspot so that we don't keep finding it.
            # Sunspot.remove_by_id(self.name, key) rescue nil
          # else
            # models << instantiate(key, attributes)
          # end
        # end
        # models
      # end
    # end
  end
end
module DatastaxRails
  module StatsMethods
    STATS_FIELDS={'sum' => 'sum', 'maximum' => 'max', 'minimum' => 'min', 'average' => 'mean', 'stddev' => 'stddev'}
    %w[sum maximum minimum average stddev].each do |op|
      define_method(op) do |field|
        calculate_stats(field)
        @stats[field][STATS_FIELDS[op]]
      end
      
      define_method("grouped_#{op}") do |field|
        self.op unless @group_value
        calculate_stats(field)
        values = {}
        @stats[field]["facets"][@group_value].each do |k,v|
          values[k] = v[STATS_FIELDS[op]]
        end
        values
      end
    end
    
    # def sum(field)
      # calculate_stats(field)
      # @stats[field]["sum"]
    # end
#     
    # def maximum(field)
      # calculate_stats(field)
      # @stats[field]["max"]
    # end
#     
    # def minimum(field)
      # calculate_stats(field)
      # @stats[field]["min"]
    # end
#     
    # def average(field)
      # calculate_stats(field)
      # @stats[field]["mean"]
    # end
#     
    # def stddev(field)
      # calculate_stats(field)
      # @stats[field]["stddev"]
    # end
    
    private
    def calculate_stats(field)
      unless @stats[field]
        @stats[field] = limit(0).compute_stats(field).stats[field]
      end
    end
  end 
end
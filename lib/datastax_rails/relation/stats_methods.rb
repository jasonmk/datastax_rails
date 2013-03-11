module DatastaxRails
  module StatsMethods
    STATS_FIELDS={'sum' => 'sum', 'maximum' => 'max', 'minimum' => 'min', 'average' => 'mean', 'stddev' => 'stddev'}
    
    # @!method sum(field)
    #   Calculates the sum of the field listed. Field must be indexed as a number.
    #   @param [Symbol] field the field to calculate
    #   @return [Fixnum,Float] the sum of the column value rows that match the query
    # @!method grouped_sum(field)
    #   Calculates the sum of the field listed for a grouped query.
    #   @param [Symbol] field the field to calculate
    #   @return [Hash] the sum of the columns that match the query by group. Group name is the key.
    # @!method maximum(field)
    #   Calculates the maximum value of the field listed. Field must be indexed as a number.
    #   @param [Symbol] field the field to calculate
    #   @return [Fixnum,Float] the maximum value of the rows that match the query
    # @!method grouped_maximum(field)
    #   Calculates the sum of the field listed for a grouped query.
    #   @param [Symbol] field the field to calculate
    #   @return [Hash] the sum of the columns that match the query by group. Group name is the key.
    # @!method sum(field)
    #   Calculates the sum of the field listed. Field must be indexed as a number.
    #   @param [Symbol] field the field to calculate
    #   @return [Fixnum,Float] the sum of the columns that match the query
    # @!method grouped_sum(field)
    #   Calculates the sum of the field listed for a grouped query.
    #   @param [Symbol] field the field to calculate
    #   @return [Hash] the sum of the columns that match the query by group. Group name is the key.
    # @!method sum(field)
    #   Calculates the sum of the field listed. Field must be indexed as a number.
    #   @param [Symbol] field the field to calculate
    #   @return [Fixnum,Float] the sum of the columns that match the query
    # @!method grouped_sum(field)
    #   Calculates the sum of the field listed for a grouped query.
    #   @param [Symbol] field the field to calculate
    #   @return [Hash] the sum of the columns that match the query by group. Group name is the key.
    # @!method sum(field)
    #   Calculates the sum of the field listed. Field must be indexed as a number.
    #   @param [Symbol] field the field to calculate
    #   @return [Fixnum,Float] the sum of the columns that match the query
    # @!method grouped_sum(field)
    #   Calculates the sum of the field listed for a grouped query.
    #   @param [Symbol] field the field to calculate
    #   @return [Hash] the sum of the columns that match the query by group. Group name is the key.
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
    
    private
    def calculate_stats(field)
      unless @stats[field]
        @stats[field] = limit(0).compute_stats(field).stats[field]
      end
    end
  end 
end
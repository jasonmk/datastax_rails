module DatastaxRails
  module Types
    class BinaryType < BaseType
      DEFAULTS = {:solr_type => false, :indexed => false, :stored => false, :multi_valued => false, :sortable => false, :tokenized => false, :fulltext => false}
      def encode(str)
        raise ArgumentError.new("#{self} requires a String") unless str.kind_of?(String)
        io = StringIO.new(Base64.encode64(str))
        chunks = []
        while chunk = io.read(1.megabyte)
          chunks << chunk
        end
        chunks
      end
      
      def decode(arr)
        if(arr.is_a?(Array))
          io = StringIO.new("","w+")
          arr.each do |chunk|
            io.write(chunk)
          end
          io.rewind
          Base64.decode64(io.read)
        else
          arr
        end
      end

      def wrap(record, name, value)
        (value.frozen? ? value.dup : value)
      end
    end
  end
end
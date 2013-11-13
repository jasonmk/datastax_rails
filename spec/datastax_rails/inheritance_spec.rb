require 'spec_helper'

class InheritanceTesterBase < DatastaxRails::Base
  self.abstract_class = true
end

class InheritanceTesterChild < InheritanceTesterBase
  
end

class InheritanceTesterGrandChild < InheritanceTesterChild
  
end

class InheritanceTesterNonAbstract < DatastaxRails::Base
  
end

describe DatastaxRails::Base do
  context "Inheritance" do
    it "raises NotImplementedError if DatastaxRails::Base is instantiated" do
      expect { DatastaxRails::Base.new }.to raise_error(NotImplementedError)
    end
    
    it "raises NotImplementedError if an abstract class is instantiated" do
      expect { InheritanceTesterBase.new }.to raise_error(NotImplementedError)
    end
    
    it "identifies the abstract class as base for direct decendants" do
      expect(InheritanceTesterChild.base_class).to eq(InheritanceTesterBase)
    end
  
    it "identifies the abstract class as base for its indirect decendants" do
      expect(InheritanceTesterGrandChild.base_class).to eq(InheritanceTesterBase)
    end  
    
    it "identifies a child of DatastaxRails::Base as base" do
      expect(InheritanceTesterNonAbstract.base_class).to eq(InheritanceTesterNonAbstract)
    end
  end
end

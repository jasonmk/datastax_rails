# A sample Guardfile
# More info at https://github.com/guard/guard#readme

guard :rspec do
  watch(%r{^spec/.+_spec\.rb$})
  watch(%r{^spec/support/.+\.rb$})
  watch(%r{^lib/(.+)\.rb$})     { |m| "spec/#{m[1]}_spec.rb" }
  watch('spec/spec_helper.rb')  { "spec" }
end


guard :rubocop, all_on_start: false, cli: '-D --auto-correct' do
  watch(%r{.+\.rb$})
  watch(%r{(?:.+/)?\.rubocop_todo\.yml$}) { |m| File.dirname(m[0]) }
end

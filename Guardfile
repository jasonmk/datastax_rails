# A sample Guardfile
# More info at https://github.com/guard/guard#readme

group :red_green_refactor, halt_on_fail: true do
  guard :rspec, failed_mode: :focus, cmd: 'BUNDLE_GEMFILE=/apps/rails_apps/datastax_rails/gemfiles/rails40.gemfile bundle exec rspec' do
    watch(%r{^spec/.+_spec\.rb$})
    watch(%r{^spec/support/.+\.rb$})
    watch(%r{^lib/(.+)\.rb$})     { |m| "spec/#{m[1]}_spec.rb" }
    watch('spec/spec_helper.rb')  { 'spec' }
  end

  guard :rubocop, all_on_start: false, cli: '-D --auto-correct' do
    watch(%r{.+\.rb$})
    watch(%r{(?:.+/)?\.rubocop\.yml$}) { |m| File.dirname(m[0]) }
  end
end

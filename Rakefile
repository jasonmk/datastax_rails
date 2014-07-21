#!/usr/bin/env rake
require 'rubygems'
begin
  require 'bundler/setup'
rescue LoadError
  puts 'You must `gem install bundler` and `bundle install` to run rake tasks'
end
begin
  require 'rdoc/task'
rescue LoadError
  require 'rdoc/rdoc'
  require 'rake/rdoctask'
  RDoc::Task = Rake::RDocTask
end

RDoc::Task.new(:rdoc) do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title    = 'DatastaxRails'
  rdoc.options << '--line-numbers'
  rdoc.options << '--main=README.rdoc'
  rdoc.rdoc_files.include('README.rdoc')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

APP_RAKEFILE = File.expand_path('../spec/dummy/Rakefile', __FILE__)
load 'rails/tasks/engine.rake'

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec) do |spec|
  # spec.rcov = true
  # spec.rcov_opts = %w{--rails --exclude osx\/objc,gems\/,spec\/,features\/,cassandra_object\/}
  spec.rspec_opts = File.read(File.expand_path('../spec/spec.opts', __FILE__)).split("\n")
end
task default: :spec

Bundler::GemHelper.install_tasks

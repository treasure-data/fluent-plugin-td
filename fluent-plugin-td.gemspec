# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'fluent/plugin/td_plugin_version'

Gem::Specification.new do |gem|
  gem.name        = "fluent-plugin-td"
  gem.description = "Treasure Data Cloud Data Service plugin for Fluentd"
  gem.homepage    = "http://www.treasuredata.com/"
  gem.summary     = gem.description
  gem.version     = Fluent::TreasureDataPlugin::VERSION
  gem.authors     = ["Treasure Data, Inc."]
  gem.email       = "support@treasure-data.com"
  gem.has_rdoc    = false
  #gem.platform    = Gem::Platform::RUBY
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_dependency "fluentd", [">= 0.10.27", "< 2"]
  gem.add_dependency "td-client", "~> 0.8.66"
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "webmock", "~> 1.16"
end

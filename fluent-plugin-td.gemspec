require File.expand_path('../lib/fluent/plugin/td_plugin_version', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = 'fluent-plugin-td'
  gem.description = 'Treasure Data Cloud Data Service plugin for Fluentd'
  gem.homepage    = 'https://www.treasuredata.com/'
  gem.summary     = gem.description
  gem.version     = Fluent::Plugin::TreasureDataPlugin::VERSION
  gem.authors     = ['Treasure Data, Inc.']
  gem.email       = 'support@treasure-data.com'
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map { |f| File.basename(f) }
  gem.require_paths = ['lib']
  gem.license       = 'Apache-2.0'

  gem.required_ruby_version = '>= 2.4'
  gem.add_dependency 'fluentd', ['>= 0.14.13', '< 2']
  gem.add_dependency 'td-client', '>= 1.0.8'
  gem.add_development_dependency 'rake', '>= 0.9.2'
  gem.add_development_dependency 'rubocop'
  gem.add_development_dependency 'rubocop-rake'
  gem.add_development_dependency 'test-unit', '~> 3.5.3'
  gem.add_development_dependency 'test-unit-rr', '~> 1.0.5'
  gem.add_development_dependency 'webmock', '~> 3.18.1'
end

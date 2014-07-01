require 'test_helper'
require 'fluent/plugin/out_tditem'

class TreasureDataItemOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  DEFAULT_CONFIG = %[
    database test
    table table
  ]

  def create_driver(conf = DEFAULT_CONFIG)
    config = %[
      apikey testkey
      buffer_type memory
    ] + conf

    Fluent::Test::BufferedOutputTestDriver.new(Fluent::TreasureDataItemOutput) do 
      def write(chunk)
        # TestDriver doesn't call acutual Output#emit so set key to get database and table in this place.
        chunk.instance_variable_set(:@key, @key)
        super(chunk)
      end
    end.configure(config)
  end

  def test_configure
    d = create_driver

    assert_equal('testkey', d.instance.apikey)
    assert_equal('test', d.instance.database)
    assert_equal('table', d.instance.table)
    assert_equal(true, d.instance.use_ssl)
    assert_equal('memory', d.instance.buffer_type)
    assert_equal(300, d.instance.flush_interval)
  end

  def test_configure_with_invalid_database
    assert_raise(Fluent::ConfigError) {
      create_driver(%[
        database a
        table table
      ])
    }
  end

  def test_configure_with_invalid_table
    assert_raise(Fluent::ConfigError) {
      create_driver(%[
        database test
        table 1
      ])
    }
  end

  def test_emit
    d = create_driver
    time, records = stub_seed_values
    stub_td_import_request(stub_request_body(records), d.instance.database, d.instance.table)

    records.each { |record|
      d.emit(record, time)
    }
    d.run

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_endpoint
    d = create_driver(DEFAULT_CONFIG + "endpoint foo.bar.baz")
    opts = {:endpoint => 'foo.bar.baz'}
    time, records = stub_seed_values
    stub_td_import_request(stub_request_body(records), d.instance.database, d.instance.table, opts)

    records.each { |record|
      d.emit(record, time)
    }
    d.run

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_too_many_keys
    d = create_driver(DEFAULT_CONFIG + "endpoint foo.bar.baz")
    opts = {:endpoint => 'foo.bar.baz'}
    time, _ = stub_seed_values
    stub_td_import_request(stub_request_body([]), d.instance.database, d.instance.table, opts)

    d.emit(create_too_many_keys_record, time)
    d.run

    assert_equal 0, d.emits.size
    assert d.instance.log.logs.select{ |line|
      line =~ / \[error\]: Too many number of keys/
    }.size == 1, "too many keys error is not logged"
  end
end

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

    assert_equal(d.instance.apikey, 'testkey')
    assert_equal(d.instance.database, 'test')
    assert_equal(d.instance.table, 'table')
    assert_equal(d.instance.use_ssl, true)
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

    assert_equal(@auth_header, 'TD1 testkey')
  end

  def test_emit
    d = create_driver(DEFAULT_CONFIG + "endpoint foo.bar.baz")
    opts = {:endpoint => 'foo.bar.baz'}

    time, records = stub_seed_values
    stub_td_import_request(stub_request_body(records), d.instance.database, d.instance.table, opts)

    records.each { |record|
      d.emit(record, time)
    }
    d.run

    assert_equal(@auth_header, 'TD1 testkey')
  end
end

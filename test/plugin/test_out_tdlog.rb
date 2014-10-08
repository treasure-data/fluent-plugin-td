require 'fluent/test'
require 'fluent/plugin/out_tdlog'

class TreasureDataLogOutputTest < Test::Unit::TestCase
  # BufferedOutputTestDriver uses module_eval, not inheritance.
  # This DummyOutput is for testing actual write method with webmock
  class TreasureDataLogDummyOutput < Fluent::TreasureDataLogOutput
  end

  def setup
    Fluent::Test.setup
  end

  TMP_DIR = File.dirname(__FILE__) + "/tmp"

  DEFAULT_CONFIG = %[
    database test
    table table
  ]

  def create_driver(conf = DEFAULT_CONFIG)
    config = %[
      apikey testkey
      buffer_path #{TMP_DIR}/buffer
    ] + conf

    Fluent::Test::BufferedOutputTestDriver.new(TreasureDataLogDummyOutput) do
      def write(chunk)
        chunk.instance_variable_set(:@key, @key)
        super(chunk)
      end
    end.configure(config)
  end

  def test_configure
    d = create_driver

    {:@apikey => 'testkey', :@use_ssl => true, :@auto_create_table => true,
     :@buffer_type => 'file', :@flush_interval => 300}.each { |k, v|
      assert_equal(d.instance.instance_variable_get(k), v)
    }
  end

  def test_emit
    d = create_driver
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)

    records.each { |record|
      d.emit(record, time)
    }
    d.run

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_time_symbole
    d = create_driver
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)

    records.each { |record|
      record[:time] = Time.now.to_i  # emit removes this :time key
      d.emit(record, time)
    }
    d.run

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_endpoint
    d = create_driver(DEFAULT_CONFIG + "endpoint foo.bar.baz")
    opts = {:endpoint => 'foo.bar.baz'}
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table, opts)
    stub_td_import_request(stub_request_body(records, time), database, table, opts)

    records.each { |record|
      d.emit(record, time)
    }
    d.run
  end

  def test_emit_with_too_many_keys
    d = create_driver(DEFAULT_CONFIG + "endpoint foo.bar.baz")
    opts = {:endpoint => 'foo.bar.baz'}
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table, opts)
    stub_td_import_request(stub_request_body([], time), database, table, opts)

    d.emit(create_too_many_keys_record, time)
    d.run

    assert_equal 0, d.emits.size
    assert d.instance.log.logs.select{ |line|
      line =~ / \[error\]: Too many number of keys/
    }.size == 1, "too many keys error is not logged"
  end

  # TODO: add normalized_msgpack / tag split test

## TODO invalid names are normalized
#  def test_invalid_name
#    d = create_driver
#    d.instance.start
#
#    es = Fluent::OneEventStream.new(Time.now.to_i, {})
#    chain = Fluent::NullOutputChain.instance
#    assert_raise(RuntimeError) do
#      d.instance.emit("test.invalid-name", es, chain)
#    end
#    assert_raise(RuntimeError) do
#      d.instance.emit("empty", es, chain)
#    end
#    assert_raise(RuntimeError) do
#      d.instance.emit("", es, chain)
#    end
#  end

## TODO invalid data is ignored
#  def test_invalid_data
#    d = create_driver
#    d.instance.start
#
#    es = Fluent::OneEventStream.new(Time.now.to_i, "invalid")
#    chain = Fluent::NullOutputChain.instance
#    assert_nothing_raised do
#      d.instance.emit("test.name", es, chain)
#    end
#  end
end


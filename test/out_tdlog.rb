require 'fluent/test'
require 'fluent/plugin/out_tdlog'

class TreasureDataLogOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TMP_DIR = File.dirname(__FILE__) + "/tmp"

  CONFIG = %[
    apikey testkey
    buffer_path #{TMP_DIR}/buffer
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::TreasureDataLogOutput) do
      def start
        super
      end

      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.run
  end

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


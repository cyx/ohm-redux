require "cutest"
require_relative "../lib/ohm"

class User < Ohm::Model
  attribute :foo, lambda { |x| x.to_i }
end

prepare {
  Ohm.redis.flushdb
}

test "attribute declaration" do
  assert User.attributes.include?(:foo)

  u = User.new
  u.foo = "1"

  assert_equal 1, u.foo

  u = User.new(foo: "2")
  assert_equal 2, u.foo
end

test "saving" do
  u = User.new
  u.foo = "1"
  u.save!

  u = User[u.id]
  assert_equal 1, u.foo

  u.foo = "2"
  u.save!

  assert_equal 2, u.foo
  u = User[u.id]
  assert_equal 2, u.foo
end

test "transactionally saving" do
  u = User.new
  u.foo = "1"
  u.save

  u = User[u.id]
  assert_equal 1, u.foo

  u.foo = 2
  u.save

  u = User[u.id]
  assert_equal 2, u.foo
end

test "pipelining mass loaded models" do
  require "benchmark"

  uids = 1000.times.map { |i| u = User.new(foo: i); u.save; u.id }

  t1 = Benchmark.realtime {
    uids.map(&User)
  }

  t2 = Benchmark.realtime {
    res = User.db.pipelined {
      uids.each { |uid| User.key[uid].hgetall }
    }

    users = res.map.with_index { |e, i| User.to_proc[uids[i], Hash[*e]] }
  }

  assert t1 > t2
end
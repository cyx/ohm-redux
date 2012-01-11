require "nest"
require "redis"
require "securerandom"

module Ohm
  def self.redis
    @redis ||= Redis.connect
  end

  def self.transaction(&block)
    t = Transaction.new
    yield t
    t.commit(redis)
  end

  class Transaction
    def watch(*keys)
      @keys = keys if keys.any?
    end

    def read(&block)
      @read = block
    end

    def write(&block)
      @write = block
    end

    def commit(db)
      loop do
        db.watch(*@keys) if @keys
        vars = @read.call if @read

        break if db.multi do
          @write.call(vars)
        end
      end
    end
  end

  class Model
    def self.attributes
      @attributes ||= []
    end

    def self.db
      Ohm.redis
    end

    def self.key
      Nest.new(self.name, db)
    end

    def self.new_id
      key[:id].incr
    end

    def self.exists?(id)
      key[:all].sismember(id)
    end

    def self.attribute(name, cast = nil)
      if cast
        define_method(name) do
          cast[read_local(name)]
        end
      else
        define_method(name) do
          read_local(name)
        end
      end

      define_method(:"#{name}=") do |value|
        write_local(name, value)
      end

      attributes << name unless attributes.include?(name)
    end

    def self.[](id)
      to_proc[id] if exists?(id)
    end

    def self.to_proc
      lambda do |id, attributes = nil|
        attributes = key[id].hgetall if attributes.nil?
        attributes[:id] = id

        new(attributes)
      end
    end

    def initialize(attributes = {})
      @_attributes = {}

      update_attributes(attributes)
    end

    def id
      @id or raise(MissingID)
    end

    def update_attributes(attributes)
      attributes.each do |key, value|
        send(:"#{key}=", value)
      end
    end

    def new?
      ! @id
    end

    def save
      return create if new?

      transaction do |t|
        t.write do |id|
          save!
        end
      end
    end

    def create
      transaction do |t|
        t.read do
          initialize_id
        end

        t.write do
          create!
        end
      end
    end

    def save!
      key.del
      key.hmset(*flattened_attributes)
    end

    def create!
      model.key[:all].sadd(id)
      key.hmset(*flattened_attributes)
    end

  protected
    def id=(id)
      @id = id
    end

    def transaction(&block)
      Ohm.transaction(&block)
    end

    def model
      self.class
    end

    def key
      model.key[id]
    end

    def attributes
      model.attributes
    end

    def initialize_id(id = model.new_id)
      @id = id
    end

    def flattened_attributes
      [].tap do |ret|
        attributes.each do |att|
          val = send(att).to_s

          ret.push(att, val) unless val.empty?
        end
      end
    end

    def write_local(attribute, value)
      @_attributes[attribute] = value
    end

    def read_local(attribute)
      @_attributes[attribute]
    end

    class Error < StandardError; end

    class MissingID < Error
      def message
        "You tried to perform an operation that needs the model ID, " +
        "but it's not present."
      end
    end
  end
end

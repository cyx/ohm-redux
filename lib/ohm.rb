require "nest"
require "redis"
require "securerandom"

module Ohm
  def self.redis
    @redis ||= Redis.connect
  end

  class Model
    def self.attributes
      @attributes ||= []
    end

    def self.db
      Ohm.redis
    end

    def self.transaction(*keys)
      loop do
        db.watch(*keys) if keys.any?

        break if db.multi do
          yield
        end
      end
    end

    def self.key
      Nest.new(self.name, db)
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
      return unless exists?(id)

      to_proc[id]
    end

    def self.to_proc
      lambda do |id, attributes = nil|
        attributes = key[id].hgetall if attributes.nil?
        attributes[:id] = id

        new(attributes)
      end
    end

    def self.exists?(id)
      key[:all].sismember(id)
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
      transaction do
        save!
      end
    end

    def save!
      if new?
        initialize_id
        self.class.key[:all].sadd(id)
      end

      key.del
      key.hmset(*flattened_attributes)
    end

  protected
    def id=(id)
      @id = id
    end

    def transaction
      if new?
        self.class.transaction { yield }
      else
        self.class.transaction(key) { yield }
      end
    end

    def key
      self.class.key[id]
    end

    def attributes
      self.class.attributes
    end

    def initialize_id
      @id ||= Digest::SHA1.hexdigest(SecureRandom.uuid)
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
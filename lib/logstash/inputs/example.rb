# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Example < LogStash::Inputs::Base
  config_name "example"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain" 

  # The message string to use in the event.
  config :message, :validate => :string, :default => "Hello World!"

  # Set how frequently messages should be sent.
  #
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 1

  public
  def register
    @host = Socket.gethostname
  end # def register

  def run(queue)
    Stud.interval(@interval) do
      event = LogStash::Event.new("message" => @message, "host" => @host)
      decorate(event)
      queue << event
    end # loop
  end # def run

end # class LogStash::Inputs::Example
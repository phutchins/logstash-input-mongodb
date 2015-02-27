# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::MongoDB < LogStash::Inputs::Base
  config_name "mongodb"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Example URI: mongodb://mydb.host:27017/mydbname?ssl=true
  config :uri, validate => :string, :required => true

  # The path to the sqlite database file.
  config :path, :validate => :string, :required => true

  # Any table to exclude by name
  config :exclude_tables, :validate => :array, :default => []

  # The database to use
  config :database, :validate => :string, :required => true

  # The collection to use. Should accept wildcard (i.e. 'events_*')
  # Example collection: events_20150227
  config :collection, :validate => :string, :required => true

  # If true, store the @timestamp field in mongodb as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates
  config :isodate, :validate => :boolean, :default => false

  # Number of seconds to wait after failure before retrying
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false

  # The message string to use in the event.
  config :message, :validate => :string, :default => "Hello World!"

  # Set how frequently messages should be sent.
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 1

  SINCE_TABLE = :since_table

  public
  def init_placeholder_table(sqlitedb)
    begin
      sqlitedb.create_table SINCE_TABLE do
        String :table
        Int :place
      end
    rescue
      @logger.debug("since table already exists")
    end
  end

  public
  def get_placeholder(sqlitedb, table)
    since = db[SINCE_TABLE]
    x = since.where(:table => "#{table}")
    if x[:place].nil?
      init_placeholder(sqlitedb, table)
      return 0
    else
      @logger.debug("placeholder already exists, it is #{x[:place]}")
      return x[:place][:place]
    end
  end

  public
  def init_placeholder(sqlitedb, table)
    @logger.debug("init placeholder for #{table}")
    since = db[SINCE_TABLE]
    since.insert(:table => table, :place => 0)
  end

  public
  def update_placeholder(db, table, place)
    @logger.debug("set placeholder to #{place}")
    since = db[SINCE_TABLE]
    since.where(:table => table).update(:place => place)
  end

  public
  def get_n_rows_from_table(mongodb, table, offset, limit)
    dataset = db[]
  end

  public
  def get_collection_names(mongodb)
    return @mongodb.collection_names
  end

  public
  def get_all_tables(mongodb)
    return @mongodb.collection_names
  end

  public
  def register
    require "mongo"
    include Mongo
    require "jdbc/sqlite3"
    require "sequel"
    uriParsed = Mongo::URIParser.new(@uri)
    conn = uriParsed.connection({})
    if uriParsed.auths.length > 0
      uriParsed.auths.each do |auth|
        if !auth['db_name'].nil?
          conn.add_auth(auth['db_name'], auth['username'], auth['password'], nil)
        end
      end
      conn.apply_saved_authentication()
    end
    @host = Socket.gethostname
    @logger.info("Registering MongoDB input", :database => @path)
    @mongodb = conn.db(@database)
    @sqlitedb = Sequel.connect("jdbc:sqlite:#{@path}")
    # Should check to see if there are new matching tables at a predefined interval or on some trigger
    @collections = get_collection_names(@mongodb)
    @collection_data = {}
    @collections.each do |collection|
      init_placeholder_table(@sqlitedb)
      last_place = get_placeholder(@sqlitedb, collection)
      @collection_data[collection] = { :name => collection, :place => last_place }
    end

  end # def register

  def run(queue)
    sleep_min = 0.01
    sleep_max = 5
    sleeptime = sleep_min

    begin
      @logger.debug("Tailing MongoDB", :path => @path)
      loop do
        count = 0
        @table_data.each do |k, table|
          table_name = table[:name]
          offset = table[:place]
          @logger.debug("offset is #{offset}", :k => k, :table => table_name)
          rows = get_n_rows_from_table(@mongodb, table_name, offset, @batch)
          count += rows.count
          rows.each do |row|
            event = LogStash::Event.new("host" => @host, "db" => @db)
            decorate(event)
            # store each column as a field in the event
            row.each do |column, element|
              next if column == :id
              event[column.to_s] = element
            end
            queue << event
            @table_data[k][:place] = row[:id]
          end
          # Store the last-seen row in the database
          update_placeholde(@sqlitedb, table_name, @table_data[k][:place])
        end

        if count == 0
          # nothing found in that iteration
          # sleep a bit
          @logger.debug("No new rows. Sleeping.", :time => sleeptime)
          sleeptime = [sleeptime * 2, sleep_max].min
          sleep(sleeptime)
        else
          sleeptime = sleep_min
        end
      end
    end
  end # def run

end # class LogStash::Inputs::Example

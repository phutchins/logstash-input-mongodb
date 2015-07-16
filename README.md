# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elasticsearch/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Documentation

### Configuration

Example
```
input {
  mongodb {
    uri => 'mongodb://10.0.0.30/my-logs?ssl=true'
    path => '/opt/logstash-mongodb/logstash_sqlite.db'
    collection => 'events_'
    unpack_mongo_id => true
    batch_size => 5000
  }
}

filter {
  date {
    match => [ "logdate", "ISO8601" ]
  }
}

output {
  redis {
    host => "localhost"
    data_type => "list"
    key => "logstash-mylogs"
  }
}
```

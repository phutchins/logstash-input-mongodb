require 'minitest/autorun'
require 'json'
require 'mongo'
require 'bson'

class TestThis < MiniTest::Test
  def test_that_this_works
    input = {"_id"=>BSON::ObjectId('558de77ec5ed007567574a58'), "tags"=>["http"], "info"=>{"method"=>"POST", "rtime"=>"127", "url"=>"apiginvoice", "data"=>{"a"=>291.35, "b"=>291.25, "c"=>291.16, "d"=>289.68, "e"=>261.73, "f"=>Float::NAN}, "host"=>"bitpay.com", "status"=>200, "referrer"=>nil, "raddr"=>"31.192.114.250", "ver"=>"1.1", "ua"=>nil, "rlen"=>nil, "rlocation"=>nil, "query"=>{}}}
    input2 = {"_id"=>BSON::ObjectId('55cade9a5ef4000e2df9403e'), "tags"=>["info", "api"], "info"=>{"message"=>"api request completed: (%(ptype)s: %(pdata)s) %(facade)s.%(method)s init: %(id)sms facade: %(fd)sms total: %(total)sms params: %(params)s", "data"=>{"ptype"=>"no policy", "pdata"=>"none", "facade"=>"public", "method"=>"getInvoice", "id"=>0, "fd"=>5, "total"=>5, "params"=>"{\"id\":\"EwkJYnFj2bFE9e6xf3aq\"}", "raddr"=>"104.10.38.126"}}}
    input3 = {"_id"=>BSON::ObjectId('55ccdccf5d36005258e35972'), "tags"=>["info", "api"], "info"=>{"message"=>"API v1 request %(url)s", "data"=>{"path"=>"/rates/:currencyCode", "url"=>"/api/rates/eur", "query"=>{}, "raddr"=>"54.85.231.179"}}}
    input4 = {"_id"=>BSON::ObjectId('55cdfede6d2800b42c8b8c6f'), "tags"=>["http"], "info"=>{"method"=>"GET", "rtime"=>"17", "url"=>"/api", "host"=>"bitpay.com", "status"=>200, "referrer"=>nil, "raddr"=>"81.171.50.83", "ver"=>"1.1", "ua"=>nil, "rlen"=>"214457", "rlocation"=>nil, "query"=>{}}}

    expected = "your mom"
    assert_equal flatten(input4), expected
  end
  def test_the_flat_parser
    input = {"_id"=>BSON::ObjectId('558de77ec5ed007567574a58'), "tags"=>["http"], "info"=>{"method"=>"POST", "rtime"=>"127", "url"=>"apiginvoice", "data"=>{"a"=>291.35, "b"=>291.25, "c"=>291.16, "d"=>289.68, "e"=>261.73, "f"=>Float::NAN}, "host"=>"bitpay.com", "status"=>200, "referrer"=>nil, "raddr"=>"31.192.114.250", "ver"=>"1.1", "ua"=>nil, "rlen"=>nil, "rlocation"=>nil, "query"=>{}}}
    input2 = {"_id"=>BSON::ObjectId('55cade9a5ef4000e2df9403e'), "tags"=>["info", "api"], "info"=>{"message"=>"api request completed: (%(ptype)s: %(pdata)s) %(facade)s.%(method)s init: %(id)sms facade: %(fd)sms total: %(total)sms params: %(params)s", "data"=>{"ptype"=>"no policy", "pdata"=>"none", "facade"=>"public", "method"=>"getInvoice", "id"=>0, "fd"=>5, "total"=>5, "params"=>"{\"id\":\"EwkJYnFj2bFE9e6xf3aq\"}", "raddr"=>"104.10.38.126"}}}
    input3 = {"_id"=>BSON::ObjectId('55ccdccf5d36005258e35972'), "tags"=>["info", "api"], "info"=>{"message"=>"API v1 request %(url)s", "data"=>{"path"=>"/rates/:currencyCode", "url"=>"/api/rates/eur", "query"=>{}, "raddr"=>"54.85.231.179"}}}
    input4 = {"_id"=>BSON::ObjectId('55cdfede6d2800b42c8b8c6f'), "tags"=>["http"], "info"=>{"method"=>"GET", "rtime"=>"17", "url"=>"/api", "host"=>"bitpay.com", "status"=>200, "referrer"=>nil, "raddr"=>"81.171.50.83", "ver"=>"1.1", "ua"=>nil, "rlen"=>"214457", "rlocation"=>nil, "query"=>{}}}
    expected = "your mom"
    assert_equal flat_doc(flatten(input4)), expected
  end
end

def flatten(my_hash)
  new_hash = {}
  if my_hash.respond_to? :each
    my_hash.each do |k1,v1|
      if v1.is_a?(Hash)
        v1.each do |k2,v2|
          if v2.is_a?(Hash)
            # puts "Found a nested hash"
            result = flatten(v2)
            result.each do |k3,v3|
              new_hash[k1.to_s+"_"+k2.to_s+"_"+k3.to_s] = v3
            end
            # puts "result: "+result.to_s+" k2: "+k2.to_s+" v2: "+v2.to_s
          else
            new_hash[k1.to_s+"_"+k2.to_s] = v2
          end
        end
      else
        # puts "Key: "+k1.to_s+" is not a hash"
        new_hash[k1.to_s] = v1
      end
    end
  end
  return new_hash
end

def flat_doc(flat_doc)
  event = {}
  flat_doc.each do |k,v|
    # Check for an integer
    if v.is_a? Numeric
      event[k.to_s] = v
    elsif v.is_a? String
      if v == "NaN"
        event[k.to_s] = Float::NAN
      elsif /\A[-+]?\d+[.][\d]+\z/ == v
        event[k.to_s] = v.to_f
      elsif (/\A[-+]?\d+\z/ === v) || (v.is_a? Integer)
        event[k.to_s] = v.to_i
      end
    else
      event[k.to_s] = v.to_s unless k.to_s == "_id" || k.to_s == "tags"
      if (k.to_s == "tags") && (v.is_a? Array)
        event['tags'] = v
      end
    end
  end
end

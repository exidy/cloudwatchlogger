require 'socket'
require 'cloudwatchlogger/threaded'

module CloudWatchLogger
  class Client
    attr_reader :input_uri, :deliverer

    def initialize(credentials, log_group_name, log_stream_name, opts = {})
      unless log_group_name
        raise LogGroupNameRequired, 'log_group_name is required'
      end

      unless log_stream_name
        raise LogStreamNameRequired, 'log_stream_name is required'
      end

      @credentials = credentials
      @log_group_name = log_group_name
      @log_stream_name = log_stream_name
      @deliverer = CloudWatchLogger::DeliveryThreadManager.new(@credentials, @log_group_name, @log_stream_name, opts)
    end

    def write(message)
      @deliverer.deliver(message)
    end

    def close
      nil
    end

    def formatter
      proc do |severity, datetime, progname, msg|
        processid = Process.pid
        host = Socket.gethostname.gsub(/(\.curb\.verifonets\.com)|(#{@log_stream_name})-/, "")
        massage_message(msg, severity, processid, host)
      end
    end

    private

    def massage_message(incoming_message, severity, processid, host)
      outgoing_message = ''

      outgoing_message << "server=#{host}, pid=#{processid}, severity=#{severity}, "

      outgoing_message << case incoming_message
      when Hash
        masher(incoming_message)
      when String
        incoming_message
      else
        incoming_message.inspect
      end
      outgoing_message
    end

    def masherize_key(prefix, key)
      [prefix, key.to_s].compact.join('.')
    end

    def masher(hash, prefix = nil)
      hash.map do |v|
        if v[1].is_a?(Hash)
          masher(v[1], masherize_key(prefix, v[0]))
        else
          "#{masherize_key(prefix, v[0])}=" << case v[1]
          when Symbol
           v[1].to_s
          else
           v[1].inspect
          end
        end
      end.join(', ')
    end
  end
end

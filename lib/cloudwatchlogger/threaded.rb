require 'aws-sdk-cloudwatchlogs'
require 'thread'
require 'busted'

module CloudWatchLogger
  class DeliveryThreadManager
    def initialize(credentials, log_group_name, log_stream_name, opts = {})
      @credentials = credentials
      @log_group_name = log_group_name
      @log_stream_name = log_stream_name
      @opts = opts
      @queue = Queue.new
      @curr_date = Date.today
      start_thread
    end

    # Pushes a message to the delivery thread, starting one if necessary
    def deliver(message)
      rotate_stream if @curr_date != Date.today
      start_thread unless @thread.alive?
      @thread.deliver(message)
      # Race condition? Sometimes we need to rescue this and start a new thread
    rescue NoMethodError
      @thread.kill # Try not to leak threads, should already be dead anyway
      start_thread
      retry
    end

    private

    def start_thread
      @thread = DeliveryThread.new(@credentials, @log_group_name, stream_name, @queue, @opts)
    end
    
    def stream_name
      [@log_stream_name, @curr_date].join("/")
    end
    
    def rotate_stream
      @curr_date = Date.today
      @thread.kill
      start_thread
    end
  end

  class DeliveryThread < Thread
    def initialize(credentials, log_group_name, log_stream_name, queue, opts = {})
      Busted.start
      opts[:open_timeout] = opts[:open_timeout] || 120
      opts[:read_timeout] = opts[:read_timeout] || 120
      @max_queue_size = opts.delete(:max_queue) || 25
      @credentials = credentials
      @log_group_name = log_group_name
      @log_stream_name = log_stream_name
      @opts = opts
      @queue = queue
      
      @exiting = false
      
      super do
        loop do
          batch = []
          itr = 0
          connect!(opts) if @client.nil?
          
          while itr < @max_queue_size
            msg = @queue.pop
            break if msg == :__delivery_thread_exit_signal__
            batch << msg
            itr += 1
          end
          
          begin
            event = {
              log_group_name: @log_group_name,
              log_stream_name: @log_stream_name,
              log_events: batch
            }
            event[:sequence_token] = @sequence_token if @sequence_token
            response = @client.put_log_events(event)
            unless response.rejected_log_events_info.nil?
              raise CloudWatchLogger::LogEventRejected
            end
            @sequence_token = response.next_sequence_token
            break if @exiting
          rescue Aws::CloudWatchLogs::Errors::InvalidSequenceTokenException => err
            @sequence_token = err.message.split(' ').last
            retry
          end
        end
      end

      at_exit do
        @queue.push(Busted.finish)
        exit!
        join
      end
    end

    # Signals the queue that we're exiting
    def exit!
      @exiting = true
      @queue.push :__delivery_thread_exit_signal__
    end

    # Pushes a message onto the internal queue
    def deliver(message)
      @queue.push({timestamp: (Time.now.utc.to_f.round(3) * 1000).to_i, message: message})
    end

    def connect!(opts = {})
      args = { http_open_timeout: opts[:open_timeout], http_read_timeout: opts[:read_timeout] }
      args[:region] = @opts[:region] if @opts[:region]
      args.merge!( @credentials.key?(:access_key_id) ? { access_key_id: @credentials[:access_key_id], secret_access_key: @credentials[:secret_access_key] } : {} )

      @client = Aws::CloudWatchLogs::Client.new(args)
      begin
        @client.create_log_stream(
          log_group_name: @log_group_name,
          log_stream_name: @log_stream_name
          )
      rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
        @client.create_log_group(
          log_group_name: @log_group_name
          )
        retry
      rescue Aws::CloudWatchLogs::Errors::ResourceAlreadyExistsException
      end
    end
  end
end
require 'cloudwatchlogger/client'
require 'logger'

module CloudWatchLogger
  class LogGroupNameRequired < ArgumentError; end
  class LogStreamNameRequired < ArgumentError; end
  class LogEventRejected < ArgumentError; end

  def self.setup_logger(log_group_name, log_stream_name = nil, level = :debug,  credentials = {}, opts = {})
    client = CloudWatchLogger::Client.new(credentials, log_group_name, log_stream_name, opts)
    logger = Logger.new(client)
    logger.level = level
    logger.formatter = client.formatter

    logger
  end
end

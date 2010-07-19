require 'lib/nntp_server'
require 'optparse'

Kernel::class_eval(open('config.rb').read)

OptionParser.new do |opts|
	opts.banner = "Usage: run.rb [options]"

	opts.on("-p", "--port [PORT]", Integer, "Specify port") do |v|
		PORT = v
	end

	opts.on("-d", "--daemonize", "Daemonize server") do |v|
		DAEMONIZE = true
	end

	opts.on("-f", "--foreground", "Run server in foreground") do |v|
		DAEMONIZE = false
	end

	opts.on("-h", "--help", "This help text") do |v|
		puts opts
		exit
	end
end.parse!

if DAEMONIZE
	require 'daemons'
	Daemons.daemonize
end

EventMachine::run {
	EventMachine::start_server '0.0.0.0', PORT, NNTPServer
}

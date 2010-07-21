$: << File.dirname(__FILE__) + '/lib'
require 'nntp_server'
require 'optparse'

EventMachine::run {
	eval(open(
		if File.readable?(File.dirname(__FILE__) + '/config.rb')
			File.dirname(__FILE__) + '/config.rb'
		else
			ENV['XDG_CONFIG_DIRS'].split(/:/).unshift('/etc').select {|dir|
				File.readable?(dir + '/xnntpd/config.rb')
			}.first + '/xnntpd/config.rb'
		end
	).read)

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

	EventMachine::start_server '0.0.0.0', PORT, NNTPServer
}

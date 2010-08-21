# encoding: utf-8
$: << File.dirname(__FILE__) + '/lib'
require 'optparse'

require 'em-mysqlplus'

EventMachine::error_handler { |e|
	LOG.error e.message
}

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

	unless defined?(LOG)
		LOG = Class.new {
			def method_missing(*args)
				p args
			end
		}.new
	end

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
		Daemons.daemonize(:app_name => "XNNTPD on #{PORT}")
	end

	DB = EventMachine::MySQL.new(MYSQL.merge(:encoding => 'utf8'))
	DB.query("CREATE TABLE IF NOT EXISTS `keys` (fingerprint CHAR(40) PRIMARY KEY, `key` BLOB)")
	DB.query("CREATE TABLE IF NOT EXISTS messages (message_id CHAR(150) PRIMARY KEY,
	          newsgroup CHAR(255), post_peer CHAR(255), encoded TEXT)")
	DB.query("CREATE TABLE IF NOT EXISTS messages_sent (message_id CHAR(150), server CHAR(150),
	          CONSTRAINT UNIQUE INDEX id_server (message_id, server))")

	# Define PEERS to have this process send messages to other servers
	if defined?(PEERS)
		require 'peers'
		process_peers(PEERS, DB, LOG)
	end

	# Define HOST and PORT to have this process accept NNTP connections
	if defined?(HOST) && defined?(PORT)
		require 'nntp_server'
		LOG.info "Started on #{HOST}:#{PORT}"

		EventMachine::start_server '0.0.0.0', PORT, NNTPServer
	end
}

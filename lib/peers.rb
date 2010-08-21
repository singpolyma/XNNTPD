# encoding: utf-8
require 'uri'

require 'em-mysqlplus'

require 'multi'
require 'util'

module IHAVEClient
	def initialize(*args)
		super
		set_pending_connect_timeout(10)
		set_comm_inactivity_timeout(10)
	end

	def post_init
		@handlers = [nil]
		@buffer = ''
	end

	def receive_data(data)
		data.force_encoding('binary').each_char { |c| # Make no assumptions about the data
			@buffer << c
			if @buffer[-2..-1] == "\r\n" # Read a line
				if h = @handlers.shift
					h.call(@buffer.chomp)
				end
					@buffer = ''
			end
		}
	end

	# c = :ihave | :post
	def command(c, m, &cb)
		@handlers << lambda {|h|
			status = h.split(/\s+/).first.to_i
			if status == 335 || status == 240
				@handlers << lambda {|h|
					quit
					cb.call(true) # Ignore errors, since they just mean we should not try again
				}
				send_data m[:encoded] + "\r\n.\r\n"
			else
				quit
				cb.call(true) # We were told not to try again
			end
		}
		if c == :ihave
			send_data "IHAVE #{m[:message_id]}\r\n"
		else # :post
			send_data "POST\r\n"
		end
	end

	def quit
		send_data "QUIT\r\n"
	end

	def unbind
		if h = @handlers.shift
			h.call(nil)
		end
	end
end

def send_ihave(db, peer, m)
	uri = URI::parse(peer)
	# Skip sending if path header says it's been there
	if m[:mime] && m[:mime][:path] && \
		(m[:mime][:path].decoded == uri.host || \
		m[:mime][:path].decoded == "#{uri.host}:#{uri.port}")
		db.query("INSERT INTO messages_sent VALUES
					('#{Mysql::escape_string(m[:message_id])}',
					 '#{Mysql::escape_string(peer)}')")
		return nil
	end
	EventMachine::connect(uri.host, uri.port || 119, IHAVEClient) { |nntp|
		nntp.command(:ihave, m) { |success|
			if success # We sent the message, record that fact
				db.query("INSERT INTO messages_sent VALUES
							('#{Mysql::escape_string(m[:message_id])}',
							 '#{Mysql::escape_string(peer)}')")
			end
		}
	}
end

def send_post(db, peer, m)
	uri = URI::parse(peer)
	EventMachine::connect(uri.host, uri.port || 119, IHAVEClient) { |nntp|
		nntp.command(:post, m) { |success|
			if success # We sent the message, record that fact
				db.query("DELETE FROM messages WHERE
				          post_peer='#{Mysql::escape_string(peer)}' AND
				          message_id='#{Mysql::escape_string(m[:message_id])}'")
			end
		}
	}
end

def get_article(r)
	if r['encoded'].to_s == ''
		Util::backend(r['newsgroup']).article(r['newsgroup'], :message_id => r['message_id']) {|m|
			m = Util::hash_to_mime(m)
			m[:encoded] = m[:mime].encoded
			m[:message_id] = m[:mime][:message_id].decoded
			yield m
		}
	else
		yield ({:encoded => r['encoded'], :message_id => r['message_id']})
	end
end

def process_peers(peers, db, log, time=60)
	process = lambda {
		request = Multi.new
		peers.each { |peer|
			request << lambda {|&cb|
				db.query("SELECT messages.message_id,newsgroup,encoded FROM messages LEFT JOIN messages_sent
				          ON messages.message_id=messages_sent.message_id AND messages_sent.server='#{peer}'
				          WHERE isNULL(server) AND isNULL(post_peer) LIMIT 20") { |result|
					result.all_hashes.each {|r|
						get_article(r) {|m|
							log.info "IHAVE #{m[:message_id]} to #{peer}"
							send_ihave(db, peer, m)
							cb.call
						}
					}
				}
			}
		}
		request << lambda {|&cb|
			db.query('SELECT message_id,newsgroup,encoded,post_peer
			          FROM messages WHERE NOT isNULL(post_peer)') { |result|
				result.all_hashes.each {|r|
					get_article(r) {|m|
						log.info "POST #{m[:message_id]} to #{peer}"
						send_post(db, r['post_peer'], m)
						cb.call
					}
				}
			}
		}
		request.call {
			EventMachine::add_timer(time, &process)
		}
	}
	process.call
end

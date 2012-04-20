# Based on https://github.com/igrigorik/em-synchrony/blob/master/lib/em-synchrony/connection_pool.rb

module EventMachine
	class ConnectionPool
		def initialize(opts={})
			@available = []
			@pending = []
		
			(opts[:size] || 1).times do
				@available.push(yield opts)
			end
		end

		def method_missing(m, *args)
			acquire { |conn|
				begin
					df = conn.__send__(m, *args)
					df.callback { release(conn) }
					df.errback { release(conn) }
					yield df
				rescue Exception
					release(conn)
					yield $!
				end
			}
		end

		protected

		def acquire(&blk)
			EM::schedule {
				if conn = @available.shift
					blk.call(conn)
				else
					@pending << blk
				end
			}
		end

		def release(conn)
			EM::schedule {
				@available << conn

				unless @pending.empty?
					@pending.shift.call(@available.shift)
				end
			}
		end
	end
end

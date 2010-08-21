# encoding: utf-8
require 'eventmachine'
require 'future'

class SimpleProtocolServer < EventMachine::Connection

	# Override this in subclasses to add more commands
	def commands
		{
			/^quit$/i => lambda {|data| close_connection},
		}
	end

	def post_init
		@buffer = ''
		@multiline = false
		@output_q = []
		peer = get_peername[2,6].unpack('nC4')
		@peer = {:port => peer.shift, :ip => peer.join('.')}
		LOG.info "Connection from #{@peer[:ip]} on port #{@peer[:port]}"
	end

	def receive_data(data)
		data.force_encoding('binary').each_char { |c| # Make no assumptions about the data
			@buffer += c
			if @multiline
				if @buffer[-5..-1] == "\r\n.\r\n" || @buffer == ".\r\n"
					LOG.info "#{@peer[:ip]} #{@buffer}"
					@output_q << @multiline.call(@buffer.gsub(/\r?\n?\.\r\n$/, ''))
					@multiline = false
					@buffer = ''
				end
			elsif @buffer[-2..-1] == "\r\n" # Commands are only one line
				@buffer.chomp!
				LOG.info "#{@peer[:ip]} #{@buffer}"
				commands.each do |pattern, block|
					if pattern === @buffer # If this command matches, defer its block
						@output_q << block.call(@buffer.gsub(pattern, ''))
						break # Only match one command
					end
				end
				@buffer = ''
			end
		}
		future_ready if @output_q.length > 0
	end

	def future_ready
		EM::schedule do
			while @output_q.length > 0 && (!@output_q.first.is_a?(Future) || @output_q.first.ready?)
				rtrn = @output_q.shift
				rtrn = rtrn.value if rtrn.is_a?(Future)
				rtrn = rtrn.join("\r\n") + "\r\n." if rtrn.respond_to?:join
				send_data(rtrn.to_s + "\r\n")
			end
		end
	end

end

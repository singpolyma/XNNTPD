# encoding: utf-8
require 'eventmachine'

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
	end

	def receive_data(data)
		data.force_encoding('binary').each_char { |c| # Make no assumptions about the data
			@buffer += c
			if @multiline
				if @buffer[-5..-1] == "\r\n.\r\n"
					# XXX: Not using defer because the data fails to get through that way
					callback(@multiline.call(@buffer.gsub(/\r?\n?\.\r\n$/, '')))
					@multiline = false
					@buffer = ''
				end
			elsif @buffer[-2..-1] == "\r\n" # Commands are only one line
				@buffer.chomp!
				commands.each do |pattern, block|
					if pattern === @buffer # If this command matches, defer its block
						EventMachine::defer lambda { block.call(@buffer.gsub(pattern, '')) }, method(:callback)
						break # Only match one command
					end
				end
				@buffer = ''
			end
		}
	end

	# Called with the return value of each command
	def callback(rtrn)
		if rtrn
			rtrn = rtrn.join("\r\n") + "\r\n." if rtrn.respond_to?:join
			send_data(rtrn.to_s + "\r\n")
		end
	end

end

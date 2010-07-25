require 'eventmachine'

class Multi < Array
	def call(&cb)
		@responses = []
		@callback = cb
		self.each do |item|
			item.call { |*args|
				@responses << args
				self.check
			}
		end
		check
		EventMachine::DefaultDeferrable.new # Tell future that we've deferred
	end

	def check
		if @responses.length >= self.length
			@callback.call(@responses)
		end
	end
end

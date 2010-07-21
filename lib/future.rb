class Future
	# parent must respond to future_ready, which will be called when this future is ready
	def initialize(parent)
		@parent = parent
		@ready = false
	end

	def ready_with(value)
		@value = value
		@ready = true
		@parent.future_ready
		@value
	end

	def ready?
		@ready
	end

	def value
		@value
	end
end

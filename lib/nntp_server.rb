$: << File.dirname(__FILE__)
require 'simple_protocol_server'

class NNTPServer < SimpleProtocolServer
	# Hash of pattern => object backends must be in NNTP_BACKENDS
	# At least one pattern must match '', for when there is no current group
	def initialize
		super()
		@current_group = nil
		@current_article = nil
	end

	# Mandatory start to LIST OVERVIEW.FMT from http://tools.ietf.org/html/rfc3977#section-8.4
	OVERVIEW_FMT = ['subject', 'from', 'date', 'message-id', 'references', :bytes, :lines]

	def commands
		super.merge!({
			/^help$/i => method(:help),
			/^date$/i => method(:date),
			/^group\s+/ => method(:group),
			/^x?over\s*/ => method(:over) # Allow XOVER for historical reasons
		})
	end

	def backend
		BACKENDS.each do |pattern, backend|
			return backend if pattern === @current_group.to_s
		end
	end

	# http://tools.ietf.org/html/rfc3977#section-5.1
	def post_init
		super
		send_data "#{banner}\r\n"
	end

	def banner
		# 200 allowed, 201 prohibited, 400 temporary, 502 permanent
		'200 Service available, posting allowed'
	end

	# http://tools.ietf.org/html/rfc3977#section-7.2
	def help(data)
		['100 Help text follows (multi-line)'] +
		commands.keys.map { |key| key.inspect }
	end

	# http://tools.ietf.org/html/rfc3977#section-7.1
	def date(data)
		'111 ' + Time.now.utc.strftime('%Y%m%d%H%M%S') + ' Server date and time'
	end

	def group(data)
		return '501 Plase pass a group to select' unless data # http://tools.ietf.org/html/rfc3977#section-3.2.1
		@current_group = nil
		if (meta = backend.group(data))
			@current_group = data
			"211 #{meta[:total]} #{meta[:min]} #{meta[:max]} #{@current_group}"
		else
			'411 Group does not exist'
		end
	end

	# http://tools.ietf.org/html/rfc3977#section-8.3
	def over(data)
		data = @current_article if data == ''
		if data[0] != '<' && !data.index('@') # Message ID
			data = "<#{data}>" unless data[0] == '<'
			unless (rtrn = backend.over(:message_id => data))
				return '430 No article with that message-id'
			end
		elsif data.index('-') # Range
			return '412 No newsgroup selected' unless @current_group
			min, max = data.split(/-/, 2)
			max = 1/0.0 if max == ''
			unless (rtrn = backend.over(:range => (min.to_i..max)))
				return '423 No articles in that range'
			end
		else
			return '412 No newsgroup selected' unless @current_group
			unless (rtrn = backend.over(:article_num => data.to_i))
				return '420 Current article number is invalid'
			end
		end
		['224 Overview information follows (multi-line)'] +
		rtrn.map {|headers|
			(OVERVIEW_FMT + backend.overview_fmt).map {|header|
				headers[header].force_encoding('binary') # Make no assumptions about the data
			}.join("\t")
		}
	end
end

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
		{
			/^capabilities/i => method(:capabilities),
			/^mode reader/i  => lambda {|d| banner}, # http://tools.ietf.org/html/rfc3977#section-5.3
			/^quit/i         => method(:quit),
			/^group\s+/i     => method(:group),
			/^listgroup/i    => method(:listgroup),
			/^last/i         => method(:last),
			/^help/i         => method(:help),
			/^date/i         => method(:date),
			/^x?over\s*/i    => method(:over), # Allow XOVER for historical reasons
			/.*/             => lambda {|d| "500 Command not recognized" } # http://tools.ietf.org/html/rfc3977#section-3.2.1
		}
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

	# http://tools.ietf.org/html/rfc3977#section-5.2
	def capabilities(data)
		['101 Capability list follows (multi-line)', 'VERSION 2', 'IMPLEMENTATION XNNTP', 'READER', 'OVER MSGID']
	end

	# http://tools.ietf.org/html/rfc3977#section-5.4
	def quit(data)
		return '501 QUIT takes no arguments' if data.to_s != '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		send_data "205 Connection closing\r\n"
		close_connection_after_writing
	end

	# http://tools.ietf.org/html/rfc3977#section-6.1.1
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

	# http://tools.ietf.org/html/rfc3977#section-6.1.2
	def listgroup(data)
		group, range = data.split(/\s+/, 2)
		return '412 No newsgroup selected' unless group.to_s != '' || @current_group
		status = group(group.to_s == '' ? @current_group : group)
		return status if status.split(' ',2).first != '211'
		range = range.to_s == '' ? nil : parse_range(range)
		[status] + backend.listgroup(range)
	end

	# http://tools.ietf.org/html/rfc3977#section-6.1.3
	# LAST means previous
	def last(data)
		return '501 LAST takes no arguments' if data # http://tools.ietf.org/html/rfc3977#section-3.2.1
		return '412 No newsgroup selected' unless @current_group
		return '420 Current article number is invalid' unless @current_article
		if (rtrn = backend.last)
			@current_article = rtrn[:article_num]
			"223 #{rtrn[:article_num]} #{rtrn[:message_id]}"
		else
			'422 No previous article in this group'
		end
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
			unless (rtrn = backend.over(:range => parse_range(data)))
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

	protected
	def parse_range(string)
		return string.to_i unless string.index('-')
		min, max = data.split(/-/, 2)
		max = 1/0.0 if max == ''
		(min.to_i..max)
	end
end

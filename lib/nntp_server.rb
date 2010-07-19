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
	def self.readonly=(v); @readonly = v; end
	def self.readonly?; @readonly; end
	def readonly?; self.class.readonly?; end

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
			/^next/i         => method(:next),
			/^article/i      => method(:article),
			/^head/i         => method(:head),
			/^body/i         => method(:body),
			/^stat/i         => method(:stat),
			/^post/i         => method(:post),
			/^help/i         => method(:help),
			/^date/i         => method(:date),
			/^x?over\s*/i    => method(:over), # Allow XOVER for historical reasons
			/.*/             => lambda {|d| "500 Command not recognized" } # http://tools.ietf.org/html/rfc3977#section-3.2.1
		}
	end

	def backend(group=@current_group.to_s)
		BACKENDS.each do |pattern, backend|
			return backend if pattern === group
		end
	end

	# http://tools.ietf.org/html/rfc3977#section-5.1
	def post_init
		super
		send_data "#{banner}\r\n"
	end

	def banner
		# 200 allowed, 201 prohibited, 400 temporary, 502 permanent
		if self.readonly?
			'201 Service available, posting prohibited'
		else
			'200 Service available, posting allowed'
		end
	end

	# http://tools.ietf.org/html/rfc3977#section-5.2
	def capabilities(data)
		c = ['101 Capability list follows (multi-line)', 'VERSION 2', 'IMPLEMENTATION XNNTP', 'READER', 'OVER MSGID']
		c << 'POST' unless readonly?
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
		if (meta = backend(data).group(data))
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
		group = group.to_s == '' ? @current_group : group
		status = self.group(group)
		return status if status.split(' ',2).first != '211'
		range = range.to_s == '' ? nil : parse_range(range)
		[status] + backend(group).listgroup(range)
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

	# http://tools.ietf.org/html/rfc3977#section-6.1.4
	def next(data)
		return '501 LAST takes no arguments' if data # http://tools.ietf.org/html/rfc3977#section-3.2.1
		return '412 No newsgroup selected' unless @current_group
		return '420 Current article number is invalid' unless @current_article
		if (rtrn = backend.next)
			@current_article = rtrn[:article_num]
			"223 #{rtrn[:article_num]} #{rtrn[:message_id]}"
		else
			'422 No previous article in this group'
		end
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.1
	def article(data)
		if (rtrn = article_part(data, backend.method(:article))).is_a?String
			return rtrn
		end
		["220 #{rtrn[:article_num]} #{rtrn[:head][:message_id]} Article follows (multi-line)"] +
		rtrn[:head].map {|k,v| "#{k.to_s.gsub(/_/, '-').capitalize}: #{v}" } + [''] +
		rtrn[:body].gsub(/\r\n/, "\n").gsub(/\r/, "\n").gsub(/\r\n./, "\r\n..").split(/\n/)
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.2
	def head(data)
		if (rtrn = article_part(data, backend.method(:head))).is_a?String
			return rtrn
		end
		["221 #{rtrn[:article_num]} #{rtrn[:head][:message_id]} Headers follow (multi-line)"] +
		rtrn[:head].map {|k,v| "#{k.to_s.gsub(/_/, '-').capitalize}: #{v}" }
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.3
	def body(data)
		if (rtrn = article_part(data, backend.method(:body))).is_a?String
			return rtrn
		end
		["222 #{rtrn[:article_num]} #{rtrn[:head][:message_id]} Body follows (multi-line)"] +
		rtrn[:body].gsub(/\r\n/, "\n").gsub(/\r/, "\n").gsub(/\r\n./, "\r\n..").split(/\n/)
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.4
	def stat(data)
		if (rtrn = article_part(data, backend.method(:stat))).is_a?String
			return rtrn
		end
		["223 #{rtrn[:article_num]} #{rtrn[:head][:message_id]}"]
	end

	# http://tools.ietf.org/html/rfc3977#section-6.3.1
	def post(data)
		return '440 Posting not permitted' if readonly?
		@multiline = lambda {|data|
			head, body = data.split(/\r\n\r\n/, 2)
			# Headers are specced to be UTF-8, body may be different based on MIME
			headers = []
			head.force_encoding('utf-8').split(/\r\n/).each {|line|
				if line[0] =~ /\s/ && headers.last # folded header
					headers.last[1] += line
				else
					line = line.split(/:\s*/, 2)
					line[0] = line.first.downcase.gsub(/-/, '_').intern
					headers << line
				end
			}
			headers = Hash[headers] # Convert to hash (was array for ordering for folding)
			body.to_s.gsub!(/\r\n../, "\r\n.")
			return '441 Posting failed' unless headers[:newsgroup].to_s != ''
			success = false
			# We can have multiple backends, up to one per group, send to them all
			headers[:newsgroup].split(/,\s*/).each {|group|
				success ||= backend(group).post(:head => headers, :body => body)
			}
			# We succeeded if any backend did
			if success
				'240 Article received OK'
			else
				'441 Posting failed'
			end
		}
		'340 Send article to be posted'
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
				headers[header.downcase.gsub(/-/,'_').intern].force_encoding('binary') # Make no assumptions about the data
			}.join("\t")
		}
	end

	protected
	def article_part(data, method)
		if data[0] != '<' && !data.index('@') # Message ID
			unless (rtrn = method.cal(:message_id => data))
				return '430 No article with that message-id'
			end
		else
			return '412 No newsgroup selected' unless @current_group
			data = @current_article if data.to_s == ''
			return '420 Current article number is invalid' if data.to_s == ''
			unless (rtrn = method.call(:article_num => data.to_i))
				return '423 No article with that number'
			end
		end
		rtrn
	end

	def parse_range(string)
		return string.to_i unless string.index('-')
		min, max = data.split(/-/, 2)
		max = 1/0.0 if max == ''
		(min.to_i..max)
	end
end

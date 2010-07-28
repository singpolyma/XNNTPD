# encoding: utf-8
$: << File.dirname(__FILE__)
require 'simple_protocol_server'
require 'multi'
require 'time'
begin
	require 'html2markdown'
rescue LoadError
	warn 'Could not load html2markdown, text/plain conversion may suck.'
	class HTML2Markdown
		def initialize(s); @s = s; end
		def to_s; @s.gsub(/<[^>]+>/, "\n"); end
	end
end

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
			/^group\s*/i     => method(:group),
			/^listgroup\s*/i => method(:listgroup),
			/^last/i         => method(:last),
			/^next/i         => method(:next),
			/^article\s*/i   => method(:article),
			/^head\s*/i      => method(:head),
			/^body\s*/i      => method(:body),
			/^stat\s*/i      => method(:stat),
			/^post/i         => method(:post),
			/^ihave\s*/i     => method(:ihave),
			/^date/i         => method(:date),
			/^help/i         => method(:help),
			/^newgroups\s*/i => method(:newgroups),
			/^newnews\s*/i   => method(:newnews),
			/^list active\s*/i => method(:list_active),
			/^list newsgroups\s*/i => method(:list_newsgroups),
			/^list overview\.fmt/i => method(:list_overview_fmt),
			/^list headers/i => method(:list_headers),
			/^list\s*/i      => method(:list_active),
			/^x?over\s*/i    => method(:over), # Allow XOVER for historical reasons
			/^x?hdr\s*/i     => method(:hdr), # Allow XHDR for historical reasons
			/.*/             => lambda {|d| "500 Command not recognized" } # http://tools.ietf.org/html/rfc3977#section-3.2.1
		}
	end

	def backend(group=@current_group.to_s)
		BACKENDS.each do |pattern, backend|
			return backend if pattern === group
		end
		Class.new {
			def method_missing(*args)
				block_given? ? yield(nil) : nil
			end
			def method(*args); end
		}.new
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
		c = ['101 Capability list follows (multi-line)', 'VERSION 2',
		     'IMPLEMENTATION XNNTP', 'READER', 'OVER MSGID', 'NEWNEWS', 'HDR',
		     'LIST ACTIVE NEWSGROUPS OVERVIEW.FMT HEADERS']
		c << 'POST' << 'IHAVE' unless readonly?
	end

	# http://tools.ietf.org/html/rfc3977#section-5.4
	def quit(data)
		return '501 QUIT takes no arguments' if data.to_s != '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		send_data "205 Connection closing\r\n"
		close_connection_after_writing
	end

	# http://tools.ietf.org/html/rfc3977#section-6.1.1
	def group(data, &blk)
		return '501 Plase pass a group to select' if data.to_s == '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		@current_group = nil
		blk = lambda {|f, status| f.ready_with(status) } unless blk
		future { |f|
			backend(data).group(data) { |meta|
				if meta
					@current_group = data
					@current_article = meta[:min]
					blk.call(f, "211 #{meta[:total]} #{meta[:min]} #{meta[:max]} #{@current_group}")
				else
					blk.call(f, '411 Group does not exist')
				end
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.1.2
	def listgroup(data)
		group, range = data.split(/\s+/, 2)
		return '412 No newsgroup selected' unless group.to_s != '' || @current_group
		group = group.to_s == '' ? @current_group : group
		self.group(group) { |f, status|
			if status.split(' ',2).first != '211'
				f.ready_with(status)
			else
				range = range.to_s == '' ? nil : parse_range(range)
				if range.is_a?(Range) && range.begin > range.end
					f.ready_with([status])
				else
					backend(group).listgroup(group, range) {|list|
						f.ready_with([status] + list)
					}
				end
			end
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.1.3
	# LAST means previous
	def last(data)
		return '501 LAST takes no arguments' if data.to_s != '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		return '412 No newsgroup selected' unless @current_group
		return '420 Current article number is invalid' unless @current_article
		future { |f|
			backend.last(@current_group, @current_article) { |rtrn|
				if rtrn
					@current_article = rtrn[:article_number]
					f.ready_with("223 #{rtrn[:article_number]} #{rtrn[:message_id]}")
				else
					f.ready_with('422 No previous article in this group')
				end
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.1.4
	def next(data)
		return '501 NEXT takes no arguments' if data.to_s != '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		return '412 No newsgroup selected' unless @current_group
		return '420 Current article number is invalid' unless @current_article
		future { |f|
			backend.next(@current_group, @current_article) { |rtrn|
				if rtrn
					@current_article = rtrn[:article_number]
					f.ready_with("223 #{rtrn[:article_number]} #{rtrn[:message_id]}")
				else
					f.ready_with('421 No next article in this group')
				end
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.1
	def article(data)
		article_part(data, :article) { |rtrn|
			@current_article = rtrn[:article_number]
			["220 #{rtrn[:article_number]} #{rtrn[:head][:message_id]} Article follows (multi-line)"] +
			format_head(rtrn[:article_number], rtrn[:head]) + [''] +
			rtrn[:body].gsub(/\r\n/, "\n").gsub(/\r/, "\n").gsub(/\r\n./, "\r\n..").split(/\n/)
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.2
	def head(data)
		article_part(data, :head) { |rtrn|
			@current_article = rtrn[:article_number]
			["221 #{rtrn[:article_number]} #{rtrn[:head][:message_id]} Headers follow (multi-line)"] +
			format_head(rtrn[:article_number], rtrn[:head])
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.3
	def body(data)
		article_part(data, :body) { |rtrn|
			@current_article = rtrn[:article_number]
			["222 #{rtrn[:article_number]} #{rtrn[:head][:message_id]} Body follows (multi-line)"] +
			rtrn[:body].gsub(/\r\n/, "\n").gsub(/\r/, "\n").gsub(/\r\n./, "\r\n..").split(/\n/)
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.4
	def stat(data)
		article_part(data, :stat) { |rtrn|
			@current_article = rtrn[:article_number]
			["223 #{rtrn[:article_number]} #{rtrn[:head][:message_id]}"]
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.3.1
	def post(data)
		return '440 Posting not permitted' if readonly?
		@multiline = lambda {|data|
			head, body = parse_message(data)
			return '441 Posting failed' unless head[:newsgroups].to_s != ''
			success = false
			# We can have multiple backends, up to one per group, send to them all
			head[:newsgroups].split(/,\s*/).each {|group|
				success ||= backend(group).post(:head => head, :body => body)
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

	# http://tools.ietf.org/html/rfc3977#section-6.3.2
	def ihave(data)
		return '501 Please pass the message-id' if data.to_s == '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		return '436 Posting not permitted' if readonly?
		# Must check all backends for message-id
		if BACKENDS.inject(false) {|c, backend| c || backend.exists?(data) }
			return '435 Article not wanted'
		end
		@multiline = lambda {|data|
			head, body = parse_message(data)
			return '437 No Newsgroups header' unless head[:newsgroups].to_s != ''
			success = false
			# We can have multiple backends, up to one per group, send to them all
			head[:newsgroups].split(/,\s*/).each {|group|
				success ||= backend(group).ihave(:head => head, :body => body)
			}
			# We succeeded if any backend did
			if success
				'235 Article transferred OK'
			else
				'437 Transfer rejected; do not try again'
			end
		}
		'335 Send article to be transferred'
	end

	# http://tools.ietf.org/html/rfc3977#section-7.1
	def date(data)
		'111 ' + Time.now.utc.strftime('%Y%m%d%H%M%S') + ' Server date and time'
	end

	# http://tools.ietf.org/html/rfc3977#section-7.2
	def help(data)
		['100 Help text follows (multi-line)'] +
		commands.keys.map { |key| key.inspect }
	end

	# http://tools.ietf.org/html/rfc3977#section-7.3
	def newgroups(data)
		date, time, gmt = data.split(/\s+/, 3)
		return '501 Use: yyyymmdd hhmmss' if date.to_s == '' || time.to_s == '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		datetime = parse_date(date, time)
		return '501 Use: yyyymmdd hhmmss' unless datetime # http://tools.ietf.org/html/rfc3977#section-3.2.1
		# Get new groups from all backends
		future { |f|
			each_backend { |backend, &cb| backend.newgroups(datetime, &cb) }.call { |groups|
				f.ready_with(['231 List of new groups follows (multi-line)'] + groups.flatten.compact)
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-7.4
	def newnews(data)
		wildmats, date, time, gmt = data.split(/\s+/, 4)
		return '501 Use: yyyymmdd hhmmss' if date.to_s == '' || time.to_s == '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		datetime = parse_date(date, time)
		return '501 Use: yyyymmdd hhmmss' unless datetime # http://tools.ietf.org/html/rfc3977#section-3.2.1
		wildmats = parse_wildmat(wildmats)
		# Get new news from all backends
		# TODO: can we match the passed wildmats against backend patterns and only ask relevant backends?
		future { |f|
			each_backend { |backend, &cb| backend.newnews(wildmats, datetime, &cb) }.call { |msgids|
				f.ready_with(['230 List of new articles follows (multi-line)'] + msgids.flatten.compact)
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-7.6.3
	def list_active(data)
		data = parse_wildmat(data)
		future { |f|
			each_backend { |backend, &cb| backend.list(data, &cb) }.call { |groups|
				f.ready_with(['215 List of groups follows (multi-line)'] +
				groups.flatten.compact.map { |group|
					"#{group[:newsgroup]} #{group[:max]} #{group[:min]} #{group[:readonly] ? 'n' : (group[:moderated] ? 'm' : 'y')}"
				})
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-7.6.6
	def list_newsgroups(data)
		data = parse_wildmat(data)
		future { |f|
			each_backend { |backend, &cb| backend.list(data, &cb) }.call { |groups|
				f.ready_with(['215 List of groups follows (multi-line)'] +
				groups.flatten.compact.map { |group|
					"#{group[:newsgroup]}\t#{group[:title]}"
				})
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-8.4
	def list_overview_fmt(data)
		['215 Order of fields from OVER command'] +
		(OVERVIEW_FMT + (backend.overview_fmt || [])).map {|i|
			if i.is_a?String
				i.capitalize.gsub(/_/, '-') + "#{':full' unless OVERVIEW_FMT.index(i)}"
			else
				i.inspect
			end
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-8.6
	def list_headers(data)
		['215 Field list follows (multi-line)', ':', ':bytes', ':lines']
	end

	# http://tools.ietf.org/html/rfc3977#section-8.3
	def over(data)
		data = @current_article.to_s if data == ''
		if data[0] == '<' || data.index('@') # Message ID
			data = "<#{data}>" unless data[0] == '<'
			args = {:message_id => data}
			error = '430 No article with that message-id'
		elsif data.index('-') # Range
			return '412 No newsgroup selected' unless @current_group
			args = {:article_number => parse_range(data)}
			error = '423 No articles in that range'
		else
			return '412 No newsgroup selected' unless @current_group
			args = {:article_number => data.to_i} # Backends see this as a range of type Fixnum
			error = '420 Current article number is invalid'
		end
		future {|f|
			backend.over(@current_group, args) {|rtrn|
				if rtrn
					f.ready_with(['224 Overview information follows (multi-line)'] +
					rtrn.map {|headers|
						headers[:article_number].to_i.to_s + "\t" +
						(OVERVIEW_FMT + (backend.overview_fmt || [])).map {|header|
							# Make no assumptions about the data
							begin
								format_head_value(headers[header.to_s.downcase.gsub(/-/,'_').intern])
							rescue Exception
								''
							end
						}.join("\t")
					})
				else
					f.ready_with(error)
				end
			}
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-8.5
	def hdr(data)
		field, range = data.split(/\s+/, 2)
		field = field.to_s.downcase.gsub(/-/, '_').intern
		range = @current_article.to_s if range.to_s == ''
		return '420 Current article number is invalid' if range.to_s == ''
		if range[0] == '<' || range.index('@') # Message ID
			future {|f|
				each_backend { |backend, &cb|
					backend.hdr(@current_group, field, :message_id => range, &cb)
				}.call { |heads|
					head = heads.flatten.compact.first || {}
					f.ready_with(['225 Headers follow (multi-line)', "0 #{format_head_value(head[field])}"])
				}
			}
		else # Range
			return '412 No newsgroup selected' unless @current_group
			range = parse_range(range)
			future {|f|
				backend(@current_group).hdr(@current_group, field, :article_number => range) {|hdrs|
					f.ready_with(['225 Headers follow (multi-line)'] + ((hdrs || []).compact.map {|head|
						next if head[field].to_s == ''
						"#{head[:article_number]} #{format_head_value(head[field])}"
					}))
				}
			}
		end
	end

	protected
	def future
		f = Future.new(self)
		r = yield(f)
		if r.is_a?(EventMachine::Deferrable)
			f
		else
			r
		end
	end

	def each_backend(&blk)
		request = Multi.new
		BACKENDS.each { |pattern, backend|
			request << lambda {|&cb| blk.call(backend, &cb) }
		}
		request
	end

	def format_head_value(v)
		v = v.utc.rfc2822 if v.is_a?Time
		v = v.join(', ') if v.respond_to?:join
		v.to_s.encode('utf-8')
	end

	def format_head(article_number, hash)
		hash[:in_reply_to] = hash[:references].last if hash[:references] && !hash[:in_reply_to]
		if hash[:newsgroups].index(@current_group)
			hash[:xref] = "#{HOST} #{@current_group}:#{article_number.to_i}"
		else
			# If asking for a message ID from another group, choose some group and we don't know the article number
			hash[:xref] = "#{HOST} #{hash[:newsgroups].first}:0"
		end
		hash[:path] = HOST unless hash[:path]
		hash.map {|k,v|
			next unless v
			"#{k.to_s.gsub(/_/, '-').capitalize}: #{format_head_value(v)}".encode('utf-8')
		}
	end

	def fix_content_type(msg)
		if !msg[:head][:content_type]
			msg[:head][:content_type] = 'text/plain; charset=utf-8'
		elsif msg[:head][:content_type] =~ /^text\/html\b/i
			# TODO: real MIME
			msg[:head][:content_type] = 'text/plain; charset=utf-8'
			msg[:body] = HTML2Markdown.new(msg[:body]).to_s if msg[:body]
		end
		msg
	end

	def article_part(data, method)
		if data[0] == '<' || data.index('@') # Message ID
			future { |f|
				each_backend { |backend, &cb|
					backend.send(method, @current_group, :message_id => data, &cb)
				}.call { |rtrn|
					rtrn = rtrn.flatten.compact.first
					if rtrn
						f.ready_with(yield fix_content_type(rtrn))
					else
						f.ready_with('430 No article with that message-id')
					end
				}
			}
		else
			method = backend.method(method)
			return '412 No newsgroup selected' unless @current_group && method
			data = @current_article if data.to_s == ''
			return '420 Current article number is invalid' if data.to_s == ''
			future { |f|
				method.call(@current_group, :article_number => data.to_i) { |rtrn|
					if rtrn
						f.ready_with(yield fix_content_type(rtrn))
					else
						f.ready_wih('423 No article with that number')
					end
				}
			}
		end
	end

	def parse_message(data)
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
		[headers, body]
	end

	def parse_wildmat(wildmat)
		wildmat = '*' if wildmat.to_s == ''
		wildmat = wildmat.split(/,/).map {|wildmat|
			wildmat.gsub!(/\./, '\.')
			wildmat.gsub!(/\?/, '.')
			wildmat.gsub!(/\*/, '.*')
			if wildmat[0] == '!'
				[Regexp.new("^#{wildmat[1..-1]}$"), false]
			else
				[Regexp.new("^#{wildmat}$"), true]
			end
		}
		def wildmat.match(str)
			self.reverse.each { |(pattern, value)|
				return value if str =~ pattern
			}
			false
		end
		wildmat
	end

	def parse_date(date, time)
		if date.length <= 6 # 2-digit year
			if date[0..1].to_i <= Time.now.year.to_s[2..-1].to_i
				date = Time.now.year.to_s[0..1] + date
			else
				date = (Time.now.year.to_s[0..1].to_i - 1).to_s + date
			end
		end
		begin
			Time.parse(date + ' ' + time + '+0000').utc # Always assume UTC
		rescue ArgumentError # When datetime is invalid
			nil
		end
	end

	def parse_range(string)
		return string.to_i unless string.index('-')
		min, max = string.split(/-/, 2)
		max = 1/0.0 if max == ''
		(min.to_i..max.to_f)
	end
end

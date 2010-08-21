# encoding: utf-8
$: << File.dirname(__FILE__)

require 'mail'

require 'simple_protocol_server'
require 'multi'
require 'util'

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
	# Plus Xref header, because many newsreaders want that
	OVERVIEW_FMT = ['subject', 'from', 'date', 'message-id', 'references', :bytes, :lines, 'xref']

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
			["220 #{rtrn[:article_number]} #{rtrn[:head][:message_id]} Article follows (multi-line)", rtrn[:mime].encoded]
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.2
	def head(data)
		article_part(data, :head) { |rtrn|
			@current_article = rtrn[:article_number]
			["221 #{rtrn[:article_number]} #{rtrn[:head][:message_id]} Headers follow (multi-line)", rtrn[:mime].header.encoded.sub(/\r\n$/, '')]
		}
	end

	# http://tools.ietf.org/html/rfc3977#section-6.2.3
	def body(data)
		article_part(data, :body) { |rtrn|
			@current_article = rtrn[:article_number]
			["222 #{rtrn[:article_number]} #{rtrn[:head][:message_id]} Body follows (multi-line)", rtrn[:mime].body.encoded]
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
			m = Mail::Message.new(data)
			return '441 Posting failed' if m[:newsgroups].to_s.to_s == '' || (m.multipart? && m.text_part.to_s.strip == '')
			m[:content_type] = 'text/plain; charset=utf-8' unless m[:content_type]
			return '441 Posting failed' if (!m.multipart? && m[:content_type].to_s !~ /^text\/plain\b/)
			if m[:path]
				m[:path] = "#{HOST}!#{m[:path]}"
			else
				m[:path] = HOST
			end
			m[:received_from] = @peer[:ip]
			# m[:xref] = nil This can delete all custom headers
			begin
				m[:in_reply_to] = m[:references].message_ids.last if m[:references] && !m[:in_reply_to]
				m[:references] = m[:in_reply_to].to_s if m[:in_reply_to] && (!m[:references] || m[:references].message_ids.last != m[:in_reply_to].to_s)
			rescue Exception
				return '411 Posting failed... references/in-reply-to malformed'
			end
			future { |f|
				# Handle control messages
				if m[:supersedes].to_s.to_s != ''
					m[:control] = "cancel #{m[:supersedes].decoded}"
				end
				if m[:control].to_s.to_s != ''
					r = control_message(m, :post) {|r|
						if r.is_a?(String)
							f.ready_with("441 #{r}")
						elsif r
							f.ready_with('240 Control message transfer OK')
						else
							f.ready_with('441 Control message rejected')
						end
					}
					next r unless m[:supersedes] # Continue processing supersedes messages
				end
				# We can have multiple backends, up to one per group, send to them all
				request = Multi.new
				m[:newsgroups].decoded.split(/,\s*/).each { |group|
					request << lambda { |&cb|
						backend(group).moderated?(group) {|moderated|
							if moderated
								backend(group).owner(group) {|owner|
									if owner[:nntp].to_s == '' || \
									   (owner[:nntp] = URI::parse("nntp://#{owner[:nntp]}")).host != HOST || \
									   (owner[:nntp].port || 119) != PORT
										LOG.info "Forwarding moderated message on to #{owner.inspect}"
										if owner[:nntp].to_s != '' # POST message to "primary" server
											m.transport_encoding = '8bit'
											m.ready_to_send!
											q = DB.query("INSERT INTO messages (message_id, post_peer, encoded) VALUES
											         ('#{Mysql::escape_string(m[:message_id].decoded)}',
											          '#{Mysql::escape_string(owner[:nntp].to_s)}',
											          '#{Mysql::escape_string(m.encoded)}')") {
												cb.call(true)
											}
											q.errback {|e| cb.call(false) }
										else
											begin
												m.transport_encoding = '7bit'
												m[:to] = owner[:mailto]
												m.delivery_method :sendmail
												m.deliver!
												cb.call(true)
											rescue Exception
												cb.call(false)
											end
										end
									else
										backend(group).post(m, &cb)
									end
								}
							else
								backend(group).post(m, &cb)
							end
						}
					}
				}
				return '411 No groups match' unless request.length > 0
				request.call { |rtrn|
					# We succeeded if any backend did
					if rtrn.flatten.compact.inject(false) {|c,v| c || v}
						f.ready_with('240 Article received OK')
					else
						f.ready_with('441 Posting failed')
					end
				}
			}
		}
		'340 Send article to be posted'
	end

	# http://tools.ietf.org/html/rfc3977#section-6.3.2
	def ihave(data)
		return '501 Please pass the message-id' if data.to_s == '' # http://tools.ietf.org/html/rfc3977#section-3.2.1
		return '436 Posting not permitted' if readonly?
		# Must check all backends for message-id
		future {|f|
			each_backend { |backend, &cb| backend.stat(nil, :message_id => data, &cb) }.call {|result|
				if result.flatten.compact.length > 0
					f.ready_with('435 Article not wanted')
				else
					@multiline = lambda {|data|
						m = Mail::Message.new(data)
						# Handle control messages
						if m[:supersedes].to_s.to_s != ''
							m[:control] = "cancel #{m[:supersedes].decoded}"
						end
						if m[:control].to_s.to_s != ''
							r = control_message(m, :ihave) {|r|
								if r.is_a?(String)
									f.ready_with("437 #{r}")
								elsif r
									f.ready_with('235 Control message transfer OK')
								else
									f.ready_with('437 Control message rejected')
								end
							}
							next r unless m[:supersedes] # Continue processing Supersedes messages
						end
						return '437 No Newsgroups header' if m[:newsgroups].to_s == ''
						return '437 No text/plain part' if (m.multipart? && m.text_part.to_s.strip == '')
						if m[:path]
							m[:path] = "#{HOST}!#{m[:path]}"
						else
							m[:path] = HOST
						end
						m[:received_from] = @peer[:ip]
						# m[:xref] = nil This can delete all custom headers
						begin
							m[:in_reply_to] = m[:references].message_ids.last if m[:references] && !m[:in_reply_to]
							m[:references] = m[:in_reply_to].to_s if m[:in_reply_to] && (!m[:references] || m[:references].message_ids.last != m[:in_reply_to].to_s)
						rescue Exception
							return '437 Transfer failed... references/in-reply-to malformed'
						end
						# We can have multiple backends, up to one per group, send to them all
						future {|f|
							request = Multi.new
							m[:newsgroups].to_s.split(/,\s*/).each {|group|
								request << lambda { |&cb|
									backend(group).moderated?(group) {|moderated|
										if moderated
											backend(group).owner(group) { |owner|
												pgpverify(m, owner[:pgpkey],
													['Newsgroups', 'Subject', 'Message-ID', 'Date']) { |verified|
													if verified
														backend(group).ihave(m, &cb)
													else
														cb.call(false)
													end
												}
											}
										else
											backend(group).ihave(m, &cb)
										end
									}
								}
							}
							# We succeeded if any backend did
							request.call {|rtrn|
								if rtrn.flatten.compact.inject(false) {|c,v| c || v}
									f.ready_with('235 Article transferred OK')
								else
									f.ready_with('437 Transfer rejected; do not try again')
								end
							}
						}
					}
					f.ready_with('335 Send article to be transferred')
				end
			}
		}
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
				i.to_s.capitalize.gsub(/_/, '-') + ':' + "#{'full' unless OVERVIEW_FMT.index(i)}"
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
						headers = fixup_headers(headers[:article_number], headers)
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
					head = hash_to_mime(:head => heads.flatten.compact.first || {})[:mime]
					f.ready_with(['225 Headers follow (multi-line)', "0 #{format_head_value(head[field])}"])
				}
			}
		else # Range
			return '412 No newsgroup selected' unless @current_group
			range = parse_range(range)
			future {|f|
				backend(@current_group).hdr(@current_group, field, :article_number => range) {|hdrs|
					f.ready_with(['225 Headers follow (multi-line)'] + ((hdrs || []).compact.map {|head|
						head = hash_to_mime(:head => head)[:mime]
						next if head[field].to_s == ''
						"#{head[:article_number]} #{format_head_value(head[field])}"
					}))
				}
			}
		end
	end

	protected
	include Util

	def control_message(m, method=:post, &cb)
		command = m[:control].decoded.strip.split(/\s+/)
		process = lambda { |key| begin
			raise 'OpenPGP signature did not verify' unless key
			case command[0].downcase
				when 'newgroup'
					nntp = begin
						nntp = URI::parse(m[:uri].decoded)
						if nntp.scheme.downcase == 'nntp'
							"#{nntp.host}#{":#{nntp.port}" if nntp.port && nntp.port != 119}/#{command[1]}"
						else
							nil
						end
					rescue Exception
					end
					title = if (title = m.body.decoded.scan(/^\s*#{command[1]}\s+(.+)$/i))
						title[0][0] if title[0]
					end
					backend(command[1]).newgroup(command[1],
						{:nntp     => nntp,
						:mailto    => m[:from].addresses.first,
						:title     => title,
						:moderated => (command[2].to_s.downcase == 'moderated'),
						:pgpkey    => key.to_s},
						&cb)
				when 'rmgroup'
					backend(command[1]).rmgroup(command[1], &cb)
				when 'cancel'
					each_backend { |backend, &cb| backend.cancel(command[1], &cb) }.call { |r|
						cb.call(r.flatten.inject(false) { |c,v| c || v })
					}
				else
					raise 'Unrecognized command in control message'
			end
			# No exceptions were thrown if we get here
			DB.query("INSERT INTO messages (message_id,encoded) VALUES(
			          '#{Mysql::escape_string(m[:message_id])}',
			          '#{Mysql::escape_string(m.encoded)}')")
			EventMachine::DefaultDeferrable.new
		rescue Exception
			cb.call($!.message)
			EventMachine::DefaultDeferrable.new
		end }

		if command[0].downcase == 'cancel'
			# TODO: Also PGPVERIFY to be from key that signed original message using PGP/MIME (if any)
			# TODO: This also happens on Supersedes, in that case allow PGP/MIME matching original
			return cb.call('No X-PGP-Sig found')
		elsif m[:x_pgp_sig]
			backend(command[1]).owner(command[1]) {|owner|
				if owner && owner[:pgpkey]
					pgpverify(m, owner[:pgpkey], &process)
				elsif command[0].downcase == 'newgroup'
					pgpverify(m, &process)
				else
					process.call(nil)
				end
			}
		else
			return cb.call('No X-PGP-Sig found')
		end

		EventMachine::DefaultDeferrable.new
	end

	def article_part(data, method)
		if data[0] == '<' || data.index('@') # Message ID
			future { |f|
				each_backend { |backend, &cb|
					backend.send(method, @current_group, :message_id => data, &cb)
				}.call { |rtrn|
					rtrn = rtrn.flatten.compact.first
					if rtrn
						f.ready_with(yield hash_to_mime(rtrn))
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
						f.ready_with(yield hash_to_mime(rtrn))
					else
						f.ready_wih('423 No article with that number')
					end
				}
			}
		end
	end

end

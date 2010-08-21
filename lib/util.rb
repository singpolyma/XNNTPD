# encoding: utf-8
$: << File.dirname(__FILE__)

require 'time'

require 'mail'
require 'em-http'
require 'openpgp'
begin
	require 'html2markdown'
rescue LoadError
	warn 'Could not load html2markdown, text/plain conversion may suck.'
	class HTML2Markdown
		def initialize(s); @s = s; end
		def to_s; @s.gsub(/<[^>]+>/, "\n"); end
	end
end

require 'multi'

module Util
	module_function

	def new_article(m)
		DB.query("INSERT INTO messages (message_id,newsgroup) VALUES(
		          '#{Mysql::escape_string(m[:message_id])}',
		          '#{Mysql::escape_string(m[:newsgroup])}')") if m[:message_id]
	end

	def backend(group)
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
		hash.map {|k,v|
			next unless v
			"#{k.to_s.gsub(/_/, '-').capitalize}: #{format_head_value(v)}".encode('utf-8')
		}
	end

	# This works outside of a mixin, but the Xref header is better in mixin
	def fixup_headers(article_number, headers)
		headers[:in_reply_to] = headers[:references].last if headers[:references] && !headers[:in_reply_to]
		if headers[:newsgroups] # Don't neet this when requesting body
			if headers[:newsgroups].index(@current_group)
				headers[:xref] = "#{HOST} #{@current_group}:#{article_number.to_i}"
			else
				# If asking for a message ID from another group, choose some group and we don't know the article number
				headers[:xref] = "#{HOST} #{headers[:newsgroups].first}:0"
			end
		end
		headers[:path] = HOST unless headers[:path]
		headers
	end

	def hash_to_mime(msg)
		msg[:body] = '' unless msg[:body]
		msg[:head] = fixup_headers(msg[:article_number], msg[:head])

		charset = msg[:body].encoding.name
		msg[:head][:content_type] = "text/plain; charset=#{charset}" if !msg[:head][:content_type]
		m = Mail::Message.new(msg[:head].inject({}) {|c, (k,v)|
			v = v.join(', ') if v.respond_to?:join
			c[k] = v.to_s
			c
		})
		if msg[:head][:content_type] =~ /^text\/html\b/i
			m.text_part {
				content_type "text/plain; charset=#{charset}"
				body HTML2Markdown.new(msg[:body]).to_s
			}
			m.html_part {
				content_type "text/html; charset=#{charset}"
				body msg[:body]
			}
			boundary = "--==_mimepart_#{msg[:head][:message_id]}_#{msg[:article_number]}"[0..70]
			m.header['content-type'].parameters[:boundary] = boundary
			m.body.boundary = boundary
		else
			m.body = msg[:body]
		end
		m.transport_encoding = '8bit'
		m.ready_to_send! # Sets up sub components, etc
		msg.merge({:mime => m})
	end

	# PGPVERIFY <ftp://ftp.isc.org/pub/pgpcontrol/FORMAT>
	# yields nil (no key found), false (signature verify failed), or OpenPGP::Packet::PublicKey
	def pgpverify(m, keys=nil, required_headers=['From', 'Control', 'URI'], header=:x_pgp_sig)
		# TODO: cache keys
		# TODO: check self-sig and expiration/revocation
		version, headers, sig = m[header].decoded.split(/\s+/,3)
		headers = headers.split(',')
		return yield false unless required_headers.inject(true) {|c, h| c && headers.grep(/^#{h}$/i).length > 0}

		sig.sub!(/=.+$/,'') # Chop off ASCII armour checksum if present
		sig = sig.unpack('m').first # unpack ignores garbage like whitespace
		sig = OpenPGP::Message.parse(sig).signature_and_data[0]

		expires = sig.hashed_subpackets.select {|p|
			p.is_a?(OpenPGP::Packet::Signature::SignatureExpirationTime)
		}.map {|p| p.data}.sort.first

		if expires && expires.to_i > 0
			created = sig.hashed_subpackets.select {|p|
				p.is_a?(OpenPGP::Packet::Signature::SignatureCreationTime)
			}.map {|p| p.data}.sort.reverse.first
			return yield false unless created && (created.to_i + expires.to_i) < Time.now.to_i
		end

		finish = proc { |keys|
			keys = keys.select {|p| p.algorithm == 1 && p.fingerprint =~ /#{sig.issuer}$/i } if keys # We only support RSA for now
			if keys
				DB.query("INSERT IGNORE INTO `keys` VALUES('#{keys.first.fingerprint.upcase}', '#{Mysql::escape_string(keys.first.to_s.force_encoding('binary'))}')")
				head = headers.map {|h| "#{h}: #{m[h] ? m[h].decoded : ''}\r\n"}.join
				data = OpenPGP::Packet::LiteralData.new(:format => :u, :data =>
					"X-Signed-Headers: #{headers.join(',')}\r\n#{head}\r\n#{m.body.decoded}\r\n")
				if OpenPGP::Engine::OpenSSL::RSA.new(keys.first).verify(OpenPGP::Message.new(sig, data))
					yield keys.first
				else
					yield false
				end
			else
				yield nil
			end
		}

		if keys
			finish.call(keys)
		else
			DB.query("SELECT `key` FROM `keys` WHERE fingerprint LIKE '%#{sig.issuer.upcase}'") { |r|
				if key = r.fetch_row
					finish.call(OpenPGP::Message.parse(key[0]))
				else
					each_keyserver((sig.hashed_subpackets.map {|p|
						p.is_a?(OpenPGP::Packet::Signature::PreferredKeyServer) ? p.body : nil
					}.compact) + ["hkp://#{HKP_KEYSERVER}"], sig.issuer, &finish)
				end
			}
		end
	rescue Exception
		yield nil
	end

	def each_keyserver(keyservers, issuer, &cb)
		return cb.call(nil) unless keyservers.length > 0
		if (keyserver = keyservers.shift) =~ /^http/i
			http = EventMachine::HttpRequest.new(keyserver).get :redirects => 1
		elsif keyserver =~ /^hkp/i
			http = EventMachine::HttpRequest.new( \
				"http://#{keyserver.sub(/^hkp:?\/*/,'').sub(/\/*$/,'')}:11371/pks/lookup").get \
				:redirects => 1, :query => {:search => "0x#{issuer}",
					:op => 'get', :exact => 'on', :options => 'mr'}
		else
			each_keyserver(keyservers, issuer, &cb) # Skip unrecognized keyserver type
		end
		http.callback {
			begin
				keys = OpenPGP::Message.parse(OpenPGP::dearmor(http.response)).select {|p|
					p.is_a?(OpenPGP::Packet::PublicKey) && p.fingerprint =~ /#{issuer}$/i
				}
				if keys.length > 0 # Found some keys, send them back
					cb.call(keys)
				else
					each_keyserver(keyservers, issuer, &cb)
				end
			rescue Exception
				each_keyserver(keyservers, issuer, &cb)
			end
		}
		http.errback {
			each_keyserver(keyservers, issuer, &cb)
		}
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

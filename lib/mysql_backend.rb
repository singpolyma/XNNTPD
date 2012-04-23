# encoding: utf-8
require 'mysql2/em'
require 'time'
require 'util'
require 'connection_pool'

class MysqlBackend
	def initialize(config)
		@db = EventMachine::ConnectionPool.new do
			Mysql2::EM::Client.new(config[:db].merge(:encoding => 'utf8'))
		end
		@readonly = config[:readonly]
		query("
			CREATE TABLE IF NOT EXISTS newsgroups(
			newsgroup CHAR(150), mailto CHAR(255), nntp CHAR(255),
			title CHAR(255), moderated TINYINT, pgpkey BLOB
			)")
		query("
			CREATE TABLE IF NOT EXISTS meta(
			newsgroup CHAR(150), article_number INT, message_id CHAR(150),
			approved TINYINT,
			CONSTRAINT UNIQUE INDEX group_number (newsgroup, article_number),
			CONSTRAINT UNIQUE INDEX group_id (newsgroup, message_id)
			)")
		query("
			CREATE TABLE IF NOT EXISTS articles(
			message_id CHAR(150) PRIMARY KEY,
			subject CHAR(255), `from` CHAR(255), date INT, `references` CHAR(255),
			headers TEXT, body TEXT
			)")
	end

	# Called to create a new group with the name in g
	# args may contain any of: nntp (HOST/group_name), mailto (owner email),
	# title, moderated (bool), pgpkey (binary)
	def newgroup(g, args)
		query("
			INSERT INTO newsgroups
			VALUES ('%s', '%s', '%s', '%s', %d, '%s')
		", g, args[:mailto], args[:nntp], args[:title],
		args[:moderated] ? 1 : 0, args[:pgpkey]) {
			yield true
		}
	end

	def rmgroup(g)
		query("DELETE FROM newsgroups WHERE newsgroup='%s'", g) {
			yield true
		}
	end

	def owner(g)
		query("
			SELECT mailto, nntp, pgpkey
			FROM newsgroups
			WHERE newsgroup='%s'
			LIMIT 1", g) {|result|
			if (result = result.first)
				yield format_hash(result)
			else
				yield nil
			end
		}
	end

	def moderated?(g)
		query("
			SELECT moderated
			FROM newsgroups
			WHERE newsgroup='%s'
			LIMIT 1", g) {|result|
			if (result = result.first)
				yield result['moderated'].to_i != 0
			else
				yield nil
			end
		}
	end

	def group(g, &blk)
		get_group_stats g, &blk
	end

	def listgroup(g, range, &blk)
		query("
			SELECT
				article_number
			FROM
				meta
			WHERE
				approved=1 AND
				newsgroup='%s' #{range_to_sql('article_number', range)}
			ORDER BY article_number", g) { |result|
			list = []
			result.each {|row| list << row['article_number'] }
			blk.call(list)
		}
	end

	def last(g, current)
		query("SELECT
			article_number
		FROM
			meta
		WHERE
			approved=1 AND
			newsgroup='%s' AND article_number < %d
		ORDER BY
			article_number DESC
		LIMIT 1", g, current.to_i) { |result|
			yield(if (result = result.first)
				{:article_number => result['article_number']}
			end)
		}
	end

	def next(g, current)
		query("SELECT
			article_number
		FROM
			meta
		WHERE
			approved=1 AND
			newsgroup='%s' AND article_number > %d
		ORDER BY
			article_number ASC
		LIMIT 1", g, current.to_i) { |result|
			yield(if (result = result.first)
				{:article_number => result['article_number']}
			end)
		}
	end

	def article(g, args, &blk)
		one_article(g, args, &blk)
	end

	def head(g, args, &blk)
		one_article(g, args, true, false, &blk)
	end

	def body(g, args, &blk)
		one_article(g, args, false, true, &blk)
	end

	def stat(g, args, &blk)
		one_article(g, args, false, false, &blk)
	end

	def post(m)
		# We can assume that the message does belong to one or more of our groups if it's here
		# Messages that fail policy checks should be sent elsewhere
		# m.header.fields.map
		m[:message_id] ||= '<' + Mail::random_tag + '@' + HOST + '>'
		Util::new_article(:message_id => m[:message_id].decoded,
		                  :newsgroup => m[:newsgroups].decoded.split(/,\s*/).first)
		headers = m.header.fields.map { |head|
			unless [:message_id, :'message-id', :subject, :from, :date, :references].index(head.name.to_s.downcase.intern)
				"#{head.name}: #{head.decoded}"
			end
		}.compact.join("\r\n")
		groups = m[:newsgroups].to_s.split(/,\s*/).map {|n| prepare("'%s'", n)}.join(', ')
		query("
			SELECT
				newsgroup,
				IF(MAX(article_number) IS NULL, 0, MAX(article_number)) AS max,
				moderated, nntp
			FROM newsgroups LEFT JOIN meta USING (newsgroup)
			WHERE newsgroup IN (#{groups})
			GROUP BY newsgroup") {|nums|
			request = Multi.new
			nums.each {|num|
				# Skip any moderated groups we don't manage
				next if num['moderated'].to_i != 0 && num['nntp'].split('/',2)[0] != HOST + (PORT != 119 ? ":#{PORT}" : '')
				time = m[:date].to_s != '' ? Time.parse(m[:date].to_s) : Time.now
				# XXX: There's an ugly race condition on the article number...
				request << lambda {|&cb|
					query("INSERT INTO meta VALUES('%s', %d, '%s', %d)",
					num['newsgroup'], num['max'].to_i + 1, m[:message_id],
					num['moderated'].to_i != 0 ? 0 : 1) {
						cb.call
					}
				}
				request << lambda {|&cb|
					query("INSERT INTO articles VALUES('%s', '%s', '%s', %d, '%s', '%s', '%s')",
					m[:message_id], m[:subject], m[:from], time,
					m[:references], headers, m.body) {
						cb.call
					}
				}
			}
			if request.length < 1
				yield false
			else
				request.call { yield true }
			end
		}
	end

	def ihave(m, &cb)
		post(m, &cb)
	end

	def newgroups(datetime)
		query("
			SELECT
				newsgroup, MIN(date) as start
			FROM articles LEFT JOIN meta USING(message_id)
			WHERE approved=1 AND date >= %d
			GROUP BY newsgroup", datetime) { |result|
			yield ((result.to_a || []).map {|h| h['newsgroup']})
		}
	end

	def newnews(wildmats, datetime)
		query("
			SELECT newsgroup, message_id
			FROM articles LEFT JOIN meta USING(message_id)
			WHERE approved=1 AND date >= %d", datetime) { |result|
			yield ((result.to_a || []) \
				.select {|h| wildmats.match(h['newsgroup'])} \
				.map {|h| h['message_id']})
		}
	end


	def list(wildmat)
		get_group_stats(nil, true) {|groups|
			yield (groups.select { |g| wildmat.match(g[:newsgroup]) }.map {|g|
					g.merge!(:readonly => @readonly)
				})
		}
	end

	def overview_fmt
		[] # No extra fields from this backend
	end

	def over(g, args)
		stmt = "SELECT article_number, message_id, subject, `from`, date, `references`, headers, body
		        FROM articles LEFT JOIN meta USING(message_id) WHERE approved=1"
		stmt << prepare(" AND newsgroup='%s'", g) if g
		stmt << prepare(" AND message_id='%s'", args[:message_id]) if args[:message_id]
		stmt << range_to_sql('article_number', args[:article_number])
		query(stmt) {|result|
			yield result.map {|h| format_hash(h, g)}
		}
	end

	def hdr(g, field, args, &cb)
		over(g, args) { |h|
			h = h.first
			if h
				yield h.merge!(Hash[h[:headers].to_s.split(/\r?\n/).map {|v| v.split(/:\s*/)}])
			else
				yield nil
			end
		}
	end

	protected

	def one_article(g, args, head=true, body=true)
		stmt = "SELECT article_number, message_id
		        #{', subject, `from`, date, `references`, headers' if head}
		        #{', body' if body}
		        FROM articles LEFT JOIN meta USING(message_id) WHERE approved=1"
		stmt << prepare(" AND newsgroup='%s'", g) if g
		stmt << prepare(" AND message_id='%s'", args[:message_id]) if args[:message_id]
		stmt << range_to_sql('article_number', args[:article_number])
		stmt << ' LIMIT 1'

		query(stmt) {|result|
			if (result = result.first)
				h = {:head => format_hash(result, g)}
				h[:article_number] = h[:head][:article_number]
				h[:head].delete(:article_number)
				h[:body] = h[:head][:body]
				h[:head].delete(:body)
				h[:head].merge!(Hash[h[:head][:headers].to_s.split(/\r?\n/).map {|v|
					v = v.split(/:\s*/, 2)
					next if v[1].to_s == ''
					v
				}])
				h[:head].delete(:headers)
				yield h
			else
				yield nil
			end
		}
	end

	def get_group_stats(g, extra=false)
		query("
			SELECT
				newsgroup,
				COUNT(article_number) AS total,
				IF(MAX(article_number) IS NULL, 0, MAX(article_number)) AS max,
				IF(MIN(article_number) IS NULL, 0, MIN(article_number)) AS min
				#{', title, moderated' if extra}
			FROM
				meta #{'LEFT JOIN newsgroups USING(newsgroup)' if extra}
			WHERE
				approved=1
				#{prepare("AND newsgroup='%s'", g) if g}
			GROUP BY newsgroup
			#{"LIMIT 1" if g}") { |result|
			result = (result || []).map {|h| h.inject({}) {|c, (k, v)|
				v = v.to_i unless k == 'newsgroup' || k == 'title'
				v = (v != 0) if k == 'moderated'
				c[k.intern] = v
				c
			} }
			if g
				yield (result.first || {:total => 0, :max => 0, :min => 0, :newsgroup => g})
			else
				yield result
			end
		}
	end

	def format_hash(h, g=nil)
		r = h.inject({}) {|c, (k, v)|
			next c if v.to_s == ''
			k = k.intern
			c[k] = v
			c
		}
		r[:newsgroups] = (r[:newsgroups].to_s.split(/,\s*/) + [g]).uniq.compact
		r[:references] = r[:references].split(/,\s*/) || [] if r[:references]
		r[:date] = Time.at(r[:date].to_i) if r[:date]
		r.merge!({:bytes => h['body'].length, :lines => h['body'].count("\n")}) if h['body']
		r
	end

	def range_to_sql(field, range)
		if range
			if range.is_a?(Fixnum)
				prepare(" AND #{field}=%d", range)
			else
				prepare(" AND #{field} >= %d", range.first) +
				(range.last == 1/0.0 ? '' : prepare(" AND #{field} <= %d", range.last))
			end
		else
			''
		end
	end

	def query(sql, *args, &cb)
		df = @db.query(prepare(sql, *args))
		df.callback &cb
		df.errback {|e| LOG.error e.inspect}
		df
	end

	def prepare(sql, *args)
		args.map! {|arg| arg.is_a?(String) ? Mysql2::Client::escape(arg) : arg }
		sql % args
	end
end

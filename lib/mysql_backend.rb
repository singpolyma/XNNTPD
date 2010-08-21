# encoding: utf-8
require 'em-mysqlplus'
require 'time'
require 'util'

class MysqlBackend
	def initialize(config)
		@db = EventMachine::MySQL.new(config[:db].merge(:encoding => 'utf8'))
		@readonly = config[:readonly]
		query("
			CREATE TABLE IF NOT EXISTS newsgroups(
			newsgroup CHAR(150), mailto CHAR(255), nntp CHAR(255),
			title CHAR(255), moderated TINYINT, pgpkey BLOB
			)")
		query("
			CREATE TABLE IF NOT EXISTS meta(
			newsgroup CHAR(150), article_number INT, message_id CHAR(150),
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

	def owner(g)
		query("
			SELECT mailto, nntp, pgpkey
			FROM newsgroups
			WHERE newsgroup='%s'
			LIMIT 1", g) {|result|
			if (result = result.fetch_hash)
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
			if (result = result.fetch_hash)
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
				newsgroup='%s' #{range_to_sql('article_number', range)}
			ORDER BY article_number", g) { |result|
			list = []
			result.each {|row| list << row[0].force_encoding('utf-8') }
			blk.call(list)
		}
	end

	def last(g, current)
		query("SELECT
			article_number
		FROM
			meta
		WHERE
			newsgroup='%s' AND article_number < %d
		ORDER BY
			article_number DESC
		LIMIT 1", g, current.to_i) { |result|
			yield(if (result = result.fetch_row)
				{:article_number => result[0].to_i}
			end)
		}
	end

	def next(g, current)
		query("SELECT
			article_number
		FROM
			meta
		WHERE
			newsgroup='%s' AND article_number > %d
		ORDER BY
			article_number ASC
		LIMIT 1", g, current.to_i) { |result|
			yield(if (result = result.fetch_row)
				{:article_number => result[0].to_i}
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
		Util::new_article(:mesage_id => m[:message_id].decoded,
		                        :newsgroup => m[:newsgroups].decoded.split(/,\s*/).first)
		headers = m.header.fields.map { |head|
			unless [:message_id, :'message-id', :subject, :from, :date, :references].index(head.name.downcase.intern)
				"#{head.name}: #{head.decoded}"
			end
		}.compact.join("\r\n")
		groups = m[:newsgroups].to_s.split(/,\s*/).map {|n| prepare("'%s'", n)}.join(', ')
		query("
			SELECT
				newsgroup,
				IF(MAX(article_number) IS NULL, 0, MAX(article_number)) AS max,
				moderated, nntp
			FROM meta LEFT JOIN newsgroups USING (newsgroup)
			WHERE newsgroup IN (#{groups})
			GROUP BY newsgroup") {|nums|
			request = Multi.new
			nums.all_hashes.each {|num|
				# Skip any moderated groups we don't manage
				next if num['moderated'].to_i != 0 && num['nntp'].split('/',2)[1] != HOST
				# XXX: There's an ugly race condition on the article number...
				request << lambda {|&cb|
					query("INSERT INTO meta VALUES('%s', %d, '%s')",
					num['newsgroup'], num['max'].to_i + 1, m[:message_id]) {
						cb.call
					}
				}
				request << lambda {|&cb|
					query("INSERT INTO articles VALUES('%s', '%s', '%s', %d, '%s', '%s', '%s')",
					m[:message_id], m[:subject], m[:from], Time.parse(m[:date].to_s),
					m[:references], headers, m.body) {
						cb.call
					}
				}
			}
			request.call {
				yield true
			}
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
			WHERE date >= %d
			GROUP BY newsgroup", datetime) { |result|
			yield ((result.all_hashes || []).map {|h| h['newsgroup']})
		}
	end

	def newnews(wildmats, datetime)
		query("
			SELECT newsgroup, message_id
			FROM articles LEFT JOIN meta USING(message_id)
			WHERE date >= %d", datetime) { |result|
			yield ((result.all_hashes || []) \
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
		        FROM articles LEFT JOIN meta USING(message_id) WHERE 1=1"
		stmt << prepare(" AND newsgroup='%s'", g) if g
		stmt << prepare(" AND message_id='%s'", args[:message_id]) if args[:message_id]
		stmt << range_to_sql('article_number', args[:article_number])
		query(stmt) {|result|
			yield result.all_hashes.map {|h| format_hash(h, g)}
		}
	end

	def hdr(g, field, args, &cb)
		over(g, args) { |h|
			yield h.merge!(Hash[h[:headers].to_s.split(/\r?\n/).map {|v| v.split(/:\s*/)}])
		}
	end

	protected

	def one_article(g, args, head=true, body=true)
		stmt = "SELECT article_number, message_id
		        #{', subject, `from`, date, `references`, headers' if head}
		        #{', body' if body}
		        FROM articles LEFT JOIN meta USING(message_id) WHERE 1=1"
		stmt << prepare(" AND newsgroup='%s'", g) if g
		stmt << prepare(" AND message_id='%s'", args[:message_id]) if args[:message_id]
		stmt << range_to_sql('article_number', args[:article_number])
		stmt << ' LIMIT 1'

		query(stmt) {|result|
			if (result = result.fetch_hash)
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

	def get_group_stats(g, extra=false, &blk)
		query("
			SELECT
				newsgroup,
				COUNT(article_number) AS total,
				IF(MAX(article_number) IS NULL, 0, MAX(article_number)) AS max,
				IF(MIN(article_number) IS NULL, 0, MIN(article_number)) AS min
				#{', title, moderated' if extra}
			FROM
				meta #{'LEFT JOIN newsgroups USING(newsgroup)' if extra}
			#{prepare("WHERE newsgroup='%s'", g) if g}
			GROUP BY newsgroup
			#{"LIMIT 1" if g}") { |result|
			result = result.all_hashes.map {|h| h.inject({}) {|c, (k, v)|
				v = v.to_i unless k == 'newsgroup' || k == 'title'
				v = (v != 0) if k == 'moderated'
				c[k.intern] = v
				c
			} }
			if g
				yield result.first
			else
				yield result
			end
		}
	end

	def format_hash(h, g=nil)
		r = h.inject({}) {|c, (k, v)|
			next c if v.to_s == ''
			k = k.intern
			c[k] = v.force_encoding('utf-8')
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
		args.map! {|arg| arg.is_a?(String) ? Mysql::escape_string(arg) : arg }
		sql % args
	end
end

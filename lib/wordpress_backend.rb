require 'em-mysqlplus'

class WordPressBackend
	def initialize(config)
		@db = EventMachine::MySQL.new(config[:db].merge(:encoding => 'utf8'))
		@table_prefix = config[:table_prefix]
		@newsgroup = config[:newsgroup]
	end

	def group(g, &blk)
		get_group_stats g, &blk
	end

	def listgroup(g, range, &blk)
		range = if range
			if range.is_a?(Fixnum)
				' AND article_number=%d' % range
			else
				(' AND article_number >= %d' % range.first) +
				(range.last == 1/0.0 ? '' : ' AND article_number <= %d' % range.last)
			end
		else
			''
		end
		@db.query(prepare("
			SELECT
				message_id
			FROM
				#{table_name('newsgroup_meta')}
			WHERE
				newsgroup='%s' #{range}
			ORDER BY article_number", g)) { |result|
			list = []
			result.each {|row| list << row[0].force_encode('utf-8') }
			blk.call(list)
		}
	end

	def last(g, current)
		@db.query(prepare("SELECT
			article_number
		FROM
			#{table_name('newsgroup_meta')}
		WHERE
			newsgroup='%s' AND article_number < %d
		ORDER BY
			article_number DESC
		LIMIT 1", g, current.to_i)) { |result|
			yield(if (result = result.fetch_row)
				{:article_number => result[0].to_i}
			end)
		}
	end

	def next(g, current)
		@db.query(prepare("SELECT
			article_number
		FROM
			#{table_name('newsgroup_meta')}
		WHERE
			newsgroup='%s' AND article_number > %d
		ORDER BY
			article_number ASC
		LIMIT 1", g, current.to_i)) { |result|
			yield(if (result = result.fetch_row)
				{:article_number => result[0].to_i}
			end)
		}
	end

	protected

	def get_group_stats(g, &blk)
		update_newsgroup_meta
		@db.query(prepare("
			SELECT
				COUNT(article_number) AS total,
				IF(MAX(article_number) IS NULL, 0, MAX(article_number)) AS max,
				IF(MIN(article_number) IS NULL, 0, MIN(article_number)) AS min
			FROM
				#{table_name('newsgroup_meta')}
			WHERE
				newsgroup='%s' LIMIT 1", g)) { |result|
			blk.call(result.fetch_hash.inject({}) {|c, (k, v)|
				c[k.intern] = v.to_i
				c
			})
		}
	end

	def update_newsgroup_meta
		# TODO
	end

	def table_name(t)
		@table_prefix.to_s + t.to_s
	end

	def prepare(sql, *args)
		args.map! {|arg| arg.is_a?(String) ? Mysql::escape_string(arg) : arg }
		sql % args
	end
end

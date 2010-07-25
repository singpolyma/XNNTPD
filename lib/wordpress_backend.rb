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
		@db.query(prepare("
			SELECT
				message_id
			FROM
				#{table_name('newsgroup_meta')}
			WHERE
				newsgroup='%s' #{range_to_sql('article_number', range)}
			ORDER BY article_number", g)) { |result|
			list = []
			result.each {|row| list << row[0].force_encoding('utf-8') }
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

	protected

	def get_categories(article, &blk)
		@db.query(prepare("
			SELECT
				GROUP_CONCAT(name SEPARATOR ', ')
			FROM
				#{table_name('term_relationships')}
				LEFT JOIN
				#{table_name('term_taxonomy')} USING(term_taxonomy_id)
				LEFT JOIN
				#{table_name('terms')} USING(term_id)
			WHERE
				object_id=%d", article['id']), &blk)
	end

	def one_article(g, args, head=true, body=true)
		@db.query(article_query(args.merge(:newsgroup => g, :limit => 1), head, body)) { |result|
			if (result = result.fetch_hash)
				hash = {:article_number => result['article_number'].to_i}
				hash.merge!(:body => result['post_content'].force_encoding('utf-8')) if body
				hash[:head] = {:message_id => result['message_id'].force_encoding('utf-8')} unless head
				if head
					format_hash(result) { |head|
						hash.merge!(:head => head)
						if result['tbl'] == table_name('posts')
							get_categories(result) { |cat|
								hash[:head][:keywords] = cat[0] if (cat = cat.fetch_row)
								yield hash
							}
						else
							yield hash
						end
					}
				else
					yield hash
				end
			else
				yield nil
			end
		}
	end

	def article_query(args, head=true, body=true)
		stmt = "SELECT M.article_number,S.*,M.message_id FROM (
			(SELECT
				A.ID as id,
				#{'display_name,
				user_email,
				post_title,
				UNIX_TIMESTAMP(post_date_gmt) AS datestamp,' if head}
				#{'post_content,' if body}
				#{'post_parent,
				0 AS comment_parent,' if head}
				'#{table_name('posts')}' AS tbl
			FROM
				#{table_name('posts')} A,
				#{table_name('users')} B
			WHERE
				A.post_type='post' AND A.post_status='publish' AND
				A.post_author=B.ID
			) UNION (
			SELECT
				comment_ID AS id,
				#{"IF(user_id = 0, comment_author, display_name) as display_name,
				IF(user_id = 0, comment_author_email, user_email) as user_email,
				CONCAT('Re: ', post_title) as post_title,
				UNIX_TIMESTAMP(comment_date_gmt) AS datestamp," if head}
				#{'comment_content AS post_content,' if body}
				#{'comment_post_ID AS post_parent,
				comment_parent,' if head}
				'#{table_name('comments')}' AS tbl
			FROM
				#{table_name('comments')} A LEFT OUTER JOIN
				#{table_name('users')} B ON user_id=B.ID,
				#{table_name('posts')} C
			WHERE
				comment_post_ID=C.ID AND
				comment_approved='1' AND
				C.post_type='post' AND C.post_status='publish'
			)
		) S, #{table_name('newsgroup_meta')} M
		WHERE
			M.id=S.id AND M.tbl=S.tbl"
		stmt << prepare(" AND newsgroup='%s'", args[:newsgroup]) if args[:newsgroup]
		stmt << prepare(" AND message_id='%s'", args[:message_id]) if args[:message_id]
		stmt << range_to_sql('article_number', args[:article_number])
		stmt << prepare(' LIMIT %d', args[:limit]) if args[:limit]
	end

	def message_id(id, table)
		@db.query(prepare("
			SELECT
				message_id
			FROM
				#{table_name('newsgroup_meta')}
			WHERE
				id=%d AND tbl='%s'", id, table_name(table))) { |result|
			if (result = result.fetch_row)
				yield result[0]
			else
				yield nil
			end
		}
	end

	def format_hash(hash)
		# Forcing encodings because mysqlplus lies to us about the encoding
		h = ({
			:newsgroups     => [@newsgroup],
			:message_id     => hash['message_id'].force_encoding('utf-8'),
			:from           => "\"#{hash['display_name']}\" <#{hash['user_email']}>".force_encoding('utf-8'),
			:subject        => hash['post_title'].force_encoding('utf-8'),
			:date           => Time.at(hash['datestamp'].to_i)
		})
		h[:references] = [] if hash['post_parent'].to_i > 0 || hash['comment_parent'].to_i > 0
		request = Multi.new
		if hash['post_parent'].to_i > 0
			request << lambda { |&cb|
				message_id(hash['post_parent'], 'posts', &cb)
			}
		end
		if hash['comment_parent'].to_i > 0
			request << lambda { |&cb|
				message_id(hash['comment_parent'], 'comments', &cb)
			}
		end
		request.call { |ref|
			h[:references] = ref.flatten if ref.length > 0
			yield h
		}
	end

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

	def prepare(sql, *args)
		args.map! {|arg| arg.is_a?(String) ? Mysql::escape_string(arg) : arg }
		sql % args
	end
end

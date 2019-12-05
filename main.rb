#!/usr/bin/env ruby
# frozen_string_literal: true

## Twitter 上のあるアカウントから出発し、その相互フォロー関係を幅優先でたどって辺リストを得る。
#
# 結果は `db.sqlite3` に後述のスキーマに基づいて出力する。
#
# API キーは `credentials.json` から取得する。形式は以下の通り。
# ```json
# {
#   "consumer_key": : "...",
#   "consumer_secret": "...",
#   "access_token": "...",
#   "access_token_secret": "..."
# }
# ````

require 'set'
require 'sqlite3'
require 'twitter'


DB_PATH = 'db.sqlite3'

is_first_run = !FileTest.exist?(DB_PATH)
db = SQLite3::Database.new(DB_PATH)

# 初回起動時にデータベースを初期化
if is_first_run
  begin
    # 最初のアカウントのユーザID。コマンドライン引数から取る
    user = Integer(ARGV[0])

    db.execute <<-SQL
      -- 辺リスト
      CREATE TABLE edges (
        v BIGINT NOT NULL,
        w BIGINT NOT NULL,
        CONSTRAINT simple UNIQUE (v, w),
        CONSTRAINT undirected CHECK (w > v)
      );
    SQL
    db.execute <<-SQL
      -- 訪問済み頂点
      CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY, -- ユーザ ID
        distance INTEGER NOT NULL, -- 初期頂点からの距離
        friends_count INTEGER, -- フォロー数
        followers_count INTEGER, -- フォロワー数
        accessible INTEGER -- アクセス可能かの真偽値。非公開アカウント等では `0` とする
      );
    SQL
    db.execute <<-SQL
      -- 幅優先探索のキュー
      CREATE TABLE queue (
        -- 内部 ID。先入れ先出しを実現するために挿入タイミングの昇順に番号づけする
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        user_id BIGINT NOT NULL
      );
    SQL

    db.execute('INSERT INTO users (id, distance) VALUES (?, 0)', user)
    db.execute('INSERT INTO queue (user_id) VALUES (?)', user)
  rescue
    db.close
    File.delete(DB_PATH)
    raise
  end
end


credentials = open('credentials.json') {|f| JSON.load(f) }
client = Twitter::REST::Client.new do |config|
  config.consumer_key = credentials['consumer_key']
  config.consumer_secret = credentials['consumer_secret']
  config.access_token = credentials['access_token']
  config.access_token_secret = credentials['access_token_secret']
end


# ヘルパーメソッドの定義

## 与えられたブロックを実行し、ブロックから返ってきた cursor から値を全て取り出して配列にまとめる。
# また、その過程でレートリミットに到達した場合に適宜スリープしてからリトライする。
#
# スリープ中の時間を有効に使うために上記の処理は別スレッドで実行し、このメソッドはそのスレッドのハンドルを返す。
#
# # 利用例
#
# ```
# handle = cursor_to_a { client.followers(user) }
# # ...
# followers = handle.value
# ```
def cursor_to_a
  t = Thread.new do
    cursor = catch_rate_limit { yield }
    catch_rate_limit { cursor.to_a }
  end
  t.report_on_exception = false if t.respond_to?(:report_on_exception=)
  t
end

## レートリミットの解消を待ちながら与えられたブロックを実行する。
def catch_rate_limit
  begin
    yield
  rescue Twitter::Error::TooManyRequests => e
    t = e.rate_limit.reset_in
    STDERR.puts("sleep: #{t} secs")
    sleep(t + 1) # サーバの時計とのズレなどを考慮して 1 秒余分にスリープ
    retry
  end
end

## データベースコネクションとブロックを受け取り、トランザクション内でブロックを実行する。
#
# `Database#transaction` にも同様の機能があるが`StandardError` 以外の例外（`Interrupt` など）時に
# 正しくロールバックされないため、ここでは自前で処理する。
def transaction(db)
  rollback = false
  db.transaction
  begin
    yield db
  rescue Exception
    rollback = true
    raise
  ensure
    if rollback
      db.rollback
    else
      db.commit
    end
  end
end


# メイン処理

# プリペアドステートメントの用意
# キューの先頭の要素をデキューせずに読む
peek_queue = db.prepare <<-SQL
  SELECT users.id, users.distance
    FROM users
    JOIN queue ON users.id == queue.user_id
    ORDER BY queue.id ASC
    LIMIT 1
SQL
set_accessibility = db.prepare <<-SQL
  UPDATE users
    SET accessible = ?2
    WHERE id = ?1
SQL
dequeue = db.prepare('DELETE FROM queue where id == (SELECT MIN(id) FROM queue)')
set_friends_followers_count = db.prepare <<-SQL
  UPDATE users
    SET friends_count = ?2, followers_count = ?3
    WHERE id = ?1
SQL
add_edge = db.prepare('REPLACE INTO edges VALUES (?, ?)')
user_is_visited = db.prepare('SELECT EXISTS (SELECT * FROM users WHERE id == ?)')
add_user = db.prepare('INSERT INTO users (id, distance) VALUES (?, ?)')
enqueue = db.prepare('INSERT INTO queue (user_id) VALUES (?)')

# 探索のメインループ
loop do # キューが空になるまで繰り返す
  # キューの先頭の頂点を読む（デキューは後のトランザクション内で行う）
  (v, d) = peek_queue.execute.first
  break unless v # キューが空なら終了

  STDERR.puts("inspecting user: #{v}, d = #{d}")

  # `v` のフォロー・フォロワーの ID を取得
  (following, followers) = begin
    following = cursor_to_a { client.friend_ids(v, count: 5000) }
    followers = cursor_to_a { client.follower_ids(v, count: 5000) }
    [following.value, followers.value]
  rescue Twitter::Error::Unauthorized
    STDERR.puts("unauthorized request for user #{v}; maybe a protected user")
    set_accessibility.execute(v, 0)
    dequeue.execute
    next
  end
  set_accessibility.execute(v, 1)

  transaction(db) do
    dequeue.execute
    set_friends_followers_count.execute(v, following.length, followers.length)

    # 各相互フォロワーについて
    (following & followers).each do |w|
      add_edge.execute(*[v, w].sort)
      if user_is_visited.execute(w).first == [0] # 未探索ならば
        # 頂点集合とキューに追加
        add_user.execute(w, d + 1)
        enqueue.execute(w)
      end
    end
  end
end

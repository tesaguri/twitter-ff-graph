#!/usr/bin/env ruby

# Twitter 上のあるアカウントから出発し、その相互フォロー関係を幅優先でたどって辺リストを得る。

require 'set'
require 'sqlite3' # gem install sqlite3
require 'twitter' # gem install twitter


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
        id INTEGER NOT NULL PRIMARY KEY,
        -- 初期頂点からの距離
        distance INTEGER NOT NULL
      );
    SQL
    db.execute <<-SQL
      -- 幅優先探索のキュー
      CREATE TABLE queue (
        -- 内部 ID。先入れ先出しを実現するために挿入タイミングの昇順に番号づけする
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        user_id BIGINT
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


# Twitter API にアクセスするためのオブジェクトを用意する
credentials = open('credentials.json') {|f| JSON.load(f) } # ログイン情報
client = Twitter::REST::Client.new do |config|
  config.consumer_key = credentials['consumer_key']
  config.consumer_secret = credentials['consumer_secret']
  config.access_token = credentials['access_token']
  config.access_token_secret = credentials['access_token_secret']
end


# ヘルパーメソッドの定義

## 値の列を返す API から 値を取得するためのメソッド。
#
# 値の列を返す API では一般には一度のリクエストで全ての値を読み切ることができないので、
# そのような API は一般に「カーソル」と呼ばれるオブジェクトを返す。カーソルは現在どこまで
# 読み進めているかなどの情報を含み、API の利用者はこれを使って API を次々に呼び出して値を
# 集める。
#
# このメソッドは与えられたブロック（クロージャ、無名関数）を実行し、ブロックから返ってきた
# カーソルから値を全て取り出して配列にまとめる。また、その過程で API 制限（レートリミット）に
# 到達した場合に適宜スリープしてリトライする。
#
# スリープ中の時間を有効に使うために上記の処理は別スレッドで実行し、このメソッドは
# そのスレッドのハンドルを返す。
#
# # 利用例
#
# ```
# # `user` のフォロワーの取得を試みる。この時点でメインスレッドはスリープしない
# handle = cursor_to_a { client.followers(user) }
# # ...
# # スレッドの終了を待ってから値を取り出す
# followers = handle.value
# ```
def cursor_to_a
  t = Thread.new do
    # `yield` は与えられたブロックを実行するキーワード
    cursor = catch_rate_limit { yield }
    catch_rate_limit { cursor.to_a } # カーソルから値を取り出して配列にする
  end
  t.report_on_exception = false if t.respond_to?(:report_on_exception=)
  t
end

## レートリミットの解消を待ちながら与えられたブロックを実行するメソッド。
def catch_rate_limit
  begin # (begin-rescue は try-catch に相当)
    yield
  rescue Twitter::Error::TooManyRequests => e # レートリミットの例外
    t = e.rate_limit.reset_in # 制限が解除されるまでの秒数
    STDERR.puts("sleep: #{t} secs") # ログ出力
    sleep(t + 1) # サーバの時計とのズレなどを考慮して 1 秒余分にスリープ
    retry
  end
end


# メイン処理

# プリペアドステートメントの用意
first_user_in_queue = db.prepare <<-SQL
  SELECT users.id, users.distance, queue.id
    FROM users
    JOIN queue ON users.id == queue.user_id
    ORDER BY queue.id ASC
    LIMIT 1
SQL
delete_from_queue = db.prepare('DELETE FROM queue WHERE id == ?')
add_edge = db.prepare('REPLACE INTO edges VALUES (?, ?)')
is_in_users = db.prepare('SELECT EXISTS (SELECT * FROM users WHERE id == ?)')
add_user = db.prepare('INSERT INTO users (id, distance) VALUES (?, ?)')
add_to_queue = db.prepare('INSERT INTO queue (user_id) VALUES (?)')

# 探索のメインループ
loop do # キューが空になるまで繰り返す
  # キューの先頭の頂点を読む（まだ取り除かない）
  (v, d, queue_id) = first_user_in_queue.execute.first
  break unless user # キューが空なら終了

  db.transaction do
    STDERR.puts("inspecting user: #{v}, d = #{d}")

    # `v` の相互フォローのアカウントを集める
    ff = begin
      # `v` がフォローしているアカウントと `v` のフォロワーの ID を取得する
      following = cursor_to_a { client.friend_ids(v, count: 5000) }
      followers = cursor_to_a { client.follower_ids(v, count: 5000) }
      # それらの共通部分をとる
      following.value & followers.value
    rescue Twitter::Error::Unauthorized # リクエスト失敗時
      STDERR.puts("unauthorized request for user #{v}; maybe a protected user")
      next
    end

    # 各相互フォロワー `w` について
    ff.each do |w|
      # 辺リストに追加
      add_edge.execute(*[v, w].sort)
      if is_in_users.execute(w).first == [0] # 未探索ならば
        add_user.execute(w, d + 1) # 頂点集合と
        add_to_queue.execute(w) # キューに追加
      end
    end

    # 実際にキューの先頭を削除
    delete_from_queue.execute(queue_id)
  end
end

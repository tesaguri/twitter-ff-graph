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

require 'securerandom'
require 'set'
require 'sqlite3'
require 'twitter'


DB_PATH = 'db.sqlite3'

is_first_run = !FileTest.exist?(DB_PATH)
db = SQLite3::Database.new(DB_PATH)

# 最初のアカウントのユーザID。コマンドライン引数から取る
user = Integer(ARGV[0])

# 初回起動時にデータベースを初期化
if is_first_run
  begin
    db.execute <<-SQL
      -- フォロー関係
      CREATE TABLE friendships (
        follower BIGINT NOT NULL, -- フォロワー
        friend BIGINT NOT NULL, -- 被フォロー
        CONSTRAINT simple UNIQUE (follower, friend)
      );
    SQL
    db.execute <<-SQL
      -- 訪問済み頂点
      CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY, -- ユーザ ID
        friends_count INTEGER, -- フォロー数
        followers_count INTEGER, -- フォロワー数
        inspected_at INTEGER, -- フォロー・フォロワーの情報を取得した時刻のナノ秒精度の UNIX time
        accessible INTEGER -- アクセス可能かの真偽値。非公開アカウント等では `0` とする
      );
    SQL

    db.execute('INSERT INTO users (id) VALUES (?)', user)
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

def now_ns
  now = Time.now
  now.to_i * (10 ** 9) + now.nsec
end


# メイン処理

# プリペアドステートメントの用意
set_accessibility = db.prepare <<-SQL
  UPDATE users
    SET accessible = ?2
    WHERE id = ?1
SQL
add_friendship = db.prepare('INSERT OR IGNORE INTO friendships (follower, friend) VALUES (?, ?)')
get_inspected_at = db.prepare('SELECT inspected_at FROM users where id == ?')
add_user = db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)')
set_inspected_at = db.prepare('UPDATE users SET inspected_at = ?2 WHERE id == ?1')

if get_inspected_at.execute(user).first == [nil]
  STDERR.puts("inspecting user: #{user}")
  cursor = catch_rate_limit { client.follower_ids(user, count: 5000) }.each
  loop do
    follower = catch_rate_limit { cursor.next }
    add_user.execute(follower)
    add_friendship.execute(follower, user)
  end
  set_inspected_at.execute(user, now_ns)
else
  STDERR.puts("user is already inspected: #{user}")
end

followers = db.prepare(<<-SQL).execute(user).map {|(id)| id}
  SELECT friendships.follower
    FROM friendships
    JOIN users ON friendships.follower == users.id
    WHERE friendships.friend == ? AND users.inspected_at IS NULL
SQL
until followers.empty?
  follower = followers.delete_at(SecureRandom.random_number(followers.length))

  # XXX: 重複コード
  STDERR.puts("inspecting user: #{follower}")
  begin
    cursor = catch_rate_limit { client.friend_ids(follower, count: 5000) }.each
    loop do
      friend = catch_rate_limit { cursor.next }
      add_user.execute(friend)
      add_friendship.execute(follower, friend)
    end
  rescue Twitter::Error::Unauthorized
    STDERR.puts("unauthorized request for user #{follower}; maybe a protected user")
    set_accessibility.execute(follower, 0)
    next
  rescue Twitter::Error::NotFound
    STDERR.puts("user has been deleted: #{user}")
    set_accessibility.execute(follower, 0)
  end
  set_inspected_at.execute(user, now_ns)
end

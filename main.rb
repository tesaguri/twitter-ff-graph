#!/usr/bin/env ruby
# frozen_string_literal: true

## Twitter 上のあるアカウント群の各アカウントのフォロワーのフォロイーを収集する。
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

# 初回起動時にデータベースを初期化
if is_first_run
  begin
    db.execute <<~SQL
      -- 訪問済み頂点
      CREATE TABLE users (
        id INTEGER NOT NULL PRIMARY KEY, -- ユーザ ID
        got_followers_at INTEGER, -- フォロワーの情報を取得した時刻（ナノ秒精度の UNIX time）
        got_friends_at INTEGER, -- フォローの情報を取得した時刻
        accessible INTEGER -- アクセス可能かの真偽値。非公開アカウント等では `0` とする
      );
    SQL
    db.execute <<~SQL
      -- 収集対象のアカウント（ターゲット）
      CREATE TABLE targets (
        id INTEGER NOT NULL PRIMARY KEY REFERENCES users(id)
      );
    SQL
    db.execute <<~SQL
      -- フォロー関係
      CREATE TABLE friendships (
        follower BIGINT NOT NULL REFERENCES users(id), -- フォロワー
        friend BIGINT NOT NULL REFERENCES users(id), -- 被フォロー
        CONSTRAINT simple UNIQUE (follower, friend)
      );
    SQL
  rescue
    db.close
    File.delete(DB_PATH)
    raise
  end
end

# コマンドライン引数に指定された ID をターゲットとして追加
unless ARGV.empty?
  users = ARGV.map {|arg| Integer(arg) }
  values = (['(?)'] * users.length).join(',')
  db.execute('INSERT INTO users (id) VALUES ' + values, users)
  db.execute('INSERT INTO targets (id) VALUES ' + values, users)
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
set_accessibility = db.prepare <<~SQL
  UPDATE users
    SET accessible = ?2
    WHERE id = ?1
SQL
add_friendship = db.prepare('INSERT OR IGNORE INTO friendships (follower, friend) VALUES (?, ?)')
add_user = db.prepare('INSERT OR IGNORE INTO users (id) VALUES (?)')
set_got_followers_at = db.prepare('UPDATE users SET got_followers_at = ?2 WHERE id == ?1')
set_got_friends_at = db.prepare('UPDATE users SET got_friends_at = ?2 WHERE id == ?1')
# ターゲットのフォロワーのうち、そのフォローが未収集であるものの中から、指定された位置にあるものを返す
uninspected_follower_at = db.prepare <<~SQL
  SELECT DISTINCT follower
    FROM friendships
    JOIN users ON follower == users.id
    WHERE friend IN targets AND users.got_friends_at IS NULL
    LIMIT ?, 1
SQL

targets = db.execute(<<~SQL)
  SELECT targets.id, users.got_followers_at
    FROM targets
    JOIN users ON users.id == targets.id
SQL

targets.each do |(user, got_followers_at)|
  if !got_followers_at # フォロワーが未収集
    STDERR.puts("inspecting target: #{user}")
    cursor = catch_rate_limit { client.follower_ids(user, count: 5000) }.each
    transaction(db) do
      loop do
        follower = catch_rate_limit { cursor.next }
        add_user.execute(follower)
        add_friendship.execute(follower, user)
      end
      set_got_followers_at.execute(user, now_ns)
    end
  else
    STDERR.puts("target is already inspected: #{user}")
  end
end

uninspected_count = db.execute(<<~SQL).first.first
  SELECT COUNT(DISTINCT follower)
    FROM friendships
    JOIN users ON follower == users.id
    WHERE friend IN targets AND users.got_friends_at IS NULL
SQL

# ターゲットのフォロワーのフォローの収集
until uninspected_count == 0
  # フォローが未収集であるフォロワーを無作為にとる
  follower = uninspected_follower_at.execute(SecureRandom.random_number(uninspected_count)).first.first

  # XXX: 重複コード
  STDERR.puts("inspecting follower: #{follower}")
  transaction(db) do
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
    rescue Twitter::Error::NotFound
      STDERR.puts("user has been deleted: #{user}")
      set_accessibility.execute(follower, 0)
    end
    set_got_friends_at.execute(follower, now_ns)
  end
  uninspected_count -= 1
end

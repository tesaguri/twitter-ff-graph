#!/usr/bin/env ruby

# Twitter 上のあるアカウントから出発し、その相互フォロー関係を幅優先でたどって辺リストを得る。

require 'set'
require 'twitter' # gem install twitter


# 辺リストの出力先ファイル
DUMP = 'edges.dat'
# 探索の状態のバックアップ用ファイル
STATE_FILE = 'state.dat'


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
  Thread.new do
    # `yield` は与えられたブロックを実行するキーワード
    cursor = catch_rate_limit { yield }
    catch_rate_limit { cursor.to_a } # カーソルから値を取り出して配列にする
  end
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

# 幅優先探索の準備。
#
# 一般に幅優先探索では初期頂点からの距離の昇順で頂点を探索していく。
# また、新たにキューの末尾に追加する頂点はキューの先頭から取り出す頂点の「1 歩先」であるから、
# ある時点のキューには高々 2 通りの距離 d(v) = d-1, d の頂点しかなく、d(v) = d-1 の頂点は
# キューの先頭側に、d(v) = d の頂点は末尾側に集まることが分かる。
# そこで今回の探索ではこれらの境界に番兵として `nil` を置いて頂点の距離を把握する。
#
# キューの模式図:
# (先頭) out <- [v_0, ..., v_m, nil, w_0, ..., w_n] <- in (末尾)
# where
#   d(v_i) = d - 1 (i = 0, ..., m),
#   d(w_i) = d (i = 0, ..., n).

# 最初のアカウント（頂点）のユーザID。コマンドライン引数から取る
v = Integer(ARGV[0])
# 幅優先探索のキュー。配列で代用している。要素の取り出しは配列の先頭から行う。一見効率が
# 悪そうだが、実際には定数時間でできる（おそらくインタプリタの実装で工夫されているのだろう）。
# また、今回はレートリミットの待ち時間が律速なので、仮に多少遅かったとしても無視できる。
q = [nil, v]
# キューの末尾の頂点の距離
d = 0
# 訪問済み頂点の集合
vertices = Set.new([v])

# バックアップファイルが存在するならその状態を読み込む
if FileTest.exist?(STATE_FILE)
  state = open(STATE_FILE) {|f| Marshal.load(f) }

  q = state[:q]
  d = state[:d]

  # 頂点集合は辺リストから再現する
  vertices = Set.new
  open(DUMP) do |f|
    f.each_line do |l|
      if (m = l.match(/^(\d+)\t(\d+)(:?$|\t)/))
        vertices.add(Integer(m[1]))
        vertices.add(Integer(m[2]))
      end
    end
  end
end

# 辺リストの出力先
out = open(DUMP, 'a')

# 探索のメインループ
until q == [nil] do # キューが空になるまで繰り返す
  v = q.shift # キューの先頭から頂点を取り出す

  unless v # キューから偽値（`nil`）が取り出されたとき（頂点距離の境界）
    STDERR.puts("processed users with d = #{d-1}")
    STDERR.puts("current queue size = #{q.length}")
    q.push(nil)
    # 辺リストに距離の情報をコメントとして記録
    out.puts("\# d = #{d}")
    d += 1
    next
  end

  STDERR.puts("inspecting user: #{v}, d = #{d-1}")

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
    # 辺リストに出力。重複は気にしないことにする（必要ならば後処理で除く）
    out.puts("#{v}\t#{w}")
    unless vertices.include?(w) # 未探索ならば
      vertices.add(w) # 頂点集合と
      q.push(w) # キューに追加
    end
  end

  # 現在の状態をバックアップに書き出す
  open(STATE_FILE, 'w') do |f|
    Marshal.dump({ q: q, d: d }, f)
  end
end

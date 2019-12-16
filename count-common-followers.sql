-- ターゲット内の 2 つのアカウントの組のそれぞれについて、その両方をフォローしているアカウントの数を出力する

.mode tabs

-- `targets` からアカウントの非順序対 (`left`, `right`) を列挙
WITH pairs AS (
  SELECT left.id AS left, right.id AS right
    FROM targets AS left, targets AS right -- `targets` と自分自身の直積を取る
    WHERE left.id < right.id -- 重複を排除
)
SELECT
  left,
  right,
  (
    -- `left` と `right` の両方をフォローしているアカウントの数
    SELECT COUNT(*)
      FROM (
        SELECT follower
          FROM friendships
          GROUP BY follower
          HAVING SUM(friend = left OR friend = right) = 2
      )
  )
  FROM pairs;

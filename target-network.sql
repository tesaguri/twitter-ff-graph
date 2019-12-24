.mode tabs
SELECT follower, friend
  FROM friendships
  WHERE follower IN targets AND friend IN targets;

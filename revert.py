import pickle

TRACKER_PATH = 'feed_tracker.pickle'

with open(TRACKER_PATH, 'rb') as handle:
    feed_tracker = pickle.load(handle)

type(feed_tracker['breaking_news']['last_modified'])

new_last_modified = 'Tue, 05 Apr 2022 14:00:00 GMT'

for feed_name, trackers in feed_tracker.items():
    last_modified = trackers.get('last_modified')
    print(feed_name, last_modified)
    trackers['last_modified'] = new_last_modified


with open(TRACKER_PATH + '_new_2', 'wb') as handle:
    pickle.dump(feed_tracker, handle, protocol=pickle.HIGHEST_PROTOCOL)

# import feedparser
# rss_feed = "https://www.straitstimes.com/news/asia/rss.xml"
# news_feed = feedparser.parse(rss_feed)
#
# for i in news_feed.entries:
#     print(i)
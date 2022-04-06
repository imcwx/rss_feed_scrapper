import time
import os
import pickle
import logging
from datetime import datetime
import feedparser
import pandas as pd
import urllib.request


def create_logger(logger_file_name):
    # Initialize logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # create logger filename
    date_time_now = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file_name = "{}_{}.log".format(logger_file_name, date_time_now)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(log_file_name)
    fh.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    return logger


def get_feed(rss_feed, modified_time=None, etag=None):
    # Get the Feed
    if modified_time:
        news_feed = feedparser.parse(rss_feed, modified=modified_time)
    elif etag:
        news_feed = feedparser.parse(rss_feed, etag=etag)
    else:
        news_feed = feedparser.parse(rss_feed)
    return news_feed


def pretty_dict(d, indent=0):
    # Pretty Print Dictionary
    for key, value in d.items():
        logger.info('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty_dict(value, indent + 1)
        else:
            logger.info('\t' * (indent + 1) + str(value))


def init_feed(rss_feed_links):
    logger.info("Initialize Feeds")
    data = {}
    feed_tracker = {}

    # Retrieve Feeds
    for feed_name, rss_feed_link in rss_feed_links.items():
        res = get_feed(rss_feed_link)
        data[feed_name] = res
        feed_tracker[feed_name] = rss_feed_link
    logger.info("Retrieved Feeds")

    # Initialize tracker attributes
    for feed_name, rss_feed in data.items():
        # dict_keys(['bozo', 'entries', 'feed', 'headers', 'etag', 'updated',
        # 'updated_parsed', 'href', 'status', 'encoding', 'version', 'namespaces'])

        # Add Tracker Attributes
        feed_tracker[feed_name] = {
            "rss_feed_link": feed_tracker[feed_name],
            "last_modified": rss_feed.get('modified', None),
            "feed_updated": rss_feed.feed.get('updated', None),
            "feed_published": rss_feed.feed.get('published', None),
            "etag": rss_feed.get('etag', None),
            "rss_feed_updated": rss_feed.get('updated', None),
        }
    return data, feed_tracker


def get_feed_update(feed_tracker, last_modified=True, last_etag=False):
    new_data = {}
    for feed_name, trackers in feed_tracker.items():
        if last_modified:   # Get feed update using modified time
            new_feed = get_feed(trackers.get('rss_feed_link'), modified_time=trackers.get('last_modified'))
        else:   # Else use etag
            new_feed = get_feed(trackers.get('rss_feed_link'), etag=trackers.get('etag'))
        new_data[feed_name] = new_feed
    return new_data


def update_tracker(rss_feed, feed_tracker, feed_name):
    # Update feed tracker in-place
    feed_tracker[feed_name] = {
        "rss_feed_link": feed_tracker[feed_name].get('rss_feed_link', None),
        "last_modified": rss_feed.get('modified', None),
        "feed_updated": rss_feed.feed.get('updated', None),
        "feed_published": rss_feed.feed.get('published', None),
        "etag": rss_feed.get('etag', None),
        "rss_feed_updated": rss_feed.get('updated', None),
    }


def save_data(data, feed_tracker, init=False):
    # Save the data entries
    if init:
        # Initialize data path if not exist
        if not os.path.exists(DATA_PATH):
            os.mkdir(DATA_PATH)

    # Loop through each feed
    for feed_name, rss_feed in data.items():
        data_feed_path = os.path.join(DATA_PATH, feed_name)
        record_path = os.path.join(data_feed_path, 'records.csv')

        if init:
            # Initialize data feed path if not exist
            if not os.path.exists(data_feed_path):
                os.mkdir(data_feed_path)

            # Initialize record tracker for each feed.
            init_cols = [
                'title', 'title_detail', 'links', 'link', 'summary', 'summary_detail', 'id',
                'guidislink', 'published', 'published_parsed', 'source']
            df_entries = pd.DataFrame([], columns=init_cols)
            df_entries.to_csv(record_path, header=True)

        # Check Feed Entries
        new_entries = rss_feed.entries
        if new_entries:
            # When there is new, update tracker, record and pull data
            num_entries = len(new_entries)
            logger.info("{} has {} new entries".format(feed_name, num_entries))

            # Update Tracker
            update_tracker(rss_feed, feed_tracker, feed_name)

            # Update feed records
            df_new_entries = pd.DataFrame(new_entries)
            df_new_entries.to_csv(record_path, mode='a', header=False)

            # Request/pull actually article
            for entry in new_entries:
                url = entry.get('link')
                request = urllib.request.Request(url)
                basename = os.path.basename(url)
                file_path = os.path.join(data_feed_path, f"{basename}.html")
                logger.info(f"Entry basename: {basename}")
                try:
                    response = urllib.request.urlopen(request)
                    data_entry = response.readlines()
                    data_string = [i.decode('utf-8') for i in data_entry]
                    with open(file_path, "w") as f:
                        f.writelines(data_string)
                except Exception as e:
                    logger.exception("Error, cannot retrieve data")
                    logger.error(f"url : {url}")
        else:
            # No new entries
            logger.info("%s has 0 new entries" % feed_name)


def main():
    if os.path.exists(TRACKER_PATH):
        logger.info("Initializing from previous tracker")
        with open(TRACKER_PATH, 'rb') as handle:
            feed_tracker = pickle.load(handle)
    else:
        logger.info("No tracker found, initializing feed")
        data, feed_tracker = init_feed(straits_time_rss_feed)
        with open(TRACKER_PATH, 'wb') as handle:
            pickle.dump(feed_tracker, handle, protocol=pickle.HIGHEST_PROTOCOL)
        save_data(data, feed_tracker, init=True)

    # Loop and update each feed.
    while True:
        logger.info("Get Feed Update")
        new_data = get_feed_update(feed_tracker)
        save_data(new_data, feed_tracker, init=False)
        logger.info(f'--------wait {SLEEP_DURATION} seconds before checking again-------')
        time.sleep(SLEEP_DURATION)


if __name__ == '__main__':
    logger = create_logger('feed')
    logger.info("Program Started")

    # Initiate parameters
    TRACKER_PATH = 'feed_tracker.pickle'
    DATA_PATH = 'data'
    SLEEP_DURATION = 60

    logger.info("straits_time_rss_feed does not support etags, use modified time")
    straits_time_rss_feed = {
        "breaking_news": "https://www.straitstimes.com/rss_breaking_news.opml",
        "world": "https://www.straitstimes.com/news/world/rss.xml",
        "business": "https://www.straitstimes.com/news/business/rss.xml",
        "sport": "https://www.straitstimes.com/news/sport/rss.xml",
        "life": "https://www.straitstimes.com/news/life/rss.xml",
        "opinion": "https://www.straitstimes.com/news/opinion/rss.xml",
        "singapore": "https://www.straitstimes.com/news/singapore/rss.xml",
        "asia": "https://www.straitstimes.com/news/asia/rss.xml",
        "tech": "https://www.straitstimes.com/news/tech/rss.xml",
        "multimedia": "https://www.straitstimes.com/news/multimedia/rss.xml",
    }
    main()
    logger.info("Program Completed")

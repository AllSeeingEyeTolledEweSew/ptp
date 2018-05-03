# The author disclaims copyright to this source code. Please see the
# accompanying UNLICENSE file.

"""Several long-lived-daemon-style classes to scrape BTN and update the cache.
"""

import feedparser
import logging
import os
import Queue
import threading
import time
from urllib import parse as urllib_parse

import ptp


def log():
    """Gets a module-level logger."""
    return logging.getLogger(__name__)


def get_int(api, key):
    """Get an integer global value from a `ptp.API`."""
    try:
        return int(api.get_global(key))
    except (ValueError, TypeError):
        return None


def set_int(api, key, value):
    """Set an integer global value on a `ptp.API`."""
    if value is None:
        api.delete_global(key)
    else:
        api.set_global(key, str(value))


def apply_contiguous_results_locked(api, sr, is_end, changestamp=None):
    """Marks torrent entries as deleted, appropriate to a search result.

    When we receive a search result for getTorrents with no filters, the result
    should be a contiguous slice of all torrents on the site, ordered by time.
    Here we search for any torrent entries in the local cache that ought to be
    in the results (the id is between the oldest and youngest id returned), but
    isn't found there. We mark all these torrent entries as deleted.

    In fact, this is the only way to know when a torrent entry has been deleted
    from the site.

    As a special case, if this is known to be the last page of results on the
    site, any torrent entries older than the oldest returned are marked as
    deleted.

    Args:
        api: A `ptp.API` instance.
        offset: The offset parameter that was passed to getTorrents.
        sr: A `ptp.SearchResult` returned as a result of getTorrents with no
            filters, and the supplied offset.
        changestamp: An integer changestamp. If None, a new changestamp will be
            generated.

    Returns:
        A (list_of_torrent_entries, is_end) tuple. The list of torrent entries
            is sorted by time descending. is_end specifies whether `sr` was
            determined to represent the last page of results.
    """
    assert all(len(m.torrents) == 1 for m in sr.movies)
    entries_by_time = sorted(
        (te for mr in sr.movies for te in mr.torrents),
        key=lambda te: (te.time, te.id))

    if entries_by_time:
        c = api.db.cursor()
        newest = entries_by_time[-1]
        oldest = entries_by_time[0]
        api.db.cursor().execute(
            "create temp table if not exists ids "
            "(id integer not null primary key)")
        api.db.cursor().execute("delete from ids")
        api.db.cursor().executemany(
            "insert into temp.ids (id) values (?)",
            [(entry.id,) for entry in entries_by_time])

        torrent_entries_to_delete = set()
        movies_to_check = set()
        if is_end:
            for id, movie_id in api.db.cursor().execute(
                    "select id, movie_id from torrent_entry "
                    "where time <= ? and id < ? and not deleted",
                    (oldest.time, oldest.id)):
                torrent_entries_to_delete.add(id)
                movies_to_check.add(movie_id)
        for id, movie_id in api.db.cursor().execute(
                "select id, movie_id from torrent_entry "
                "where (not deleted) and time < ? and time > ? and "
                "id not in (select id from temp.ids)",
                (newest.time, oldest.time)):
            torrent_entries_to_delete.add(id)
            movies_to_check.add(movie_id)

        if torrent_entries_to_delete:
            log().debug(
                "Deleting torrent entries: %s",
                sorted(torrent_entries_to_delete))

            if changestamp is None:
                changestamp = api.get_changestamp()

            api.db.cursor().executemany(
                "update torrent_entry set deleted = 1, updated_at = ? "
                "where id = ?",
                [(changestamp, id) for id in torrent_entries_to_delete])
            ptp.Movie._maybe_delete(
                api, *list(movies_to_check), changestamp=changestamp)

    return entries_by_time


def apply_whole_movie_results_locked(api, offset, sr, changestamp=None):
    torrent_entries_to_delete = set()
    for mr in sr.movies:
        for id, in api.db.cursor().execute(
                "select id from torrent_entry "
                "where (not deleted) and movie_id = ? and "
                "id not in (%s)" % ",".join(["?"] * len(mr.torrents)),
                [mr.movie.id] + [te.id for te in mr.torrents]):
            torrent_entries_to_delete.add(id)

    if torrent_entries_to_delete:
        log().debug(
            "Deleting torrent entries: %s",
            sorted(torrent_entries_to_delete))

        if changestamp is None:
            changestamp = api.get_changestamp()

        api.db.cursor().executemany(
            "update torrent_entry set deleted = 1, updated_at = ? "
            "where id = ?",
            [(changestamp, id) for id in torrent_entries_to_delete])


class MetadataScraper(object):
    """A long-lived daemon that updates cached data about all torrents.

    This daemon calls getTorrents with no filters and varying offset. The
    intent is to discover deleted torrents, and freshen metadata.

    This daemon will consume as many tokens from `api.api_token_bucket` as
    possible, up to a configured limit. The intent of this is to defer token
    use to `MetadataTipScraper`.

    Attributes:
        api: A `ptp.API` instance.
        target_tokens: A number of tokens to leave as leftover in
            `api.api_token_bucket`.
    """

    KEY_OFFSET = "scrape_next_offset"
    KEY_RESULTS = "scrape_last_results"

    BLOCK_SIZE = 1000

    DEFAULT_TARGET_TOKENS = 0
    DEFAULT_NUM_THREADS = 10

    def __init__(self, api, target_tokens=None, num_threads=None, once=False):
        if num_threads is None:
            num_threads = self.DEFAULT_THREADS
        if target_tokens is None:
            target_tokens = self.DEFAULT_TARGET_TOKENS

        if api.username is None:
            raise ValueError("username not configured")
        if api.password is None:
            raise ValueError("password not configured")
        if api.passkey is None:
            raise ValueError("passkey not configured")

        self.api = api
        self.target_tokens = target_tokens
        self.num_threads = num_threads
        self.once = once

        self.lock = threading.RLock()
        self.tokens = None
        self.threads = []

    def update_step(self):
        if self.once:
            tokens, _, _ = self.api.api_token_bucket.peek()
            with self.lock:
                if self.tokens is not None and tokens > self.tokens:
                    log().info("Tokens refilled, quitting")
                    return True
                self.tokens = tokens

        target_tokens = self.target_tokens

        success, _, _, _ = self.api.api_token_bucket.try_consume(
            1, leave=target_tokens)
        if not success:
            return True

        with self.api.begin():
            offset = get_int(self.api, self.KEY_OFFSET) or 0
            results = get_int(self.api, self.KEY_RESULTS)
            next_offset = offset + self.BLOCK_SIZE - 1
            if results and next_offset > results:
                next_offset = 0
            set_int(self.api, self.KEY_OFFSET, next_offset)

        log().info(
            "Trying update at offset %s, %s tokens left", offset,
            self.api.api_token_bucket.peek()[0])

        try:
            sr = self.api.getTorrents(
                results=2**31, offset=offset, consume_token=False)
        except ptp.WouldBlock:
            log().info("Out of tokens, quitting")
            return True
        except ptp.APIError as e:
            if e.code == e.CODE_CALL_LIMIT_EXCEEDED:
                log().debug("Call limit exceeded, quitting")
                return True
            else:
                raise

        with self.api.begin():
            set_int(self.api, self.KEY_RESULTS, sr.results)
            apply_contiguous_results_locked(self.api, offset, sr)

        return False

    def run(self):
        try:
            while True:
                try:
                    done = self.update_step()
                except:
                    log().exception("during update")
                    done = True
                if done:
                    if self.once:
                        break
                    else:
                        time.sleep(60)
        finally:
            log().debug("shutting down")

    def start(self):
        if self.threads:
            return
        for i in range(self.num_threads):
            t = threading.Thread(
                name="metadata-scraper-%d" % i, target=self.run, daemon=True)
            t.start()
            self.threads.append(t)

    def join(self):
        for t in self.threads:
            t.join()


class MetadataTipScraper(object):

    KEY_LAST = "tip_last_scraped"
    KEY_LAST_TS = "tip_last_scraped_ts"
    KEY_PAGE = "tip_scrape_page"
    KEY_NEWEST = "tip_scrape_newest"
    KEY_NEWEST_TS = "tip_scrape_newest_ts"

    def __init__(self, api, once=False):
        if api.username is None:
            raise ValueError("username not configured")
        if api.password is None:
            raise ValueError("password not configured")
        if api.passkey is None:
            raise ValueError("passkey not configured")

        self.api = api
        self.once = once
        self.thread = None

    def update_scrape_results_locked(self, sr, is_end):
        entries_by_time = apply_contiguous_results_locked(self.api, sr, is_end)

        last_scraped_id = get_int(self.api, self.KEY_LAST)
        last_scraped_ts = get_int(self.api, self.KEY_LAST_TS)
        newest_id = get_int(self.api, self.KEY_NEWEST)
        newest_ts = get_int(self.api, self.KEY_NEWEST_TS)

        if entries_by_time:
            oldest_in_page = entries_by_time[0]
            newest_in_page = entries_by_time[-1]
        else:
            oldest_in_page = None
            newest_in_page = None

        if newest_ts is None or (newest_in_page and (
                (newest_in_page.time, newest_in_page.id) >=
                (newest_ts, newest_id))):
            newest_id = newest_in_page.id
            newest_ts = newest_in_page.time

        done = False
        if is_end:
            log().info("We reached the oldest torrent entry.")
            done = True
        elif last_scraped_ts is not None and (
                (oldest_in_page.time, oldest_in_page.id) <=
                (last_scraped_ts, last_scraped_id)):
            log().info("Caught up. Current as of %s.", newest_id)
            done = True

        if done:
            set_int(self.api, self.KEY_LAST, newest_id)
            set_int(self.api, self.KEY_LAST_TS, newest_ts)
            set_int(self.api, self.KEY_NEWEST, None)
            set_int(self.api, self.KEY_NEWEST_TS, None)
        else:
            set_int(self.api, self.KEY_NEWEST, newest_id)
            set_int(self.api, self.KEY_NEWEST_TS, newest_ts)

        return done

    def scrape_step(self):
        # Our strategy is to order torrents by upload time ascending, and visit
        # them in reverse order. The reason for this awkward traversal is
        # that in almost all available orderings, a given torrent may move
        # either forward or backward in the list, if older torrents are deleted
        # or new ones are added; this means a torrent may jump between pages of
        # results during our traversal, and we may never see it. However with
        # upload time / ascending ordering, a torrent may only ever move to an
        # earlier index (when an older torrent is deleted). By traversing
        # backward, we can ensure we visit every torrent at least once.

        with self.api.db:
            page = get_int(self.api, self.KEY_PAGE)

        # If we're just starting, we don't know which page represents the end
        # of the list. We peek at the first page of results ordered by upload
        # time descending, and use the "total results" field to figure out
        # where the end of the list is.

        peek = (page is None)

        if peek:
            log().info("Peeking most recent torrents")
            sr = self.api.browse(order_by="timenoreseed", grouping=0)
            is_end = False
        else:
            log().info("Scraping page %s", page)
            sr = self.api.browse(
                order_by="timenoreseed", grouping=0, page=page,
                order_way="asc")
            is_end = (page <= 1)

        with self.api.begin():
            done = self.update_scrape_results_locked(sr, is_end)
            if done:
                set_int(self.api, self.KEY_PAGE, None)
            else:
                if peek:
                    page = (sr.results // 50) + 1
                else:
                    page -= 1
                set_int(self.api, self.KEY_PAGE, page)

        return done

    def run(self):
        try:
            while True:
                try:
                    done = self.scrape_step()
                except KeyboardInterrupt:
                    raise
                except:
                    log().exception("during scrape")
                    done = True
                if done:
                    if self.once:
                        break
                    else:
                        time.sleep(60)
        finally:
            log().debug("shutting down")

    def start(self):
        if self.thread:
            return
        t = threading.Thread(
            name="metadata-tip-scraper", target=self.run, daemon=True)
        t.start()
        self.thread = t

    def join(self):
        if self.thread:
            self.thread.join()


class TorrentFileScraper(object):

    DEFAULT_RESET_TIME = 3600

    def __init__(self, api, reset_time=None):
        if reset_time is None:
            reset_time = self.DEFAULT_RESET_TIME

        if api.username is None:
            raise ValueError("username not configured")
        if api.password is None:
            raise ValueError("password not configured")
        if api.passkey is None:
            raise ValueError("passkey not configured")

        self.api = api
        self.reset_time = reset_time

        self.thread = None
        self.ts = None
        self.queue = None
        self.last_reset_time = None

    def get_unfilled_ids(self):
        c = self.api.db.cursor().execute(
            "select torrent_entry.id "
            "from torrent_entry "
            "left join file_info on torrent_entry.id = file_info.id "
            "where file_info.id is null "
            "and torrent_entry.deleted = 0 "
            "and torrent_entry.updated_at > ? "
            "order by torrent_entry.updated_at", (self.ts,))
        for r in c:
            yield r

    def update_ts(self):
        r = self.api.db.cursor().execute(
            "select max(updated_at) from torrent_entry").fetchone()
        self.ts = r[0]

    def step(self):
        now = time.time()
        if (self.last_reset_time is None or
                now - self.last_reset_time > self.reset_time):
            self.ts = -1
            self.queue = Queue.PriorityQueue()
            self.last_reset_time = now

        with self.api.db:
            for id, in self.get_unfilled_ids():
                self.queue.put((-id, id))
            self.update_ts()

        try:
            _, id = self.queue.get_nowait()
        except Queue.Empty:
            id = None

        if id is not None:
            te = self.api.get_torrent_entry(id)
            _ = te.raw_torrent

        return id

    def run(self):
        try:
            while True:
                try:
                    id = self.step()
                except KeyboardInterrupt:
                    raise
                except:
                    log().exception("during scrape")
                    time.sleep(60)
                else:
                    if id is None:
                        time.sleep(1)
        finally:
            log().debug("shutting down")

    def start(self):
        if self.thread:
            return
        t = threading.Thread(
            name="torrent-file-scraper", target=self.run, daemon=True)
        t.start()
        self.thread = t

    def join(self):
        if self.thread:
            self.thread.join()


class SnatchlistScraper(object):
    """A long-lived daemon that updates the user's snatchlist.

    This daemon calls getUserSnatchlist with no filters and varying offset.

    This daemon will consume as many tokens from `api.api_token_bucket` as
    possible, up to a configured limit. The intent of this is to defer token
    use to `MetadataTipScraper`.

    Attributes:
        api: A `ptp.API` instance.
        target_tokens: A number of tokens to leave as leftover in
            `api.api_token_bucket`.
    """

    KEY_OFFSET = "snatchlist_scrape_next_offset"
    KEY_RESULTS = "snatchlist_scrape_last_results"

    BLOCK_SIZE = 10000

    DEFAULT_TARGET_TOKENS = 0
    DEFAULT_NUM_THREADS = 10

    def __init__(self, api, target_tokens=None, num_threads=None, once=False):
        if num_threads is None:
            num_threads = self.DEFAULT_THREADS
        if target_tokens is None:
            target_tokens = self.DEFAULT_TARGET_TOKENS

        if api.username is None:
            raise ValueError("username not configured")
        if api.password is None:
            raise ValueError("password not configured")
        if api.passkey is None:
            raise ValueError("passkey not configured")

        self.api = api
        self.target_tokens = target_tokens
        self.num_threads = num_threads
        self.once = once

        self.lock = threading.RLock()
        self.tokens = None
        self.threads = []

    def update_step(self):
        if self.once:
            tokens, _, _ = self.api.api_token_bucket.peek()
            with self.lock:
                if self.tokens is not None and tokens > self.tokens:
                    log().info("Tokens refilled, quitting")
                    return True
                self.tokens = tokens

        target_tokens = self.target_tokens

        success, _, _, _ = self.api.api_token_bucket.try_consume(
            1, leave=target_tokens)
        if not success:
            return True

        with self.api.begin():
            offset = get_int(self.api, self.KEY_OFFSET) or 0
            results = get_int(self.api, self.KEY_RESULTS)
            next_offset = offset + self.BLOCK_SIZE
            if results and next_offset > results:
                next_offset = 0
            set_int(self.api, self.KEY_OFFSET, next_offset)

        log().info(
            "Trying update at offset %s, %s tokens left", offset,
            self.api.api_token_bucket.peek()[0])

        try:
            sr = self.api.getUserSnatchlist(
                results=self.BLOCK_SIZE, offset=offset, consume_token=False)
        except ptp.WouldBlock:
            log().info("Out of tokens, quitting")
            return True
        except ptp.APIError as e:
            if e.code == e.CODE_CALL_LIMIT_EXCEEDED:
                log().debug("Call limit exceeded, quitting")
                return True
            else:
                raise

        with self.api.begin():
            set_int(self.api, self.KEY_RESULTS, sr.results)

        return False

    def run(self):
        try:
            while True:
                try:
                    done = self.update_step()
                except:
                    log().exception("during update")
                    done = True
                if done:
                    if self.once:
                        break
                    else:
                        time.sleep(60)
        finally:
            log().debug("shutting down")

    def start(self):
        if self.threads:
            return
        for i in range(self.num_threads):
            t = threading.Thread(
                name="snatchlist-scraper-%d" % i, target=self.run, daemon=True)
            t.start()
            self.threads.append(t)

    def join(self):
        for t in self.threads:
            t.join()

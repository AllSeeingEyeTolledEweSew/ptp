# The author disclaims copyright to this source code. Please see the
# accompanying UNLICENSE file.

import argparse
import logging
import sys

import ptp
from ptp import scrape as ptp_scrape


def log():
    return logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="count")
    parser.add_argument("--metadata", action="store_true")
    parser.add_argument("--metadata_tip", action="store_true")
    parser.add_argument("--torrent_files", action="store_true")
    parser.add_argument("--snatchlist", action="store_true")
    parser.add_argument("--site_log", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--once", action="store_true")
    ptp.add_arguments(parser, create_group=True)

    args = parser.parse_args()

    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        stream=sys.stdout, level=level,
        format="%(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s")

    if args.all:
        args.metadata = True
        args.metadata_tip = True
        args.torrent_files = True
        args.snatchlist = True
        args.site_log = True

    api = ptp.API.from_args(parser, args)

    scrapers = []
    if args.metadata:
        scrapers.append(ptp_scrape.MetadataScraper(api))
    if args.metadata_tip:
        scrapers.append(
            ptp_scrape.MetadataTipScraper(api, once=args.once))
    if args.torrent_files:
        if args.once:
            log().fatal("--torrent_files --once isn't implemented")
        scrapers.append(ptp_scrape.TorrentFileScraper(api))
    if args.snatchlist:
        scrapers.append(ptp_scrape.SnatchlistScraper(api))
    if args.site_log:
        scrapers.append(ptp_scrape.SiteLogScraper(api))

    if not scrapers:
        log().fatal("Nothing to do.")

    for scraper in scrapers:
        scraper.start()
    for scraper in scrapers:
        scraper.join()

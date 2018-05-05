# The author disclaims copyright to this source code. Please see the
# accompanying UNLICENSE file.

"""
A frontend to the PTP metadata API, tailored to maintain a local cache.
"""

import calendar
import contextlib
import cookielib
import json as json_lib
import hashlib
import html
import logging
import os
import re
import threading
import time
import urllib
import urlparse

import apsw
import better_bencode
import requests
import tbucket
import yaml


__all__ = [
    "TRACKER_REGEXES",
    "EPISODE_SEED_TIME",
    "EPISODE_SEED_RATIO",
    "SEASON_SEED_TIME",
    "SEASON_SEED_RATIO",
    "TORRENT_HISTORY_FRACTION",
    "add_arguments",
    "API",
    "SearchResult",
    "Error",
    "HTTPError",
    "TorrentEntry",
    "Artist",
    "FileInfo",
    "Movie",
]


"""A list of precompiled regexes to match PTP's trackers URLs."""
TRACKER_REGEXES = (
    re.compile(
        r"https?://please.passthepopcorn.me:2710/(?P<passkey>[a-z0-9]{32})"),
)

"""The minimum time to seed a torrent, in seconds."""
SEED_TIME = 48 * 3600
"""The minimum ratio to seed a torrent."""
SEED_RATIO = 1.0

"""The minimum fraction of downloaded data for it to count on your history."""
TORRENT_HISTORY_FRACTION = 0.95


def log():
    """Gets a module-level logger."""
    return logging.getLogger(__name__)


def _unescape(j):
    if type(j) is str:
        return html.unescape(j)
    if type(j) is list:
        return [_unescape(x) for x in j]
    if type(j) is dict:
        return {html.unescape(k): _unescape(v) for k, v in j.items()}
    return j


class Movie(object):
    """A movie on PTP.

    Attributes:
        api: The API instance to which this Movie is tied.
        id: The integer id of the Movie on PTP.
        imdb_id: The string imdb_id of the Movie. Typically in 7-digit
            '0123456' format.
        title: The movie title.
        cover: A URL to the portrait-style cover image.
        directors: A list of Artist objects.
        year: The movie's release year.
        tags: A list of string tags.
    """

    @classmethod
    def _create_schema(cls, api):
        """Initializes the database schema of an API instance.

        This should only be called at initialization time.

        This will perform a SAVEPOINT / DML / RELEASE command sequence on the
        database.

        Args:
            api: An API instance.
        """
        with api.db:
            c = api.db.cursor()
            c.execute(
                "create table if not exists movie ("
                "id integer primary key, "
                "imdb_id text, "
                "title text, "
                "cover text, "
                "year integer, "
                "updated_at integer not null, "
                "deleted tinyint not null default 0)")
            c.execute(
                "create index if not exists movie_on_updated_at "
                "on movie (updated_at)")
            c.execute(
                "create index if not exists movie_on_imdb_id "
                "on movie (imdb_id)")

            c.execute(
                "create table if not exists movie_director ("
                "movie_id integer not null, "
                "artist_id integer not null)")
            c.execute(
                "create unique index if not exists "
                "movie_director_movie_artist "
                "on movie_director (movie_id, artist_id)")
            c.execute(
                "create index if not exists "
                "movie_director_artist "
                "on movie_director (artist_id)")

            c.execute(
                "create table if not exists movie_tag ("
                "id integer not null, "
                "tag text not null)")
            c.execute(
                "create unique index if not exists movie_tag_id_tag "
                "on movie_tag (id, tag)")
            c.execute(
                "create index if not exists movie_tag_tag "
                "on movie_tag (tag)")

    @classmethod
    def _from_db(cls, api, id):
        """Creates a Movie from the cached metadata in an API instance.

        This just deserializes data from the API's database into a python
        object; it doesn't make any API calls.

        A new Movie object is always created from this call. Movie objects
        are not cached.

        This only calls SELECT on the database.

        Args:
            api: An API instance.
            id: A Movie id.

        Returns:
            A Movie with the data cached in the API's database, or None if the
                movie wasn't found in the cache.
        """
        c = api.db.cursor()
        row = c.execute(
            "select * from movie where id = ?", (id,)).fetchone()
        if not row:
            return None
        row = dict(zip((n for n, t in c.getdescription()), row))
        for k in ("updated_at", "deleted"):
            del row[k]
        row["tags"] = [
            tag for (tag,) in api.db.cursor().execute(
                "select tag from movie_tag where id = ?", (id,))]
        row["directors"] = [
            Artist._from_db(api, aid) for (aid,) in api.db.cursor().execute(
                "select artist_id from movie_director where movie_id = ?",
                (id,))]
        return cls(api, **row)

    @classmethod
    def _maybe_delete(cls, api, *ids, changestamp=None):
        """Mark a movie as deleted, if it has no non-deleted torrent entries.

        This will perform a SAVEPOINT / DML / RELEASE sequence on the database.

        Args:
            api: An API instance.
            *ids: A list of movie ids to consider for deletion.
            changestamp: An integer changestamp. If None, a new changestamp
                will be generated.
        """
        if not ids:
            return
        with api.db:
            rows = api.db.cursor().execute(
                "select id from movie "
                "where id in (%s) and not deleted and not exists ("
                "select id from torrent_entry "
                "where movie_id = movie.id and not deleted)" %
                ",".join(["?"] * len(ids)),
                tuple(ids))
            series_ids_to_delete = set()
            for id, in rows:
                series_ids_to_delete.add(id)
            if not series_ids_to_delete:
                return
            log().debug("Deleting movie: %s", sorted(series_ids_to_delete))
            if changestamp is None:
                changestamp = api.get_changestamp()
            api.db.cursor().executemany(
                "update movie set deleted = 1, updated_at = ? where id = ?",
                [(changestamp, id) for id in series_ids_to_delete])

    def __init__(self, api, id=None, imdb_id=None, title=None, year=None,
                 cover=None, tags=None, directors=None):
        self.api = api
        self.id = int(id)
        self.imdb_id = imdb_id
        self.title = title
        self.year = year
        self.cover = cover
        self.tags = tags
        self.directors = directors

    def serialize(self, changestamp=None):
        """Serialize the Movie's data to its API's database.

        This will write all the fields of this Movie to the "movie" table of
        the database of the `api`, as well as the intermediary tables
        "movie_tag" and "movie_director". If any fields have changed, the
        `updated_at` column of the "movie" table will be updated. If no data
        changes, the corresponding row in "movie" won't change at all.

        This performs a SAVEPOINT / DML / RELEASE sequence against the API.

        Args:
            changestamp: A changestamp from the API. If None, a new changestamp
                will be generated from the API.
        """
        with self.api.db:
            if changestamp is None:
                changestamp = self.api.get_changestamp()

            force_update = False

            self.api.db.cursor().executemany(
                "insert or ignore into movie_tag (id, tag) values (?, ?)",
                [(self.id, tag) for tag in self.tags])
            if self.api.db.changes():
                force_update = True
            self.api.db.cursor().execute(
                "delete from movie_tag where id = ? and tag not in (%s)" %
                ",".join("?" * len(self.tags)), [self.id] + list(self.tags))
            if self.api.db.changes():
                force_update = True

            for artist in self.directors:
                artist.serialize()
                self.api.db.cursor().execute(
                    "insert or ignore into movie_director "
                    "(movie_id, artist_id) values (?, ?)",
                    (self.id, artist.id))
                if self.api.db.changes():
                    force_update = True
            all_director_ids = set(
                id for (id,) in self.api.db.cursor().execute(
                    "select artist_id from movie_director where movie_id = ?",
                    (self.id,)))
            director_ids_to_delete = all_director_ids - set(
                a.id for a in self.directors)
            if director_ids_to_delete:
                self.api.db.cursor().executemany(
                    "delete from movie_director where movie_id = ? and "
                    "artist_id = ?",
                    [(self.id, id) for id in director_ids_to_delete])
                force_update = True

            params = {
                "imdb_id": self.imdb_id,
                "title": self.title,
                "year": self.year,
                "cover": self.cover,
                "deleted": 0,
            }
            insert_params = {"updated_at": changestamp, "id": self.id}
            insert_params.update(params)
            names = sorted(insert_params.keys())
            self.api.db.cursor().execute(
                "insert or ignore into movie (%(n)s) values (%(v)s)" %
                {"n": ",".join(names), "v": ",".join(":" + n for n in names)},
                insert_params)
            where_names = sorted(params.keys())
            set_names = sorted(params.keys())
            set_names.append("updated_at")
            update_params = dict(params)
            update_params["updated_at"] = changestamp
            update_params["id"] = self.id
            if force_update:
                where = "1"
            else:
                where = " or ".join(
                    "%(n)s is not :%(n)s" % {"n": n} for n in where_names)
            self.api.db.cursor().execute(
                "update movie set %(u)s where id = :id and (%(w)s)" %
                {"u": ",".join("%(n)s = :%(n)s" % {"n": n} for n in set_names),
                 "w": where}, update_params)

    def __repr__(self):
        return "<Movie %s \"%s\">" % (self.id, self.title)


class Artist(object):
    """An artist on PTP.

    Attributes:
        api: The API instance to which this Artist is tied.
        id: The integer id of the Artist on PTP.
        name: The name of the Artist.
    """

    @classmethod
    def _create_schema(cls, api):
        """Initializes the database schema of an API instance.

        This should only be called at initialization time.

        This will perform a SAVEPOINT / DML / RELEASE command sequence on the
        database.

        Args:
            api: An API instance.
        """
        with api.db:
            c = api.db.cursor()
            c.execute(
                "create table if not exists artist ("
                "id integer primary key, "
                "name text)")

    @classmethod
    def _from_db(cls, api, id):
        """Creates an Artist from the cached metadata in an API instance.

        This just deserializes data from the API's database into a python
        object; it doesn't make any API calls.

        A new Artist object is always created from this call. Artist objects
        are not cached.

        This only calls SELECT on the database.

        Args:
            api: An API instance.
            id: A Movie id.

        Returns:
            An Artist with the data cached in the API's database, or None if it
                wasn't found in the cache.
        """
        c = api.db.cursor()
        row = c.execute(
            "select * from artist where id = ?", (id,)).fetchone()
        if not row:
            return None
        row = dict(zip((n for n, t in c.getdescription()), row))
        return cls(api, **row)

    def __init__(self, api, id=None, name=None):
        self.api = api
        self.id = int(id)
        self.name = name

    def serialize(self):
        """Serialize the Artist's data to its API's database.

        This will write all the fields of this Movie to the "artist" table of
        the database of the `api`.

        This performs a SAVEPOINT / DML / RELEASE sequence against the API.
        """
        with self.api.db:
            self.api.db.cursor().execute(
                "insert or ignore into artist (id, name) values (?, ?)",
                (self.id, self.name))
            self.api.db.cursor().execute(
                "update artist set name = :name where id = :id and "
                "name is not :name", {"id": self.id, "name": self.name})

    def __repr__(self):
        return "<Artist %s \"%s\">" % (self.id, self.name)


class FileInfo(object):
    """Metadata about a particular file in a torrent on PTP.

    Attributes:
        index: The integer index of this file within the torrent metafile.
        length: The integer length of the file in bytes.
        path: The recommended pathname as a string, as it appears in the
            torrent metafile.
    """

    @classmethod
    def _from_db(cls, api, id):
        """Creates FileInfo objects from the cached metadata in an API
        instance.

        This just deserializes data from the API's database into a python
        object; it doesn't make any API calls.

        This function always creates new FileInfo objects. The objects are not
        cached.

        This calls SAVEPOINT / SELECT / RELEASE on the database.

        Args:
            api: An API instance.
            id: A torrent entry id.

        Returns:
            A tuple of FileInfo objects with the data cached in the API's
                database associated with a given torrent entry id, or an empty
                tuple if none were found.
        """
        with api.db:
            c = api.db.cursor()
            rows = c.execute(
                "select "
                "file_index as 'index', "
                "path, "
                "length "
                "from file_info "
                "where id = ?",
                (id,))
            return (cls(index=r[0], path=r[1], length=r[2]) for r in rows)

    @classmethod
    def _from_tobj(cls, tobj):
        """Generates FileInfo objects from a deserialized torrent metafile.

        Args:
            tobj: A deserialized torrent metafile; i.e. the result of
                `better_bencode.load*()`.

        Yields:
            FileInfo objects for the given torrent metafile, in the order they
                appear in the metafile.
        """
        ti = tobj[b"info"]
        values = []
        if b"files" in ti:
            for idx, fi in enumerate(ti[b"files"]):
                length = fi[b"length"]
                path_parts = [ti[b"name"]]
                path_parts.extend(list(fi[b"path"]))
                path = b"/".join(path_parts)
                yield cls(index=idx, path=path, length=length)
        else:
            yield cls(index=0, path=ti[b"name"], length=ti[b"length"])

    def __init__(self, index=None, path=None, length=None):
        self.index = index
        self.path = path
        self.length = length

    def __repr__(self):
        return "<FileInfo %s \"%s\">" % (self.index, self.path)


class Report(object):
    """Metadata about a user issue report for a torrent entry on PTP.

    Attributes:
        api: The API instance to which this Report is tied.
        torrent_entry: The TorrentEntry to which this Report is tied.
        comment: The comment text of this Report.
        time: The time (seconds since epoch) the report was submitted.
        type: The type of report. Common values include "Trump" or "Request
            Delete".
    """

    @classmethod
    def _create_schema(cls, api):
        """Initializes the database schema of an API instance.

        This should only be called at initialization time.

        This will perform a SAVEPOINT / DML / RELEASE command sequence on the
        database.

        Args:
            api: An API instance.
        """
        with api.db:
            c = api.db.cursor()
            c.execute(
                "create table if not exists report ("
                "id integer not null, "
                "comment text, "
                "time integer not null, "
                "type text not null)")
            c.execute(
                "create unique index if not exists report_id_time_type "
                "on report (id, time, type)")

    def __init__(self, api, torrent_entry, comment=None, time=None, type=None):
        self.api = api
        self.torrent_entry = torrent_entry
        self.comment = comment
        self.time = time
        self.type = type

    def serialize(self):
        """Serialize the Report's data to its API's database.

        This will write all the fields of this Report to the
        "report" table of the database of the `api`.

        This performs a SAVEPOINT / DML / RELEASE sequence against the API.
        """
        with self.api.db:
            self.api.db.cursor().execute(
                "insert or ignore into report_type (name) values (?)",
                (self.type,))
            type_id = self.api.db.cursor().execute(
                "select id from report_type where name = ?",
                (self.type,)).fetchone()[0]
            self.api.db.cursor().execute(
                "insert or replace into report "
                "(torrent_entry_id, comment, time, type_id) values "
                "(?, ?, ?, ?)",
                (self.torrent_entry.id, self.comment, self.time, type_id))


class TorrentEntry(object):
    """Metadata about a torrent on PTP.

    A TorrentEntry is named to distinguish it from a "torrent". A TorrentEntry
    is the tracker's metadata about a torrent, including at least its info
    hash; the word "torrent" may refer to either the metafile or the actual
    data files.

    Attributes:
        api: The API instance to which this TorrentEntry is tied.
        id: The integer id of the torrent entry on PTP.
        checked: Whether this torrent entry has been checked by staff as valid.
        codec: The name of the torrent's codec. Common values include "x264".
        container: The name of the torrent's container. Common values include
            "MKV".
        golden_popcorn: Whether this torrent entry has been granted "golden
            popcorn" status on PTP.
        leechers: The current number of clients leeching the torrent.
        quality: The "quality label" of the torrent entry. Common values
            include "Standard Definition", "High Definition" and "Ultra High
            Definition".
        release_name: The release name of the torrent as a string. This
            commonly obeys a particular format laid out by the scene.
        remasters: A list of remaster titles. Common values include "Remux",
            "With Commentary", and "Extended Edition".
        resolution: The resolution of the torrent. This is either one of
            several well-known values like "1080p" or "720p", or a "WxH" string
            like "720x406".
        scene: Whether this is a scene torrent.
        seeders: The current number of client seeding the torrent.
        size: The total size of the torrent in bytes.
        snatched: The number of times the torrent has been "snatched" (fully
            downloaded).
        source: The source of the torrent. Common values include "Blu-ray".
        time: The UNIX time (seconds since epoch) that this torrent was
            uploaded to the tracker.
    """

    @classmethod
    def _create_schema(cls, api):
        """Initializes the database schema of an API instance.

        This should only be called at initialization time.

        This will perform a SAVEPOINT / DML / RELEASE command sequence on the
        database.

        Args:
            api: An API instance.
        """
        with api.db:
            c = api.db.cursor()
            c.execute(
                "create table if not exists torrent_entry ("
                "id integer primary key, "
                "checked tinyint not null, "
                "codec text not null, "
                "container text not null, "
                "golden_popcorn tinyint not null, "
                "info_hash text, "
                "movie_id integer not null, "
                "quality text not null, "
                "release_name text not null, "
                "resolution text not null, "
                "scene tinyint not null, "
                "size integer not null, "
                "source text not null, "
                "time integer not null, "
                "raw_torrent_cached tinyint not null default 0, "
                "updated_at integer not null, "
                "deleted tinyint not null default 0)")
            c.execute(
                "create index if not exists torrent_entry_updated_at "
                "on torrent_entry (updated_at)")
            c.execute(
                "create index if not exists torrent_entry_on_info_hash "
                "on torrent_entry (info_hash)")
            c.execute(
                "create index if not exists torrent_entry_on_movie_id "
                "on torrent_entry (movie_id)")
            c.execute(
                "create index if not exists torrent_entry_codec "
                "on torrent_entry (codec)")
            c.execute(
                "create index if not exists torrent_entry_container "
                "on torrent_entry (container)")
            c.execute(
                "create index if not exists torrent_entry_quality "
                "on torrent_entry (quality)")
            c.execute(
                "create index if not exists torrent_entry_resolution "
                "on torrent_entry (resolution)")
            c.execute(
                "create index if not exists torrent_entry_source "
                "on torrent_entry (source)")

            c.execute(
                "create table if not exists torrent_entry_remaster ("
                "id integer not null, remaster text not null)")
            c.execute(
                "create unique index if not exists "
                "torrent_entry_remaster_id_name "
                "on torrent_entry_remaster (id, remaster)")
            c.execute(
                "create index if not exists torrent_entry_remaster_name "
                "on torrent_entry_remaster (remaster)")

            c.execute(
                "create table if not exists file_info ("
                "id integer not null, "
                "file_index integer not null, "
                "path text not null, "
                "length integer not null, "
                "updated_at integer not null)")
            c.execute(
                "create unique index if not exists file_info_id_index "
                "on file_info (id, file_index)")
            c.execute(
                "create index if not exists file_info_id_index_path "
                "on file_info (id, file_index, path)")
            c.execute(
                "create index if not exists file_info_updated_at "
                "on file_info (updated_at)")
            c.execute(
                "create table if not exists tracker_stats ("
                "id integer primary key,"
                "snatched integer not null, "
                "seeders integer not null, "
                "leechers integer not null)")

    @classmethod
    def _from_db(cls, api, id):
        """Creates a TorrentEntry from the cached metadata in an API instance.

        This just deserializes data from the API's database into a python
        object; it doesn't make any API calls.

        A new TorrentEntry object, and attached FileInfo and Movie objects, are
        always created from this function. None of these objects are cached for
        reuse.

        This calls SAVEPOINT / SELECT / RELEASE on the database.

        Args:
            api: An API instance.
            id: A TorrentEntry id.

        Returns:
            A TorrentEntry with the data cached in the API's database, or None
                if the group wasn't found in the cache.
        """
        with api.db:
            remasters = [
                name for (name,) in api.db.cursor().execute(
                    "select remaster from torrent_entry_remaster where id = ?",
                    (id,))]
            c = api.db.cursor()
            c.execute(
                "select "
                "torrent_entry.id as id, "
                "torrent_entry.checked as checked, "
                "torrent_entry.codec as codec, "
                "torrent_entry.container as container, "
                "torrent_entry.golden_popcorn as golden_popcorn, "
                "torrent_entry.info_hash as info_hash, "
                "tracker_stats.leechers as leechers, "
                "torrent_entry.quality as quality, "
                "torrent_entry.release_name as release_name, "
                "torrent_entry.resolution as resolution, "
                "torrent_entry.scene as scene, "
                "tracker_stats.seeders as seeders, "
                "torrent_entry.size as size, "
                "tracker_stats.snatched as snatched, "
                "torrent_entry.source as source, "
                "torrent_entry.time as time, "
                "torrent_entry.movie_id as movie_id "
                "from torrent_entry "
                "left outer join tracker_stats "
                "on tracker_stats.id = torrent_entry.id "
                "where torrent_entry.id = ?",
                (id,))
            row = c.fetchone()
            if not row:
                return None
            row = dict(zip((n for n, t in c.getdescription()), row))
            movie_id = row.pop("movie_id")
            movie = Movie._from_db(api, movie_id)
            return cls(api, remasters=remasters, movie=movie, **row)

    def __init__(self, api, id=None, checked=None, codec=None, container=None,
                 golden_popcorn=None, info_hash=None, leechers=None,
                 quality=None, release_name=None, remasters=None,
                 resolution=None, scene=None, seeders=None, size=None,
                 snatched=None, source=None, time=None, movie=None):
        self.api = api

        self.id = id
        self.checked = checked
        self.codec = codec
        self.container = container
        self.golden_popcorn = golden_popcorn
        self._info_hash = info_hash
        self.leechers = leechers
        self.quality = quality
        self.release_name = release_name
        self.remasters = remasters
        self.resolution = resolution
        self.scene = scene
        self.seeders = seeders
        self.size = size
        self.snatched = snatched
        self.source = source
        self.time = time
        self.movie = movie

        self._lock = threading.RLock()
        self._raw_torrent = None
        self._file_info = None

    def serialize(self, changestamp=None):
        """Serialize the TorrentEntry's data to its API's database.

        This will write all the fields of this TorrentEntry to the
        "torrent_entry" table of the database of the `api`. If any fields have
        changed, the `updated_at` column of the "torrent_entry" table will be
        updated. If no data changes, the corresponding row in "torrent_entry"
        won't change at all.

        The "tracker_stats" table is always updated.

        The "enum value" tables "codec", "container", "quality", "resolution",
        "source" and "remaster" will also be updated with new values if
        necessary.

        If the raw torrent has been cached, this function will also update the
        "file_info" table if necessary.

        This also calls `serialize()` on the associated `movie`.

        This performs a SAVEPOINT / DML / RELEASE sequence against the API.

        Args:
            changestamp: A changestamp from the API. If None, a new changestamp
                will be generated from the API.
        """
        file_info = None
        if self.raw_torrent_cached and not any(self.file_info_cached):
            file_info = list(FileInfo._from_tobj(self.torrent_object))
        force_update = False
        with self.api.db:
            force_update = False
            self.api.db.cursor().executemany(
                "insert or ignore into torrent_entry_remaster (id, remaster) "
                "values (?, ?)",
                [(self.id, remaster) for remaster in self.remasters])
            if self.api.db.changes():
                force_update = True
            self.api.db.cursor().execute(
                "delete from torrent_entry_remaster where id = ? "
                "and remaster not in (%s)" %
                ",".join("?" * len(self.remasters)),
                [self.id] + list(self.remasters))
            if self.api.db.changes():
                force_update = True

            r = self.api.db.cursor().execute(
                "select movie_id from torrent_entry where id = ?",
                (self.id,)).fetchone()
            old_movie_id = r[0] if r is not None else None

            if changestamp is None:
                changestamp = self.api.get_changestamp()
            self.movie.serialize(changestamp=changestamp)
            params = {
                "checked": self.checked,
                "codec": self.codec,
                "container": self.container,
                "golden_popcorn": self.golden_popcorn,
                "movie_id": self.movie.id,
                "quality": self.quality,
                "release_name": self.release_name,
                "resolution": self.resolution,
                "scene": self.scene,
                "size": self.size,
                "source": self.source,
                "time": self.time,
                "raw_torrent_cached": self.raw_torrent_cached,
                "deleted": 0,
            }
            if self._info_hash is not None:
                params["info_hash"] = self._info_hash
            insert_params = {
                    "id": self.id, "updated_at": changestamp, "info_hash":
                    self._info_hash}
            insert_params.update(params)
            names = sorted(insert_params.keys())
            self.api.db.cursor().execute(
                "insert or ignore into torrent_entry (%(n)s) values (%(v)s)" %
                {"n": ",".join(names), "v": ",".join(":" + n for n in names)},
                insert_params)
            where_names = sorted(params.keys())
            set_names = sorted(params.keys())
            set_names.append("updated_at")
            update_params = dict(params)
            update_params["updated_at"] = changestamp
            update_params["id"] = self.id
            if force_update:
                where = "1"
            else:
                where = " or ".join(
                    "%(n)s is not :%(n)s" % {"n": n} for n in where_names)
            self.api.db.cursor().execute(
                "update torrent_entry set %(u)s "
                "where id = :id and (%(w)s)" %
                {"u": ",".join("%(n)s = :%(n)s" % {"n": n} for n in set_names),
                 "w": where},
                update_params)
            self.api.db.cursor().execute(
                "insert or replace into tracker_stats "
                "(id, snatched, seeders, leechers) values (?, ?, ?, ?)",
                (self.id, self.snatched, self.seeders, self.leechers))
            if old_movie_id != self.movie.id:
                Movie._maybe_delete(
                    self.api, old_movie_id, changestamp=changestamp)

            if file_info:
                values = [
                    (self.id, fi.index, fi.path, fi.length, changestamp)
                    for fi in file_info]
                self.api.db.cursor().executemany(
                    "insert or ignore into file_info "
                    "(id, file_index, path, length, updated_at) values "
                    "(?, ?, ?, ?, ?)", values)

    @property
    def link(self):
        """A link to the torrent metafile."""
        return self.api._mk_url(
            "https", self.api.HOST, "/torrents.php", action="download",
            authkey=self.api.authkey, torrent_pass=self.api.passkey,
            id=self.id)

    @property
    def raw_torrent_path(self):
        """The path to the cached torrent metafile."""
        return os.path.join(
            self.api.raw_torrent_cache_path, "%s.torrent" % self.id)

    @property
    def raw_torrent_cached(self):
        """Whether or not the torrent metafile has been locally cached."""
        return os.path.exists(self.raw_torrent_path)

    def _got_raw_torrent(self, raw_torrent):
        """Callback that should be called when we receive the torrent metafile.

        This will write the torrent metafile to `raw_torrent_path`.

        Will call `serialize()`.

        Args:
            raw_torrent: The bencoded torrent metafile.
        """
        self._raw_torrent = raw_torrent
        if self.api.store_raw_torrent:
            if not os.path.exists(os.path.dirname(self.raw_torrent_path)):
                os.makedirs(os.path.dirname(self.raw_torrent_path))
            with open(self.raw_torrent_path, mode="wb") as f:
                f.write(self._raw_torrent)
        if self._info_hash is None:
            self._info_hash = hashlib.sha1(better_bencode.dumps(
                self.torrent_object[b"info"])).hexdigest().encode()
        while True:
            try:
                with self.api.begin():
                    self.serialize()
            except apsw.BusyError:
                log().warning(
                    "BusyError while trying to serialize, will retry")
            else:
                break
        return self._raw_torrent

    @property
    def raw_torrent(self):
        """The torrent metafile.

        If the torrent metafile isn't locally cached, it will be fetched from
        BTN and cached.
        """
        with self._lock:
            if self._raw_torrent is not None:
                return self._raw_torrent
            if self.raw_torrent_cached:
                with open(self.raw_torrent_path, mode="rb") as f:
                    self._raw_torrent = f.read()
                if self._info_hash is None:
                    self._info_hash = hashlib.sha1(better_bencode.dumps(
                        self.torrent_object[b"info"])).hexdigest().encode()
                    while True:
                        try:
                            with self.api.begin():
                                self.serialize()
                        except apsw.BusyError:
                            log().warning(
                                "BusyError while trying to serialize, will retry")
                        else:
                            break
                return self._raw_torrent
            log().debug("Fetching raw torrent for %s", repr(self))
            response = self.api._call(
                "get", "/torrents.php", params=dict(
                    action="download", authkey=self.api.authkey,
                    torrent_pass=self.api.passkey, id=self.id))
            if self._check_response_indicates_deletion(response):
                return None
            self._got_raw_torrent(response.content)
            return self._raw_torrent

    def _check_response_indicates_deletion(self, response):
        u = urllib.parse.urlparse(response.url)
        if u.path == "/log.php" and "was deleted" in response.text:
            self._delete()
            return True
        return False

    def _delete(self, changestamp=None):
        with self.api.begin():
            if changestamp is None:
                changestamp = self.api.get_changestamp()
            self.api.db.cursor().execute(
                "update torrent_entry set deleted = 1, updated_at = ? "
                "where id = ? and not deleted",
                (changestamp, self.id))
            Movie._maybe_delete(
                self.api, self.movie.id, changestamp=changestamp)

    @property
    def file_info_cached(self):
        """A tuple of cached FileInfo objects from the database."""
        with self._lock:
            if self._file_info is None:
                self._file_info = FileInfo._from_db(self.api, self.id)
            return self._file_info

    @property
    def torrent_object(self):
        """The torrent metafile, deserialized via `better_bencode.load*()`."""
        return better_bencode.loads(self.raw_torrent)

    def __repr__(self):
        return "<TorrentEntry %d \"%s\">" % (self.id, self.release_name)


class SearchResult(object):
    """The result of a call to `API.browse`.

    Attributes:
        results: The total integer number of torrents matched by the filters in
            the getTorrents call.
        torrents: A list of TorrentEntry objects.
    """

    def __init__(self, results, movies):
        self.results = int(results)
        self.movies = movies or ()



class MovieResult(object):

    def __init__(self, movie, torrents):
        self.movie = movie
        self.torrents = torrents


class Error(Exception):
    """Top-level class for exceptions in this module."""

    pass


class LoginError(Error):

    pass


class PermanentFailure(Error):

    pass


class WouldBlock(Error):

    pass


def add_arguments(parser, create_group=True):
    """A helper function to add standard command-line options to make an API
    instance.

    This adds the following command-line options:
        --ptp_cache_path: The path to the PTP cache.

    Args:
        parser: An argparse.ArgumentParser.
        create_group: Whether or not to create a subgroup for PTP API options.
            Defaults to True.

    Returns:
        Either the argparse.ArgumentParser or the subgroup that was implicitly
            created.
    """
    if create_group:
        target = parser.add_argument_group("PTP API options")
    else:
        target = parser

    target.add_argument("--ptp_cache_path", type=str)

    return target


class API(object):
    """An API to BTN, and associated data cache.

    Attributes:
        cache_path: The path to the data cache directory.
        username: The string username.
        password: The user's password.
        auth: The user's "auth" string.
        passkey: The user's PTP passkey.
        authkey: The user's "authkey" string.
        user: The user's numeric user id.
        token_rate: The `rate` parameter to `token_bucket`.
        token_period: The `period` parameter to `token_bucket`.
        store_raw_torrent: Whether or not to cache torrent metafiles to disk,
            whenever they are fetched.
        token_bucket: An instance of tbucket.TokenBucket which controls access
            to most HTTP requests to PTP, such as when downloading torrent
            metafiles.
    """

    """The protocol scheme used to access the API."""
    SCHEME = "https"

    """The hostname of the PTP website."""
    HOST = "passthepopcorn.me"

    DEFAULT_TOKEN_RATE = 20
    DEFAULT_TOKEN_PERIOD = 100

    @classmethod
    def from_args(cls, parser, args):
        """Helper function to create an API from command-line arguments.

        This is intended to be used with a parser which has been configured
        with `add_arguments()`.

        Args:
            parser: An argparse.ArgumentParser.
            args: An argparse.Namespace resulting from calling `parse_args()`
                on the given parser.
        """
        return cls(cache_path=args.ptp_cache_path)

    def __init__(self, username=None, password=None, passkey=None, user=None,
                 authkey=None, token_bucket=None, cache_path=None,
                 store_raw_torrent=None, auth=None):
        if cache_path is None:
            cache_path = os.path.expanduser("~/.ptp")

        self.cache_path = cache_path

        if os.path.exists(self.config_path):
            with open(self.config_path) as f:
                config = yaml.load(f)
        else:
                config = {}

        self.username = config.get("username")
        self.password = config.get("password")
        self.passkey = config.get("passkey")
        self.auth = config.get("auth")
        self.authkey = config.get("authkey")
        self.user = config.get("user")
        self.token_rate = config.get("token_rate")
        self.token_period = config.get("token_period")
        self.store_raw_torrent = config.get("store_raw_torrent")

        if username is not None:
            self.username = username
        if password is not None:
            self.password = password
        if auth is not None:
            self.auth = auth
        if passkey is not None:
            self.passkey = passkey
        if authkey is not None:
            self.authkey = authkey
        if store_raw_torrent is not None:
            self.store_raw_torrent = store_raw_torrent

        if self.token_rate is None:
            self.token_rate = self.DEFAULT_TOKEN_RATE
        if self.token_period is None:
            self.token_period = self.DEFAULT_TOKEN_PERIOD

        if token_bucket is not None:
            self.token_bucket = token_bucket
        else:
            self.token_bucket = tbucket.TokenBucket(
                self.user_db_path, self.username, self.token_rate,
                self.token_period)

        self._local = threading.local()
        self._db = None
        self._lock = threading.Lock()
        self._cookiejar = None
        self._login_lock = threading.RLock()
        self._login_retries = 5
        self._permanent_failure = False

    @property
    def metadata_db_path(self):
        """The path to metadata.db."""
        if self.cache_path:
            return os.path.join(self.cache_path, "metadata.db")
        return None

    @property
    def user_db_path(self):
        """The path to user.db."""
        if self.cache_path:
            return os.path.join(self.cache_path, "user.db")
        return None

    @property
    def config_path(self):
        """The path to config.yaml."""
        if self.cache_path:
            return os.path.join(self.cache_path, "config.yaml")
        return None

    @property
    def raw_torrent_cache_path(self):
        """The path to the directory of cached torrent metafiles."""
        if self.cache_path:
            return os.path.join(self.cache_path, "torrents")

    @property
    def cookiejar_path(self):
        if self.cache_path:
            return os.path.join(self.cache_path, "cookies.txt")

    @property
    def db(self):
        """A thread-local apsw.Connection.

        The primary database will be the metadata database. The user database
        will be attached under the schema name "user".
        """
        db = getattr(self._local, "db", None)
        if db is not None:
            return db
        if self.metadata_db_path is None:
            return None
        if not os.path.exists(os.path.dirname(self.metadata_db_path)):
            os.makedirs(os.path.dirname(self.metadata_db_path))
        db = apsw.Connection(self.metadata_db_path)
        db.setbusytimeout(120000)
        self._local.db = db
        c = db.cursor()
        c.execute(
            "attach database ? as user", (self.user_db_path,))
        with db:
            Movie._create_schema(self)
            Artist._create_schema(self)
            Report._create_schema(self)
            TorrentEntry._create_schema(self)
            c.execute(
                "create table if not exists user.global ("
                "  name text not null,"
                "  value text not null)")
            c.execute(
                "create unique index if not exists user.global_name "
                "on global (name)")
            c.execute(
                "create table if not exists user.snatchlist ("
                "id integer primary key, "
                "uploaded integer not null, "
                "downloaded integer not null, "
                "downloaded_fraction float not null, "
                "ratio float not null, "
                "last_action integer not null, "
                "seeding tinyint not null, "
                "seed_time_left integer not null, "
                "seed_time integer not null, "
                "hnr tinyint not null)")
        c.execute("pragma journal_mode=wal").fetchall()
        return db

    @property
    def cookiejar(self):
        with self._lock:
            if self._cookiejar is None:
                if not os.path.exists(os.path.dirname(self.cookiejar_path)):
                    os.makedirs(os.path.dirname(self.cookiejar_path))
                self._cookiejar = cookielib.LWPCookieJar(self.cookiejar_path)
                try:
                    self._cookiejar.load()
                except IOError:
                    pass
            return self._cookiejar

    @property
    def session(self):
        session = getattr(self._local, "session", None)
        if session is not None:
            return session
        session = requests.Session()
        session.cookies = self.cookiejar
        self._local.session = session
        return session

    @contextlib.contextmanager
    def begin(self, mode="immediate"):
        """Gets a context manager for a BEGIN IMMEDIATE transaction.

        Args:
            mode: The transaction mode. This will be directly used in the
                command to begin the transaction: "BEGIN <mode>". Defaults to
                "IMMEDIATE".

        Returns:
            A context manager for the transaction. If the context succeeds, the
                context manager will issue COMMIT. If it fails, the manager
                will issue ROLLBACK.
        """
        self.db.cursor().execute("begin %s" % mode)
        try:
            yield
        except:
            self.db.cursor().execute("rollback")
            raise
        else:
            self.db.cursor().execute("commit")

    def _mk_url(self, scheme, host, path, **qdict):
        query = urlparse.urlencode(qdict)
        return urlparse.urlunparse((scheme, host, path, None, query, None))

    @property
    def announce_urls(self):
        """Yields all user-specific announce URLs currently used by BTN."""
        yield self._mk_url(
            "http", "please.passthepopcorn.me", "%s/announce" % self.passkey)

    def login(self):
        r = self._call(
            "post", "/ajax.php", params=dict(action="login"), data=dict(
                username=self.username, password=self.password,
                passkey=self.passkey, keeplogged=1, login="Login"))
        if r.json()["Result"] != "Ok":
            raise LoginError(r.text)

    def _get(self, path, **kwargs):
        """A helper function to make a normal HTTP GET call to the PTP site.

        This will consume a token from `token_bucket`, blocking if necessary.

        Args:
            path: The HTTP path to the URL to call.
            **params: A dictionary of query parameters.

        Returns:
            The `requests.response`.
        """
        return self._call("get", path, **kwargs)

    def _call(self, method, path, leave_tokens=None, block_on_token=None,
              consume_token=None, **kwargs):
        """A helper function to make a normal HTTP call to the PTP site.

        This may consume a token from `token_bucket`, blocking if necessary.

        Args:
            method: A `requests.method` to use when calling (i.e., either
                `requests.get` or `requests.post`).
            path: The HTTP path to the URL to call.
            leave_tokens: Block until we would be able to leave at least this
                many tokens in `token_bucket`, after one is consumed.
                Defaults to 0.
            block_on_token: Whether or not to block waiting for a token. If
                False and no tokens are available, `WouldBlock` is raised.
                Defaults to True.
            consume_token: Whether or not to consume a token at all. Defaults
                to True. This should only be False when you are handling token
                management outside this function.
            **kwargs: A kwargs dictionary to pass to the method.

        Returns:
            The `requests.response`.
        """
        if block_on_token is None:
            block_on_token = True
        if consume_token is None:
            consume_token = True

        if consume_token and self.token_bucket:
            if block_on_token:
                self.token_bucket.consume(1, leave=leave_tokens)
            else:
                success, _, _, _ = self.token_bucket.try_consume(
                    1, leave=leave_tokens)
                if not success:
                    raise WouldBlock()

        url = self._mk_url(self.SCHEME, self.HOST, path)
        response = getattr(self.session, method)(url, **kwargs)
        response.raise_for_status()

        if len(response.content) < 100:
            log().debug("-> %r", response.content)
        else:
            log().debug("-> %.97r...", response.content)

        return response

    def permanent_failure(self):
        return self._permanent_failure

    def try_login(self):
        with self._login_lock:
            if self._login_retries <= 0:
                raise PermanentFailure()
            self._login_retries -= 1
            try:
                self.login()
            except:
                if self._login_retries == 0:
                    self._permanent_failure = True

    def _call_authed(self, method, path, leave_tokens=None,
                     block_on_token=None, consume_token=None, **kwargs):
        if self.permanent_failure():
            raise PermanentFailure()
        while True:
            r = self._call(
                method, path, leave_tokens=leave_tokens,
                block_on_token=block_on_token, consume_token=consume_token,
                **kwargs)
            self.cookiejar.save()
            u = urllib.parse.urlparse(r.url)
            if u.path != "/login.php":
                return r
            self.try_login()

    def get_global(self, name):
        """Gets a value from the "global" table in the user database.

        Values in the "global" are stored with BLOB affinity, so the return
        value may be of any data type that can be stored in SQLite.

        This function only issues SELECT against the database; no transaction
        is used.

        Args:
            name: The string name of the global value entry.

        Returns:
            The value from the "global" table, or None if no matching row
                exists.
        """
        row = self.db.cursor().execute(
            "select value from user.global where name = ?", (name,)).fetchone()
        return row[0] if row else None

    def set_global(self, name, value):
        """Sets a value in the "global" table in the user database.

        This function issues a SAVEPOINT / DML / RELEASE sequence to the
        database.

        Args:
            name: The string name of the global value entry.
            value: The value of the global value entry. May be any data type
                that can be coerced in SQLite.
        """
        with self.db:
            self.db.cursor().execute(
                "insert or replace into user.global (name, value) "
                "values (?, ?)",
                (name, value))

    def delete_global(self, name):
        """Deletes a value from the "global" table in the user database.

        This function issues a SAVEPOINT / DML / RELEASE sequence to the
        database.

        Args:
            name: The string name of the global value entry.
        """
        with self.db:
            self.db.cursor().execute(
                "delete from user.global where name = ?", (name,))

    def get_changestamp(self):
        """Gets a new changestamp from the increasing sequence in the database.

        This function issues a SAVEPOINT / DML / RELEASE sequence to the
        database.

        Returns:
            An integer changestamps, unique and larger than the result of any
                previous call to `get_changestamp()` for this database.
        """
        assert not self.db.getautocommit()
        r = self.db.cursor().execute(
            "select max(updated_at) from ("
            "select max(updated_at) updated_at from torrent_entry union all "
            "select max(updated_at) updated_at from movie)").fetchone()
        if r:
            return r[0] + 1
        else:
            return 0

    def browse_json(self, leave_tokens=None, block_on_token=None,
                    consume_token=None, **kwargs):
        """Access the /torrents.php endpoint, and return the result as JSON.

        The `json=noredirect` parameter is automatically added.

        Args:
            leave_tokens: Block until we would be able to leave at least this
                many tokens in `api_token_bucket`, after one is consumed.
                Defaults to 0.
            block_on_token: Whether or not to block waiting for a token. If
                False and no tokens are available, `WouldBlock` is raised.
                Defaults to True.
            consume_token: Whether or not to consume a token at all. Defaults
                to True. This should only be False when you are handling token
                management outside this function.
            **kwargs: A dictionary of query parameters to /torrents.php.

        Returns:
            A dictionary representing the JSON structure returned by
                /torrents.php.

        Raises:
            WouldBlock: When block_on_token is False and no tokens are
                available.
            HTTPError: If there was an HTTP-level error.
        """
        kwargs["json"] = "noredirect"
        bj = self._call_authed(
            "get", "/torrents.php", params=kwargs, leave_tokens=leave_tokens,
            block_on_token=block_on_token, consume_token=consume_token).json()
        bj = _unescape(bj)
        return bj

    def _torrent_entry_from_json(self, tj):
        """Create a TorrentEntry from parsed JSON.

        Args:
            tj: A parsed JSON object, as returned from
                /torrents.php?json=noredirect.

        Returns:
            A TorrentEntry object.
        """
        if tj.get("RemasterTitle"):
            remasters = tj["RemasterTitle"].split(" / ")
        else:
            remasters = []
        return TorrentEntry(
            self, id=int(tj["Id"]), checked=bool(tj["Checked"]),
            codec=tj["Codec"], container=tj["Container"],
            golden_popcorn=bool(tj["GoldenPopcorn"]),
            leechers=int(tj["Leechers"]), quality=tj["Quality"],
            release_name=tj["ReleaseName"], remasters=remasters,
            resolution=tj["Resolution"], scene=bool(tj["Scene"]),
            seeders=int(tj["Seeders"]), size=int(tj["Size"]),
            snatched=int(tj["Snatched"]), source=tj["Source"],
            time=calendar.timegm(time.strptime(
                tj["UploadTime"], "%Y-%m-%d %H:%M:%S")))

    def _movie_result_from_json(self, m):
        directors = [
            Artist(self, id=a["Id"], name=a["Name"])
            for a in m.get("Directors", [])]
        movie = Movie(
            self, id=int(m["GroupId"]), title=m["Title"], cover=m["Cover"],
            year=int(m["Year"]), imdb_id=m.get("ImdbId"), tags=m["Tags"],
            directors=directors)
        torrents = []
        for tj in m["Torrents"]:
            torrent = self._torrent_entry_from_json(tj)
            torrent.movie = movie
            torrents.append(torrent)
        return MovieResult(movie, torrents)

    def browse(self, leave_tokens=None, block_on_token=None,
               consume_token=None, **kwargs):
        """Access the /torrents.php endpoint.

        The `json=noredirect` parameter is automatically added.

        Args:
            leave_tokens: Block until we would be able to leave at least this
                many tokens in `api_token_bucket`, after one is consumed.
                Defaults to 0.
            block_on_token: Whether or not to block waiting for a token. If
                False and no tokens are available, `WouldBlock` is raised.
                Defaults to True.
            consume_token: Whether or not to consume a token at all. Defaults
                to True. This should only be False when you are handling token
                management outside this function.
            **kwargs: A dictionary of query parameters.
                for filter semantics.

        Returns:
            A `SearchResult` object.

        Raises:
            WouldBlock: When block_on_token is False and no tokens are
                available.
        """
        sj = self.browse_json(
            leave_tokens=leave_tokens, block_on_token=block_on_token,
            consume_token=consume_token, **kwargs)
        movie_results = [
            self._movie_result_from_json(m) for m in sj["Movies"]]
        sr = SearchResult(int(sj["TotalResults"]), movie_results)
        while True:
            try:
                with self.begin():
                    changestamp = self.get_changestamp()
                    for movie in sr.movies:
                        for te in movie.torrents:
                            te.serialize(changestamp=changestamp)
            except apsw.BusyError:
                log().warning(
                    "BusyError while trying to serialize, will retry")
            else:
                break
        return sr

    def get_torrent_entry(self, id):
        """Get a `TorrentEntry` from the local cache.

        Args:
            id: The id of the torrent entry on BTN.

        Returns:
            A `TorrentEntry`, or None if the given id is not found in the local
                cache.
        """
        return TorrentEntry._from_db(self, id)

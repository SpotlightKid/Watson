# -*- coding: utf-8 -*-

import os
import json

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

import arrow
import click
import pkg_resources

from .config import ConfigParser
from .frames import Frames
from .version import version as __version__  # noqa


class WatsonError(RuntimeError):
    pass


class ConfigurationError(WatsonError, configparser.Error):
    pass


class Watson(object):
    def __init__(self, **kwargs):
        """
        :param frames: If given, should be a list representating the
                        frames.
                        If not given, the value is extracted
                        from the frames file.
        :type frames: list

        :param current: If given, should be a dict representating the
                        current frame.
                        If not given, the value is extracted
                        from the state file.
        :type current: dict

        :param config_dir: If given, the directory where the configuration
                           files will be
        """
        self._current = None
        self._old_state = None
        self._frames = None
        self._last_sync = None
        self._config = None
        self._config_changed = False

        self._dir = (kwargs.pop('config_dir', None) or
                     click.get_app_dir('watson'))

        self.config_file = os.path.join(self._dir, 'config')
        self.frames_file = os.path.join(self._dir, 'frames')
        self.state_file = os.path.join(self._dir, 'state')
        self.last_sync_file = os.path.join(self._dir, 'last_sync')

        if 'frames' in kwargs:
            self.frames = kwargs['frames']

        if 'current' in kwargs:
            self.current = kwargs['current']

        if 'last_sync' in kwargs:
            self.last_sync = kwargs['last_sync']

    def _load_json_file(self, filename, type=dict):
        """
        Return the content of the the given JSON file.
        If the file doesn't exist, return an empty instance of the
        given type.
        """
        try:
            with open(filename) as f:
                return json.load(f)
        except IOError:
            return type()
        except ValueError as e:
            # If we get an error because the file is empty, we ignore
            # it and return an empty dict. Otherwise, we raise
            # an exception in order to avoid corrupting the file.
            if os.path.getsize(filename) == 0:
                return type()
            else:
                raise WatsonError(
                    "Invalid JSON file {}: {}".format(filename, e)
                )
        except Exception as e:
            raise WatsonError(
                "Unexpected error while loading JSON file {}: {}".format(
                    filename, e
                )
            )

    def _parse_date(self, date):
        return arrow.Arrow.utcfromtimestamp(date).to('local')

    def _format_date(self, date):
        if not isinstance(date, arrow.Arrow):
            date = arrow.get(date)

        return date.timestamp

    @property
    def config(self):
        """
        Return Watson's config as a ConfigParser object.
        """
        if not self._config:
            try:
                config = ConfigParser()
                config.read(self.config_file)
            except configparser.Error as e:
                raise ConfigurationError(
                    "Cannot parse config file: {}".format(e))

            self._config = config

        return self._config

    @config.setter
    def config(self, value):
        """
        Set a ConfigParser object as the current configuration.
        """
        self._config = value
        self._config_changed = True

    def save(self):
        """
        Save the state in the appropriate files. Create them if necessary.
        """
        try:
            if not os.path.isdir(self._dir):
                os.makedirs(self._dir)

            if self._current is not None and self._old_state != self._current:
                if self.is_started:
                    current = {
                        'project': self.current['project'],
                        'start': self._format_date(self.current['start']),
                        'tags': self.current['tags'],
                    }
                else:
                    current = {}

                with open(self.state_file, 'w+') as f:
                    json.dump(current, f, indent=1, ensure_ascii=False)

            if self._frames is not None and self._frames.changed:
                with open(self.frames_file, 'w+') as f:
                    json.dump(self.frames.dump(), f, indent=1,
                              ensure_ascii=False)

            if self._config_changed:
                with open(self.config_file, 'w+') as f:
                    self.config.write(f)

            if self._last_sync is not None:
                with open(self.last_sync_file, 'w+') as f:
                    json.dump(self._format_date(self.last_sync), f)
        except OSError as e:
            raise WatsonError(
                "Impossible to write {}: {}".format(e.filename, e)
            )

    @property
    def frames(self):
        if self._frames is None:
            self.frames = self._load_json_file(self.frames_file, type=list)

        return self._frames

    @frames.setter
    def frames(self, frames):
        self._frames = Frames(frames)

    @property
    def current(self):
        if self._current is None:
            self.current = self._load_json_file(self.state_file)

        if self._old_state is None:
            self._old_state = self._current

        return dict(self._current)

    @current.setter
    def current(self, value):
        if not value or 'project' not in value:
            self._current = {}

            if self._old_state is None:
                self._old_state = {}

            return

        start = value.get('start', arrow.now())

        if not isinstance(start, arrow.Arrow):
            start = self._parse_date(start)

        self._current = {
            'project': value['project'],
            'start': start,
            'tags': value.get('tags') or []
        }

        if self._old_state is None:
            self._old_state = self._current

    @property
    def last_sync(self):
        if self._last_sync is None:
            self.last_sync = self._load_json_file(
                self.last_sync_file, type=int
            )

        return self._last_sync

    @last_sync.setter
    def last_sync(self, value):
        if not value:
            self._last_sync = arrow.get(0)
            return

        if not isinstance(value, arrow.Arrow):
            value = self._parse_date(value)

        self._last_sync = value

    @property
    def is_started(self):
        return bool(self.current)

    def start(self, project, tags=None):
        if not project:
            raise WatsonError("No project given.")

        if self.is_started:
            raise WatsonError(
                "Project {} is already started.".format(
                    self.current['project']
                )
            )

        self.current = {'project': project, 'tags': tags}
        return self.current

    def stop(self):
        if not self.is_started:
            raise WatsonError("No project started.")

        old = self.current
        frame = self.frames.add(
            old['project'], old['start'], arrow.now(), tags=old['tags']
        )
        self.current = None

        return frame

    def cancel(self):
        if not self.is_started:
            raise WatsonError("No project started.")

        old_current = self.current
        self.current = None
        return old_current

    @property
    def projects(self):
        """
        Return the list of all the existing projects, sorted by name.
        """
        return sorted(set(self.frames['project']))

    @property
    def tags(self):
        """
        Return the list of the tags, sorted by name.
        """
        return sorted(set(tag for tags in self.frames['tags'] for tag in tags))

    def _load_backend(self, name=None):
        if not name:
            name = (self.config.get('backend', 'name')
                    if self.config.has_option('backend', 'name')
                    else 'artichio')

        if not name:
            raise WatsonError("Sync backend name must not be empty.")

        for plugin in pkg_resources.iter_entry_points('watson.sync'):
            if plugin.name == name:
                try:
                    return plugin.load()
                except Exception as exc:
                    raise WatsonError(
                        "Sync backend '{}' failed to load: {}".format(
                            name, exc
                        )
                    )
        else:
            raise WatsonError(
                "Sync backend '{}' is not installed.".format(name))

    def pull(self):
        backend = self._load_backend()(self.config)
        last_sync = self.last_sync
        frames = []

        for frame in backend.pull(last_sync):
            if frame.id not in self.frames or frame.updated_at > last_sync:
                self.frames[frame.id] = frame
                frames.append(frame)
            # XXX: what about deleted frames?

        return frames

    def push(self, last_pull):
        backend = self._load_backend()(self.config)
        frames = (frame for frame in self.frames
                  if last_pull > frame.updated_at > self.last_sync)
        # XXX: what about deleted frames?
        return backend.push(frames)

    def merge_report(self, frames_with_conflict):
        conflict_file_frames = Frames(self._load_json_file(
                                      frames_with_conflict, type=list))
        conflicting = []
        merging = []

        for conflict_frame in conflict_file_frames:
            try:
                original_frame = self.frames[conflict_frame.id]

                if original_frame != conflict_frame:
                    # frame from conflict frames file conflicts with frame
                    # from original frames file
                    conflicting.append(conflict_frame)

            except KeyError:
                # conflicting frame doesn't exist in original frame
                merging.append(conflict_frame)

        return conflicting, merging

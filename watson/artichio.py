# -*- coding: utf-8 -*-
"""Watson synchronization backend plugin for artich.io."""

from __future__ import absolute_import, unicode_literals

import json
import requests

from .frames import Frame
from .watson import ConfigurationError, WatsonError


__all__ = ('ArtichIOSync',)


class ArtichIOSync(object):
    """Pushes and pulls watson frames from and to the artich.io service."""

    def __init__(self, config):
        self.config = config

    def _get_request_info(self, route):
        backend_url = self.config.get('backend', 'url')
        token = self.config.get('backend', 'token')

        if backend_url and token:
            dest = "{}/{}/".format(
                backend_url.rstrip('/'),
                route.strip('/')
            )
        else:
            raise ConfigurationError(
                "You must specify a remote URL (backend.url) and a token "
                "(backend.token) using the config command."
            )

        headers = {
            'content-type': 'application/json',
            'Authorization': "Token {}".format(token)
        }

        return dest, headers

    def _get_remote_projects(self):
        if not hasattr(self, '_remote_projects'):
            dest, headers = self._get_request_info('projects')

            try:
                response = requests.get(dest, headers=headers)
                assert response.status_code == 200

                self._remote_projects = response.json()
            except requests.ConnectionError:
                raise WatsonError("Unable to reach the server.")
            except AssertionError:
                raise WatsonError(
                    "An error occured with the remote "
                    "server: {}".format(response.json())
                )

        return self._remote_projects

    def pull(self, last_sync):
        dest, headers = self._get_request_info('frames')

        try:
            response = requests.get(
                dest, params={'last_sync': last_sync}, headers=headers
            )
            assert response.status_code == 200
        except requests.ConnectionError:
            raise WatsonError("Unable to reach the server.")
        except AssertionError:
            raise WatsonError(
                "An error occured with the remote "
                "server: {}".format(response.json())
            )

        for frame in response.json() or ():
            try:
                # Try to find the project name, as the API returns an URL
                project = next(
                    p['name'] for p in self._get_remote_projects()
                    if p['url'] == frame['project']
                )
            except StopIteration:
                raise WatsonError(
                    "Received frame with invalid project from the server "
                    "(id: {})".format(frame['project']['id'])
                )

            yield Frame(frame['start'], frame['stop'], project, frame['id'],
                        frame['tags'], frame.get('updated_at'))

    def push(self, frames):
        dest, headers = self._get_request_info('frames/bulk')

        to_upload = []

        for frame in frames:
            try:
                # Find the url of the project
                project = next(
                    p['url'] for p in self._get_remote_projects()
                    if p['name'] == frame.project
                )
            except StopIteration:
                raise WatsonError(
                    "The project {} does not exists on the remote server, "
                    "please create it or edit the frame (id: {})".format(
                        frame.project, frame.id
                    )
                )

            to_upload.append({
                'id': frame.id,
                'start': str(frame.start),
                'stop': str(frame.stop),
                'project': project,
                'tags': frame.tags
            })

        try:
            response = requests.post(dest, json.dumps(to_upload),
                                     headers=headers)
            assert response.status_code == 201
        except requests.ConnectionError:
            raise WatsonError("Unable to reach the server.")
        except AssertionError:
            raise WatsonError(
                "An error occured with the remote "
                "server: {}".format(response.json())
            )

        return to_upload

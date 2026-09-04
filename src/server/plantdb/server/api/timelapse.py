#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Timelapse REST API Resources

Provides Flask-RESTful resources for managing timelapse datasets in PlantDB.
"""

import logging
from flask import request
from flask_restful import Resource

from plantdb.commons.fsdb.core import FSDB
from plantdb.commons.fsdb.exceptions import NoAuthUserError
from plantdb.commons.fsdb.exceptions import TimeLapseExistsError
from plantdb.commons.fsdb.exceptions import TimeLapseNotFoundError
from plantdb.commons.log import get_logger
from plantdb.server.core.security import add_jwt_from_header
from plantdb.server.core.security import rate_limit
from plantdb.server.core.security import sanitize_ids
from plantdb.server.core.security import use_guest_as_default


class Timelapses(Resource):
    """Resource for listing and creating timelapse containers."""

    def __init__(self, db, logger=None):
        self.db: FSDB = db
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__)

    @rate_limit(max_requests=30, window_seconds=60)
    def get(self):
        """List all timelapse containers."""
        try:
            tl_list = self.db.list_timelapses()
            return tl_list, 200
        except Exception as e:
            return {'error': f'Error retrieving timelapse list: {str(e)}'}, 500

    @rate_limit(max_requests=30, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default
    def post(self, **kwargs):
        """Create a new timelapse container."""
        data = request.get_json(silent=True)
        if not data or not isinstance(data, dict):
            return {'error': 'Invalid request body: expected JSON object'}, 400

        tl_id = data.get('id') or data.get('name')
        if not tl_id:
            return {'error': "Missing 'id' field for timelapse creation"}, 400

        metadata = data.get('metadata', {})
        try:
            tl = self.db.create_timelapse(tl_id, metadata=metadata, **kwargs)
            res = tl.to_dict() if hasattr(tl, "to_dict") else tl
            return res, 201
        except TimeLapseExistsError as e:
            return {'error': str(e)}, 409
        except ValueError as e:
            return {'error': str(e)}, 400
        except PermissionError as e:
            return {'error': str(e)}, 403
        except NoAuthUserError as e:
            return {'error': str(e)}, 401
        except Exception as e:
            return {'error': f'Error creating timelapse: {str(e)}'}, 500


class Timelapse(Resource):
    """Resource for inspecting and deleting a specific timelapse container."""

    def __init__(self, db, logger=None):
        self.db: FSDB = db
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__)

    @sanitize_ids('timelapse_id')
    @rate_limit(max_requests=60, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default
    def get(self, timelapse_id, **kwargs):
        """Get timelapse metadata and scan counts."""
        try:
            tl_data = self.db.get_timelapse(timelapse_id, **kwargs)
            res = tl_data.to_dict() if hasattr(tl_data, "to_dict") else tl_data
            return res, 200
        except TimeLapseNotFoundError as e:
            return {'error': str(e)}, 404
        except Exception as e:
            return {'error': f'Error getting timelapse: {str(e)}'}, 500

    @sanitize_ids('timelapse_id')
    @rate_limit(max_requests=30, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default
    def delete(self, timelapse_id, **kwargs):
        """Delete a timelapse container."""
        recursive_param = request.args.get('recursive', 'false').lower()
        recursive = recursive_param in ('true', '1', 'yes')
        try:
            self.db.delete_timelapse(timelapse_id, recursive=recursive, **kwargs)
            return "", 204
        except TimeLapseNotFoundError as e:
            return {'error': str(e)}, 404
        except ValueError as e:
            return {'error': str(e)}, 409
        except PermissionError as e:
            return {'error': str(e)}, 403
        except NoAuthUserError as e:
            return {'error': str(e)}, 401
        except Exception as e:
            return {'error': f'Error deleting timelapse: {str(e)}'}, 500


class TimelapseScans(Resource):
    """Resource for retrieving member scans of a specific timelapse."""

    def __init__(self, db, logger=None):
        self.db: FSDB = db
        self.logger: logging.Logger = logger if logger else get_logger(self.__class__.__name__)

    @sanitize_ids('timelapse_id')
    @rate_limit(max_requests=60, window_seconds=60)
    @add_jwt_from_header
    @use_guest_as_default
    def get(self, timelapse_id, **kwargs):
        """List member scans of a timelapse, sorted."""
        sort = request.args.get('sort', 'timelapse.scheduled')
        fuzzy = request.args.get('fuzzy', False, type=bool)
        try:
            scans_list = self.db.list_scans(query={"timelapse": {"id": timelapse_id}}, fuzzy=fuzzy, owner_only=False, sort=sort)
            return scans_list, 200
        except Exception as e:
            return {'error': f'Error retrieving timelapse scans: {str(e)}'}, 500

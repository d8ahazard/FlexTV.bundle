from helpers.storage import StorageHelper

import logging
import os
import sys

log = logging.getLogger(__name__)


class PathHelper(object):
    @classmethod
    def insert(cls, base, system, architecture, *args):
        """Insert a new path into `sys.path` if it passes basic validation

        :type base: str
        :type system: str
        :type architecture: str
        """

        path = os.path.join(base, system, architecture, *args)

        if path in sys.path:
            log.debug("path exists in system path already: %r", StorageHelper.to_relative_path(path))
            return False

        if not os.path.exists(path):
            log.debug("path doesn't exist: %r", StorageHelper.to_relative_path(path))
            return False

        sys.path.insert(0, path)

        log.debug('Inserted path: %r', StorageHelper.to_relative_path(path))
        return True

    @classmethod
    def remove(cls, path):
        """Remove path from `sys.path` if it exists

        :type path: str
        """

        if path not in sys.path:
            return False

        sys.path.remove(path)

        log.debug('Removed path: %r', StorageHelper.to_relative_path(path))
        return True

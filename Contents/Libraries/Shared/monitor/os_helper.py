import logging
import os
import platform

log = logging.getLogger(__name__)

NAME_MAP = {
    'Darwin': 'MacOSX'
}

FALLBACK_EXECUTABLE = '/bin/ls'


class OsHelper(object):
    @classmethod
    def name(cls):
        """Retrieve system name (Windows, Linux, FreeBSD, MacOSX)"""

        system = platform.system()

        # Check for android platform
        if (system == 'Linux') & (cls.is_android()):
            system = 'Android'

        # Apply system name map
        if system in NAME_MAP:
            system = NAME_MAP[system]

        return system

    @classmethod
    def is_android(cls):
        result = False
        try:
            # Check android platform criteria
            if not os.path.exists('/system/build.prop'):
                # Couldn't find "build.prop" file
                result = False
            elif os.path.exists('/system/lib/libandroid_runtime.so'):
                # Found "libandroid_runtime.so" file
                log.info('Detected android system (found the "libandroid_runtime.so" file)')
                result = True
            elif '-google' in platform.python_compiler():
                # Found "-google" in the python compiler attribute
                log.info('Detected android system (found "-google" in the python compiler attribute)')
                result = True
            else:
                log.warn('Found the "build.prop" file, but could\'t confirm if the system is running android')
                result = False

        except Exception as ex:
            log.warn('Unable to check if the system is running android: %s', ex, exc_info=True)

        return result
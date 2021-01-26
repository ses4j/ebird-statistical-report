#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys


def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ebirddb.settings')

    os.environ.setdefault('OSGEO4W_ROOT', r'C:\OSGeo4W')
    os.environ.setdefault('GDAL_DATA', r'C:\OSGeo4W\share\gdal')
    os.environ.setdefault('PROJ_LIB', r'C:\OSGeo4W\share\proj')
    
    os.environ['PATH'] = os.environ['PATH'] + ";" + os.environ['OSGEO4W_ROOT'] + r"\bin"

    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()

import os
import pytest

# Settings for OMERO

DEFAULT_OMERO_USER = "root"
DEFAULT_OMERO_PASS = "omero"
DEFAULT_OMERO_HOST = "localhost"
DEFAULT_OMERO_PORT = 6064
DEFAULT_OMERO_SECURE = 1

def pytest_addoption(parser):
    parser.addoption("--omero-user", action="store",
        default=os.environ.get("OMERO_USER", DEFAULT_OMERO_USER))
    parser.addoption("--omero-pass", action="store",
        default=os.environ.get("OMERO_PASS", DEFAULT_OMERO_PASS))
    parser.addoption("--omero-host", action="store",
        default=os.environ.get("OMERO_HOST", DEFAULT_OMERO_HOST))
    parser.addoption("--omero-port", action="store", type=int,
        default=int(os.environ.get("OMERO_PORT", DEFAULT_OMERO_PORT)))
    parser.addoption("--omero-secure", action="store",
        default=bool(os.environ.get("OMERO_HOST", DEFAULT_OMERO_SECURE)))

""" Functions and extensions to transparently convert to and from shapely.

    This module provides the "adapt_shapely" and "cast_hexwkb" functions, that
    convert between shapely BaseGeometries and PostGIS's hex-wkb format.
    If the register-functions are called, casting happens transparently behind
    the scenes, and shapely objects can be passed as substitution arguments
    to cursor.execute.

    :author: Gil Georges <gil.georges@lav.mavt.ethz.ch>
    :date: November 23, 2016
    """

import psycopg2.extensions
import psycopg2
import shapely.wkb
from shapely.geometry.base import BaseGeometry
import re
import warnings


class PostGISnotInstalled(Exception):
    """PostGIS not available in current connection."""

    pass


def has_postgis(conn):
    """Check if the connection has access to PostGIS."""
    try:
        get_postgis_version(conn)
    except PostGISnotInstalled:
        return False
    else:
        return True


def get_postgis_version(conn):
    """Return PostGIS version as tuple (major, minor).

    Raises as PostGISnotInstalled Exception if PostGIS is not available.
    Otherwise returns a tuple of integers.

    Returns:
        tuple - int
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT * from PostGIS_Version()")
    except psycopg2.ProgrammingError:
        return False
    else:
        ver = cur.fetchone()[0]
        match = re.match("^([0-9]+)\.([0-9]+)", ver)
        return tuple(int(s) for s in match.groups())
    finally:
        cur.close()
    raise PostGISnotInstalled()


def cast_hexwkb(value, cur):
    """Convert PostGIS 'value' to the corresponding shapely type."""
    if value is None:
        return None
    return shapely.wkb.loads(value, hex=True)


def adapt_shapely(value):
    """Convert a shapely object to PostGIS hex-wkb."""
    wkb = shapely.wkb.dumps(value, hex=True, include_srid=True)
    return psycopg2.extensions.AsIs(psycopg2.extensions.adapt(wkb))


def register_postgis2shapely(conn):
    """Register 'cast_hexwkb' to transparently convert results from queries."""
    for t_name in ("GEOGRAPHY", "GEOMETRY"):
        t_code = get_type_code(t_name, conn)
        typ = psycopg2.extensions.new_type((t_code,), t_name, cast_hexwkb)
        psycopg2.extensions.register_type(typ)


def get_type_code(type, conn):
    """Get typecopde from database."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT NULL::{}".format(str(type)))
        code = cur.description[0].type_code
    except psycopg2.ProgrammingError as e:
        conn.rollback()
        if str(e.pgcode) == "42704":  # re-raise if it's not what we expected
            raise PostGISnotInstalled
        raise e
    finally:
        cur.close()
    return code


def get_type_object(name):
    """Get registered type by name."""
    try:
        return next(s for s in psycopg2.extensions.string_types.values()
                    if s.name == name)
    except StopIteration:
        raise KeyError(name)


def register_shapely2postgis():
    """Register 'adapt_shapely' as an adapter."""
    psycopg2.extensions.register_adapter(BaseGeometry, adapt_shapely)


def activate(conn=None):
    """Register postgis -> shapely and back."""
    register_postgis2shapely(conn)
    register_shapely2postgis()

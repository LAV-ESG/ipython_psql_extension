from .ipython_extension import pgMagics
import os


def load_ipython_extension(ipython):
    # The `ipython` argument is the currently active `InteractiveShell`
    # instance, which can be used in any way. This allows you to register
    # new magics or aliases, for example.
    magics = pgMagics(ipython,
                      default_host="lav-fileserver",
                      default_port=5433,
                      default_sslcert=os.path.join(os.path.expanduser("~"),
                                                   "subnetz.org.crt"),
                      default_user=os.getlogin())
    ipython.register_magics(magics)

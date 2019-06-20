#!/usr/bin/env python

from setuptools import setup
from subprocess import check_output, PIPE

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements


def git(*args):
    cmd = ["git"] + list(args)
    return check_output(cmd, stderr=PIPE).decode('utf-8').strip()


def version():
    try:
        return git('describe', '--tags')
    except:
        pass

    try:
        return git('rev-parse', '--short', 'HEAD')
    except:
        pass

    return "(unkown)"


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements("requirements.txt", session=False)

# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]


setup(name='ipython_pg',
      version=version(),
      description='An IPython extension to access Postgres',
      author='Gil Georges',
      author_email='ggeorges@ethz.ch',
      url='www.gilgeorges.ch',
      packages=['ipython_pg'],
      install_requires=reqs
      )

#!/bin/bash

die() {
	echo "ABORTED: $1"
	exit 1
}

python ./setup.py clean
#python ./setup.py nosetests || die "tests failed"
python ./setup.py build || die "build failed"
python ./setup.py sdist || die "source-dist failed"
python ./setup.py bdist_wheel || die "bdist-wheel failed"
python ./setup.py install || echo "ERROR: local installation failed"

echo "cleaning up..."
python ./setup.py clean
rm -r build
rm -r ipython_pg.egg-info


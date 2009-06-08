#! /usr/bin/env python

from distutils.core import setup

setup(
    name = "pkgloader",
    license = "BSD",
    version = '1.0',
    maintainer = "Marko Kreen",
    maintainer_email = "markokr@gmail.com",
    package_dir = {'': 'python'},
    py_modules = ['pkgloader'],
)


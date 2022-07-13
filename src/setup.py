import os
import sys
import platform
from setuptools import setup, find_packages
from distutils.core import Extension

setup(name = 'pyg-mongo', version = '0.0.13', packages = find_packages(), python_requires = '>=3.6.')
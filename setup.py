"""
    Setup file 
"""

import os
import pathlib
from setuptools import setup, find_packages

# Utility function to read the README file.

def read(fname):
    return open(pathlib.Path(__file__).parent.joinpath(fname)).read()
#    return open(os.path.join(os.path.dirname(__file__), fname)).read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='PyAppCodacAssignment',
    author='Monika Wojciechowska',
    author_email='monika.wojciechowska@capgemini.com',
    version='1.3',
    description=('Application with python and pyspark code'),
    long_description=read('README.md'),
    packages=find_packages(exclude=['tests']),
    install_requires=required,
    project_urls={'Source': 'https://github.com/mowoj/PythonAssignment.git'}
)
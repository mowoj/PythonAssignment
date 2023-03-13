"""
    Setup file 
"""

import os
import pathlib
from setuptools import setup, find_packages

def read_file_contentad(fname):
    """
    Utility function to read the README file.
    Args:
        fname (str): file name

    Returns:
        str: readme.md file content
    """
    return open(pathlib.Path(__file__).parent.joinpath(fname)).read()

def open_file (fname):
    """
    Utility function to read requirements.txt file
    Args:
        fname (str): file name

    Returns:
        list: requirements.txt file content
    """
    with open(fname) as f:
        required = f.read().splitlines()
    return required

setup(
    name='PyAppCodacAssignment',
    author='Monika Wojciechowska',
    author_email='monika.wojciechowska@capgemini.com',  
    version='1.3',
    description=('Application with python and pyspark code'),
    long_description=read_file_contentad('README.md'),
    packages=find_packages(exclude=['tests']),
    install_requires=open_file('requirements.txt'),
    project_urls={'Source': 'https://github.com/mowoj/PythonAssignment.git'}
)
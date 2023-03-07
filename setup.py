"""
    Setup file 
"""

import pathlib 
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
     return open(pathlib.Path(__file__).parent.joinpath(fname)).read()
#    return open(os.path.join(os.path.dirname(__file__), fname)).read()

#setup(
#     name = 'PyAppCodacAssignment'
#    ,author = 'Monika Wojciechowska'
#    ,author_email = 'monika.wojciechowska@capgemini.com'
#    ,version = '1.0'
#    #,description = ('.........'
#    #                '.........')   #todo fill in
#    #,long_description = read('README.md')
#    #,package_dir = {'.....': '....'} #todo fill in
#    ,packages=find_packages()
#    #,packages = find_packages(where = '....') #todo fill in
#    #,setup_requires = ['......'] #todo fill in
#    #,project_urls={ 'Source': 'https://github.com/.......'  }#todo fill in
#    
#    )
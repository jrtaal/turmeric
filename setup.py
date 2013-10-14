import os
from setuptools import setup
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()
    
setup(
    name = "turmeric",
    version = "0.1",
    author = "Jacco Taal",
    author_email = "jacco@bitnomica.com",
    description = ("""Turmeric is  database management tool to
                   create, initialize, backup and populate a database. It is based on the excellent SQLAlchemy project.
                   """),
    license = "All rights reserved",
    keywords = "database management",
    url = "http://gitlab.bitnomica.com/vidacle-team/pyplyne",
    long_description=read('README.md'),
    classifiers=[ ],
    packages=find_packages(exclude = ["test"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=["sqlalchemy>0.8" , "psycopg2" ,"babel"],
    tests_require=[],
    setup_requires = [ "setuptools-git>=0.3"],
    test_suite="lifeshare",
    entry_points = """\
      [console_scripts]
      turmeric = turmeric.turmeric:main
      """,
)


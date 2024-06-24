#! python

"""
Setup script for biofilter package.

This script is used to configure the installation of the biofilter package. It specifies various metadata about the package, such as its name, version, author, and dependencies. Additionally, it defines the distribution files to be included in the package and any custom commands needed for installation and distribution.

Attributes:
    name (str): The name of the package.
    version (str): The version number of the package.
    description (str): The description of the package.
    author (str): The author(s) of the package.
    author_email (str): The email address of the package author(s).
    url (str): The URL of the package's homepage.
    scripts (list): A list of script files to be included in the package.
    packages (list): A list of Python packages to be included in the package.
    cmdclass (dict): A dictionary mapping custom command names to their respective classes.
    data_files (list): A list of additional data files to be included in the package.
"""

import setuptools

setuptools.setup(
        name='biofilter',
        version='3.0.0',
        author='Ritchie Lab',
        author_email='Software_RitchieLab@pennmedicine.upenn.edu',
        url='https://ritchielab.org',
        scripts=[
                'loki-build.py',
                'biofilter.py',
		'biofilterClass.py'
        ],
	packages=[
                'loki',
                'loki.loaders',
                'loki.loaders.test',
                'loki.util'
        ],
	data_files=[
                ('', ['CHANGELOG','biofilter-manual-2.4.pdf'])
        ]
)

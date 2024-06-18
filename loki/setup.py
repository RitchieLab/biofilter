#! python

"""
Setup script for LOKI knowledge base.

This script is used to configure the installation of the LOKI knowledge base. It specifies various metadata about the package, such as its name, version, author, and dependencies. Additionally, it defines the distribution files to be included in the package and any custom commands needed for installation and distribution.

Attributes:
    name (str): The name of the knowledge base.
    version (str): The version number of the knowledge base.
    author (str): The author(s) of the knowledge base.
    author_email (str): The email address of the knowledge base author(s).
    url (str): The URL of the knowledge base's homepage.
    scripts (list): A list of script files to be included in the knowledge base.
    packages (list): A list of Python packages to be included in the knowledge base.
    cmdclass (dict): A dictionary mapping custom command names to their respective classes.
    data_files (list): A list of additional data files to be included in the knowledge base.
"""


import distutils.core
import distutils.command.install
import distutils.command.sdist

distutils.core.setup(
	name='LOKI',
	version='3.0.0',
	author='Ritchie Lab',
	author_email='Software_RitchieLab@pennmedicine.upenn.edu',
	url='https://ritchielab.org',
	scripts=[
		'loki-build.py'
	],
	packages=[
		'loki',
		'loki.loaders',
		'loki.loaders.test',
		'loki.util'
	],
	cmdclass={
		'install':distutils.command.install.install,
		'sdist':distutils.command.sdist.sdist
	},
	data_files=[
		('', ['CHANGELOG'])
	]
)
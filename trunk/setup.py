#! python

import distutils.core
import distutils.command.install
import distutils.command.sdist

distutils.core.setup(
	name='biofilter',
	version='2.4.2',
	author='Ritchie Lab',
	author_email='Software_RitchieLab@pennmedicine.upenn.edu',
	url='https://ritchielab.org',
	scripts=[
		'loki-build.py',
		'biofilter.py'
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
		('', ['CHANGELOG','biofilter-manual-2.4.pdf'])
	]
)

#! python



from distutils.core import setup
import distutils.command.install

import autodist

autodist.auto_dirs = ['loki/loadPops']

class my_install(autodist.auto_install):
	"""
	Script for installation (add --ldprofile)
	"""
	
	distutils.command.install.install.user_options.append(("ldprofile", None, "Enable ldprofile scripts"))
	distutils.command.install.install.boolean_options.append("ldprofile")
	
	def initialize_options(self):
		autodist.auto_install.initialize_options(self)
		self.ldprofile = False
		
	def run(self):
		"""
		Call auto_install ONLY if ldprofile is enabled!
		"""
		# If not using the ldprofile, remove buildpopulations
		if not self.ldprofile:
			self.distribution.scripts.remove('loki/loadPops/buildPopulations.py')
			distutils.command.install.install.run(self)
		else:
			autodist.auto_install.run(self)

setup(name='biofilter',version='2.0.0',
	author='Ritchie Lab',author_email='software@ritchielab.psu.edu',
	url='http://ritchielab.psu.edu',
	scripts=['loki/loki-build.py','biofilter.py','loki/loadPops/buildPopulations.py'],
	packages=['loki','loki.loaders','loki.loaders/test','loki.util'],
	cmdclass={'install':my_install, 'sdist':autodist.auto_sdist})

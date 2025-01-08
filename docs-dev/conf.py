# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
sys.path.insert(0, os.path.abspath("../"))

project = 'Biofilter Developer Documentation'
copyright = '2024, Ritchie Lab'
author = 'Ritchie Lab'
release = '2.4.4'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon'
    # 'recommonmark'  # Support.md files
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# html_theme = 'alabaster'
html_static_path = ['_static']

html_theme = "sphinx_material"

html_theme_options = {
    "nav_title": "Biofilter Developer Docs",
    "base_url": "https://github.com/RitchieLab/biofilter",
    "color_primary": "blue",
    "color_accent": "light-blue",
    "repo_url": "https://github.com/RitchieLab/biofilter",
    "repo_name": "Biofilter",
    "globaltoc_depth": 2,
    "globaltoc_collapse": True,
    "globaltoc_includehidden": True,
}

html_sidebars = {
    "**": [
        "logo-text.html",
        "globaltoc.html",
        "localtoc.html",
        "searchbox.html"
        ]
}

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = 'plantdb'
copyright = '2021, Robotics for Microfarms'
author = 'Robotics for Microfarms'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'myst_parser',  # Markdown parser
    'sphinx.ext.autodoc',  # Include documentation from docstrings
    'sphinx.ext.autosummary',  # Include documentation from docstrings
    'sphinx.ext.intersphinx',  # Link to other projects’ documentation
    'sphinx.ext.napoleon',  # Support for NumPy and Google style docstrings
    'sphinx.ext.viewcode'  # Add links to highlighted source code
]

myst_enable_extensions = [
    "amsmath",
    "dollarmath",
    "colon_fence",
    "deflist"
]
# Activate auto-generated header anchors (for Hearders H1-3)
myst_heading_anchors = 3

# sphinx.ext.napoleon settings:
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_keyword = True
napoleon_use_param = True
napoleon_use_rtype = False

# sphinx.ext.viewcode settings:
viewcode_follow_imported_members = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The suffix of source filenames.
source_suffix = '.md'

# The main index filename.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_material'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_css_files = ["_static/css/extra.css"]

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    'repo_url': 'https://github.com/romi/plantdb/',
    'globaltoc_depth': 2,
    'color_primary': 'green'
}

# A shorter title for the navigation bar.  Default is the same as html_title.
html_short_title = project

# The name of an image file (relative to this directory) to place at the top
# of the sidebar.
html_logo = '_static/images/logo.svg'

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
html_favicon = '_static/images/ROMI_favicon_green.png'


# -- Intersphinx -------------------------------------------------------------
# Configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {'python': ('https://docs.python.org/3', None),
                       'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
                       'numpy': ('https://numpy.org/doc/stable/', None),
                       'matplotlib': ('https://matplotlib.org/stable/', None),
                       'scikit-learn': ('https://scikit-learn.org/stable/', None),
                       'scikit-image': ('https://scikit-image.org/docs/stable/', None),
                       'scipy': ('https://docs.scipy.org/doc/scipy/', None)
                       }

# List of `intersphinx_mapping`:
# https://gist.github.com/bskinn/0e164963428d4b51017cebdb6cda5209


NAME = 'stdf-utils'
VERSION = '0.2.0'
DESCRIPTION = 'stdf file parser and emitter'
LONG_DESCRIPTION = """\
stdf is the standard format of ATE result, and this pacakge supports
reading and (possibly) editing the stdf file comes form ATE.
"""
AUTHOR = "Peter JC. Wu"
AUTHOR_EMAIL = "wolf952@gmail.com"
LICENSE = "MIT"
PLATFORMS = "any"
CLASSIFIERS = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Cython",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: Text Processing :: Markup",
]

PROJECT_URLS = {
   'Source Code': 'https://github.com/peterjcwu/stdf-utils',
}

dependencies = [
]

from setuptools import setup

if __name__ == '__main__':
    setup(
        name=NAME,
        version=VERSION,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        license=LICENSE,
        platforms=PLATFORMS,
        classifiers=CLASSIFIERS,
        project_urls=PROJECT_URLS,
        python_requires='>=3.6',
    )

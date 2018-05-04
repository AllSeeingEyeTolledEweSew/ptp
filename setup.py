# The author disclaims copyright to this source code. Please see the
# accompanying UNLICENSE file.

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="ptp",
    version="0.0.1",
    description="API and local cache for passthepopcorn.me",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/ptp",
    license="Unlicense",
    packages=find_packages(),
    use_2to3=True,
    install_requires=[
        "beautifulsoup4>=4.6.0",
        "python-dateutil>=2.7.2",
        "better-bencode>=0.2.1",
        "PyYAML>=3.12",
        "requests>=2.12.3",
        "tbucket>=1.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ptp_scrape = ptp.cli.scrape:main",
        ],
    },
    #classifiers=[
    #    "Development Status :: 5 - Production/Stable",
    #    "Intended Audience :: Developers",
    #    "License :: Public Domain",
    #    "Programming Language :: Python",
    #    "Topic :: Communications :: File Sharing",
    #    "Topic :: Database",
    #    "Topic :: Software Development :: Libraries :: Python Modules",
    #    "Topic :: System :: Networking",
    #    "Operating System :: OS Independent",
    #    "License :: Public Domain",
    #],
)

from setuptools import setup, find_packages
# import os
# import re
from mycluster.version import get_git_version


classes = """
    Development Status :: 4 - Beta
    Intended Audience :: Developers
    License :: OSI Approved :: BSD License
    Topic :: System :: Logging
    Programming Language :: Python
    Programming Language :: Python :: 2
    Programming Language :: Python :: 2.7
    Operating System :: POSIX :: Linux
"""
classifiers = [s.strip() for s in classes.split('\n') if s]


setup(
    name='MyCluster',
    version=get_git_version(),
    packages=['mycluster'],
    package_dir={'mycluster': 'mycluster'},
    license='BSD',
    author='Zenotech',
    author_email='admin@zenotech.com',
    url='https://github.com/zenotech/MyCluster',
    classifiers=classifiers,
    description='Utilities to support interacting with multiple HPC clusters',
    long_description=open('README.md').read(),
    install_requires=['ZODB', 'SysScribe', 'fabric<2.0', 'zodbpickle', 'Jinja2'],
    scripts=['scripts/mycluster'],
    include_package_data=True,
    package_data={
        'mycluster': ['templates/*.jinja'],
        '': ['*.md', 'RELEASE-VERSION']
    },
    data_files=[('share/MyCluster', ['RELEASE-VERSION']),
                ('share/MyCluster', ['share/mycluster-OF-simpleFoam.bsh',
                                     'share/mycluster-fluent.bsh',
                                     'share/mycluster-zcfd.bsh',
                                     'share/mycluster-paraview.bsh'])
                ],
)

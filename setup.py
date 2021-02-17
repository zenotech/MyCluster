from setuptools import setup, find_packages
import os


classes = """
    Development Status :: 4 - Beta
    Intended Audience :: Developers
    License :: OSI Approved :: BSD License
    Topic :: System :: Logging
    Programming Language :: Python
    Programming Language :: Python :: 3
    Programming Language :: Python :: 3.7
    Programming Language :: Python :: 3.8
    Operating System :: POSIX :: Linux
"""
classifiers = [s.strip() for s in classes.split('\n') if s]

version = os.environ.get('RELEASE_VERSION', '0.0.0')

if __name__ == "__main__":
    setup(
        name='MyCluster',
        version=version,
        packages=['mycluster'],
        package_dir={'mycluster': 'mycluster'},
        license='BSD',
        author='Zenotech',
        author_email='admin@zenotech.com',
        url='https://github.com/zenotech/MyCluster',
        classifiers=classifiers,
        description='Utilities to support interacting with multiple HPC clusters',
        long_description_content_type="text/markdown",
        long_description=open('README.md').read(),
        install_requires=['ZODB', 'SysScribe', 'future', 'Fabric', 'zodbpickle', 'Jinja2'],
        scripts=['scripts/mycluster'],
        include_package_data=True,
        package_data={
            'mycluster': ['templates/*.jinja'],
            '': ['*.md']
            },
        data_files=[
            ('share/MyCluster', ['share/mycluster-OF-simpleFoam.bsh',
                'share/mycluster-fluent.bsh',
                'share/mycluster-zcfd.bsh',
                'share/mycluster-paraview.bsh'])
            ],
    )

#!/usr/bin/env python
import os
import setuptools

version = os.environ.get("RELEASE_VERSION", "0.0.0")

if __name__ == "__main__":
    setuptools.setup(version=version)

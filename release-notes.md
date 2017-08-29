
run prospector
> prospector --die-on-tool-error
Fix issues

Create annotated tag

> git tag -a "0.1.9" -m "Tagging new release"
> git push --tags

See http://peterdowns.com/posts/first-time-with-pypi.html except skip the
repository line in the .pypirc file

Setup venv
  pip install twine setuptools
> python setup.py sdist
  twine upload dist/*

run prospector
> prospector --die-on-tool-error
Fix issues

Create annotated tag

> git tag -a "0.1.9" -m "Tagging new release"
> git push --tags

See http://peterdowns.com/posts/first-time-with-pypi.html 

> python setup.py sdist upload -r pypi
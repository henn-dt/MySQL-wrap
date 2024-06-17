# henn_package_template
A repository to use as a template for creating packages.

put the code under src/project_name
adjust the project name under devcontainer.json, pyproject.toml

if the package need dependencies, manually add them under [project] in pyproject.toml:

dependencies = [
          package1,
          package2,
          ...
          ]

to build the package:

    python3 -m build

this will create the package files, a .whl and a .tar.gz file. 
after that push to github

to use the package, add this line to the requirements.txt of your app:

    git+{url of the repo}.git


To run tests: 

- add your test file to the tests/ folder

- import the modules you want to test using src.folder.module path

- run tests from terminal from the project root folder:
    python3 -m unittest tests.{test file} 

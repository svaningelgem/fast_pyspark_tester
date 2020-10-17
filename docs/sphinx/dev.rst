.. _dev:

Development
===========

Fork the Github repository and apply your changes in a feature branch.
To run fast_pyspark_tester's unit tests:

.. code-block:: sh

    # install
    pip install -e .[hdfs,performance,streaming,test]
    flake8 --install-hook

    # run linting and test
    flake8
    pytest -vv

Don't run ``python setup.py test`` as this will
not execute the doctests. When all tests pass, create a Pull Request on GitHub.
Please also update ``HISTORY.rst`` with short description of your change.

To preview the docs locally, install the extra dependencies with
``pip install -r docs/requirements.txt``, and then cd into ``docs/sphinx``,
run ``make html`` and open ``_build/html/index.html``.

Please also try not to add derivative work from other projects. If you do,
incorporate proper handling of external licenses in your Pull Request.

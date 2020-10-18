#   -*- coding: utf-8 -*-
import logging
from pathlib import Path

from pybuilder.core import use_plugin, init, before, after, Author, Project
from pybuilder.errors import BuildFailedException
from pybuilder.vcs import VCSRevision

use_plugin('python.core')
use_plugin('python.flake8')
use_plugin('python.distutils')
use_plugin('python.pylint')
# https://github.com/AlexeySanko/pybuilder_pytest
use_plugin('pypi:pybuilder_pytest')
# https://github.com/AlexeySanko/pybuilder_pytest_coverage
use_plugin('pypi:pybuilder_pytest_coverage')


name = 'fast_pyspark_tester'
default_task = 'publish'
version = '0.6.0'
license = 'MIT'
summary = 'Pure Python implementation of the pyspark interface.'
description = None
authors = [
    Author(name='Sven Kreiss', email='me@svenkreiss.com'),
    Author(name="Erwan Guyomarc'h", email='tools4origins@gmail.com'),
    Author(name='Steven Van Ingelgem', email='steven@vaningelgem.be'),
]
maintainers = [
    Author(name='Steven Van Ingelgem', email='steven@vaningelgem.be'),
]
requires_python = '>= 3.6'
url = 'https://github.com/svaningelgem/fast_pyspark_tester/'

_logging_root_level: int = logging.root.getEffectiveLevel()


@before('run_unit_tests')
def _set_debug_mode():
    logging.root.setLevel('DEBUG')


@after('run_unit_tests')
def _reset_debug_mode():
    logging.root.setLevel(_logging_root_level)


@after('package')
def _add_extras_require(project, logger):
    indent_size = 4
    encoding = 'utf-8'

    setup_script = Path(project.expand_path('$dir_dist', 'setup.py'))
    logger.info("Adding 'extras_require' to setup.py")
    setup = setup_script.read_text(encoding=encoding).rstrip()
    if setup[-1] != ')':
        raise BuildFailedException('This setup.py seems to be wrong?')

    # Get the requirements-dev.txt file line by line, ready for insertion.
    requirements_dev = '\n'.join(
        ' ' * 4 * indent_size + "'" + x.strip() + "',"
        for x in (Path(__file__).parent / 'requirements-build.txt').read_text().split('\n')
        if x
    )

    # TODO: find a nicer way to embed this!
    new_setup = (
        setup[:-1].rstrip()
        + f'''
        extras_require={{
            'hdfs': ['hdfs>=2.0.0'],
            'pandas': ['pandas>=0.23.2'],
            'performance': ['matplotlib>=1.5.3'],
            'streaming': ['tornado>=4.3'],
            'test': [
{requirements_dev}
            ]
        }},
    )
'''
    )

    setup_script.write_text(new_setup, encoding=encoding)


@after('package')
def write_version_into_library(project: Project):
    version_file = Path(project.expand_path('$dir_dist', f'{project.name}/__version__.py'))
    version_file.write_text(
        version_file.read_text().replace('%version%', project.version).replace('%hash%', VCSRevision().get_git_hash())
    )


@init
def set_properties(project):
    # Small tweak to project.list_scripts() as that method lists EVERYTHING in the scripts directory.
    #   and we're only interested in *.py files:
    old_project_list_scripts = project.list_scripts

    def _my_list_scripts():
        return [filename for filename in old_project_list_scripts() if filename.lower().endswith('.py')]

    setattr(project, 'list_scripts', _my_list_scripts)

    project.depends_on_requirements(file='requirements.txt')

    project.set_property('pylint_break_build', True)

    project.set_property('flake8_break_build', True)
    project.set_property('flake8_include_test_sources', True)
    project.set_property('flake8_include_scripts', True)

    project.set_property('distutils_readme_description', True)
    project.set_property('distutils_readme_file', 'README.rst')

    project.set_property('pytest_coverage_xml', True)
    project.set_property('pytest_coverage_html', True)
    project.set_property('pytest_coverage_break_build_threshold', 0)  # Don't let coverage break the build (for now)

    project.set_property('distutils_console_scripts', [])
    project.set_property(
        'distutils_classifiers',
        [
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'Natural Language :: English',
            'License :: OSI Approved :: MIT License',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: Implementation :: PyPy',
        ],
    )


if __name__ == '__main__':
    from pybuilder.cli import main

    main('-CX', '--no-venvs')

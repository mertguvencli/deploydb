#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [ 'pyodbc', 'GitPython', 'pydantic' ]

test_requirements = [ 'pyodbc', 'GitPython', 'pydantic' ]

setup(
    author="Mert Güvençli",
    author_email='guvenclimert@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Deploy your database objects automatically when the git branch is updated.",
    # entry_points={
    #     'console_scripts': [
    #         'deploydb=deploydb.cli:main',
    #     ],
    # },
    install_requires=requirements,
    license="GNU General Public License v3",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords=['deploydb', 'sql server', 'source control', 'deployment'],
    name='deploydb',
    packages=find_packages(include=['deploydb', 'deploydb.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/mertguvencli/deploydb',
    version='0.1.3',
    zip_safe=False,
)

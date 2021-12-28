import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as fh:
   requirements = fh.readlines()
   requirements = [requirement.strip().replace('\n','').replace('\r','') for requirement in requirements]
   requirements = [requirement for requirement in requirements if len(requirement) != 0 and requirement[0] != '#']

setuptools.setup(
    name='databasepy',
    version='0.0.2',
    author='Kingsley Nweye',
    author_email='nweye@utexas.edu',
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=requirements,
    entry_points={'console_scripts': ['databasepy = databasepy.main:main']},
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)
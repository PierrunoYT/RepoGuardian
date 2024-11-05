from setuptools import setup, find_packages

setup(
    name='github-backup-sync-tool',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'GitPython>=3.1.31',
        'python-dotenv'
    ],
    entry_points={
        'console_scripts': [
            'github-backup=main:main',
        ],
    },
    author='Your Name',
    description='A tool for backing up and syncing GitHub repositories',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)

from setuptools import setup, find_packages
import tornetcd

setup(
        name="tornetcd",
        version=tornetcd.__version__,
        description="async etcd(python-etcd) client for tornado",
        long_description="async etcd client for tornado.",
        keywords='python etcd async tornado',
        author="mqingyn",
        url="https://github.com/mqingyn/tornetcd",
        license="BSD",
        packages=find_packages(),
        author_email="mqingyn@gmail.com",
        requires=['tornado'],
        classifiers=[
            'Development Status :: 4 - Beta',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.6',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
        ],
        scripts=[],
        install_requires=[
            'tornado',
        ],
)

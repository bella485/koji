from setuptools import setup


def get_install_requires():
    # To install all build requires:
    # $ dnf install python-pip git krb5-devel gcc redhat-rpm-config \
    #               glib2-devel sqlite-devel libxml2-devel python-devel \
    #               openssl-devel libffi-devel

    requires = [
        'python-dateutil',
        'requests',
        'requests-gssapi',
        'pyOpenSSL',
        # 'libcomps',
        # 'rpm-py-installer', # it is optional feature
        # 'rpm',
    ]

    return requires


setup(
    name="koji",
    version="1.22.0",
    description=("Koji is a system for building and tracking RPMS. The base"
                 " package contains shared libraries and the command-line"
                 " interface."),
    license="LGPLv2 and GPLv2+",
    url="http://pagure.io/koji/",
    author='Koji developers',
    author_email='koji-devel@lists.fedorahosted.org',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
        "Topic :: Utilities"
    ],
    packages=['koji', 'koji_cli', 'koji_cli_plugins'],
    package_dir={
        'koji': 'koji',
        'koji_cli': 'cli/koji_cli',
        'koji_cli_plugins': 'plugins/cli',
    },
    # doesn't make sense, as we have only example config
    # data_files=[
    #     ('/etc', ['cli/koji.conf']),
    # ],
    scripts=[
        'cli/koji',
        'util/koji-gc',
        'util/koji-shadow',
        'util/koji-sweep-db',
        'util/kojira',
    ],
    python_requires='>=3.0',
    install_requires=get_install_requires(),
)

from setuptools import setup

import os


version = "0.0.1"

long_description = "\n\n".join(
    [open("README.rst").read(), open("CHANGES.rst").read()]
)

install_requires = []

# emulate "--no-deps" on the readthedocs build (there is no way to specify this
# behaviour in the .readthedocs.yml)
if os.environ.get("READTHEDOCS") == "True":
    install_requires = []


tests_require = [
    "pytest",
]

setup(
    name="tiny-blocks",
    version=version,
    description="Tiny Blocks to build large and complex pipelines!",
    long_description=long_description,
    # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=["python", "tiny-blocks"],
    author="JM Vazquez",
    author_email="",
    url="https://github.com/pyprogrammerblog/tiny-blocks",
    license="MIT License",
    packages=["tiny_blocks"],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    python_requires=">=3.9",
    extras_require={"test": tests_require},
    entry_points={"console_scripts": []},
)

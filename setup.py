from setuptools import setup


version = "0.1.1"

long_description = "\n\n".join(
    [
        open("README.md").read(),
        open("README_USERS.md").read(),
        open("CHANGES.md").read(),
    ]
)

setup(
    name="tiny-blocks",
    version=version,
    description="Tiny Blocks to build large and complex pipelines!",
    long_description=long_description,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=["python", "tiny-blocks"],
    author="JM Vazquez",
    url="https://github.com/pyprogrammerblog/tiny-blocks",
    license="MIT License",
    packages=["tiny_blocks"],
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.9",
)

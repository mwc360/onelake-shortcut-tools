import setuptools

# Read the contents of your README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read the contents of the requirements.txt file
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="onlake-shortcut-tools",
    version="0.1.0",
    author="Miles Cole",
    author_email="m.w.c.360@gmail.com",
    description="A library of tools for OneLake shortcuts.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    project_urls={},
    classifiers=[
        "Development Status :: Beta",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development",
        "License :: OSI Approved :: MIT License",
    ],
    packages=setuptools.find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.9",
    install_requires=requirements
)

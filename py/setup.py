import os

import setuptools


dir_name = os.path.abspath(os.path.dirname(__file__))

version_contents = {}
with open(os.path.join(dir_name, "src", "qvm", "version.py"), encoding="utf-8") as f:
    exec(f.read(), version_contents)

with open(os.path.join(dir_name, "README.md"), "r", encoding="utf-8") as f:
    long_description = f.read()

install_requires = [
    "nba_api",
]

extras_require = {
    "dev": [
        "black",
        "build",
        "flake8",
        "flake8-isort",
        "IPython",
        "isort==5.10.1",
        "pre-commit",
        "pytest",
        "twine",
    ],
}

extras_require["all"] = sorted({package for packages in extras_require.values() for package in packages})

setuptools.setup(
    name="qvm",
    version=version_contents["VERSION"],
    author="MAndrews & Ankur",
    author_email="ankrgyl@gmail.com",
    description="Scripts/utils for working with QVM",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TODO/qvm",
    project_urls={
        "Bug Tracker": "https://github.com/TODO/qvm/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.7.0",
    entry_points={"console_scripts": ["nba-scraper = qvm.nba.__main__:main"]},
    install_requires=install_requires,
    extras_require=extras_require,
)

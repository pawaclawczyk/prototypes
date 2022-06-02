import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def read_requirements(path: str = "requirements.txt") -> list[str]:
    with open(path, "r") as fp:
        return fp.readlines()


setuptools.setup(
    name="budget_pacing",
    version="0.1.0+dev",
    author="Paweł Wacławczyk",
    author_email="p.a.waclawczyk@gmail.com",
    description="Simulation comapring various budget pacing algorithms",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pawaclawczyk/simulation-pacing",
    project_urls={
        "Bug Tracker": "https://github.com/pawaclawczyk/simulation-pacing/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=read_requirements(),
    extras_require={
        "dev": read_requirements("requirements.dev.txt")
    },
    python_requires=">=3.10",
)

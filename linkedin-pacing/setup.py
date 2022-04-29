import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="simulation-pacing_simulation",
    version="0.1.0",
    author="Paweł Wacławczyk",
    author_email="p.a.waclawczyk@gmail.com",
    description="Simulation of ad server workload with LinkedIn pacing_simulation algorithm",
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
    python_requires=">=3.10",
)

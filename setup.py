#! python Setup.py

from setuptools import setup, find_packages

setup(
    name="biofilter",
    version="2.4.4",
    author="Ritchie Lab",
    author_email="Software_RitchieLab@pennmedicine.upenn.edu",
    url="https://ritchielab.org",
    packages=find_packages(
        include=[
            "biofilter_modules",
            "biofilter_modules.*",
            "loki_modules",
            "loki_modules.*",
        ]
    ),
    install_requires=[
        "apsw==3.46.1.0",
        "click==8.1.7",
        "iniconfig==2.0.0",
        "packaging==24.1",
        "platformdirs==4.3.6",
        "pluggy==1.5.0",
    ],
    include_package_data=True,
    # package_data={
    #     "": ["CHANGELOG", "biofilter-manual-2.4.pdf"],
    # },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "biofilter=biofilter_modules.biofilter:main",
            "loki-build=loki_modules.loki_build:main",
        ],
    },
)

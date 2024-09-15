from setuptools import find_packages, setup

setup(
    name="vaccination_us",
    version="1.1",
    description="Vaccination Data for US",
    package_dir={"": "ingestion"},
    packages=find_packages("ingestion"),
    install_requires=["acryl-datahub"],
)

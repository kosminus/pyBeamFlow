import setuptools

setuptools.setup(
    name='pyBeamFlow',
    version='1.0.0',
    install_requires=["configparser","pyarrow","fastavro"],
    packages=setuptools.find_packages(include=["beamflow"])
)

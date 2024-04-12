import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jax-omeroutils",
    version="0.1.2",
    maintainer="Dave Mellert",
    maintainer_email="Dave.Mellert@jax.org",
    description=("A package for working with OMERO"
                 "by Research IT at The Jackson Laboratory."),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TheJacksonLaboratory/jax-omeroutils",
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas>=1.1.5',
        'numpy>=1.22.0,<2.0.0',
        'openpyxl==3.0.9',
        'omero-cli-transfer>=1.0.0,<1.1.0'
    ],
    python_requires='>=3.8'
)

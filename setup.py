import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="jax-omeroutils",
    version="0.0.1",
    maintainer="Dave Mellert",
    maintainer_email="Dave.Mellert@jax.org",
    description=("A package for working with OMERO"
                 "by ResearchIT at The Jackson Laboratory."),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TheJacksonLaboratory/jax-omeroutils",
    packages=setuptools.find_packages(),
    install_requires=[
        'omero-py',
        'pandas',
        'numpy',
        'xlrd'
    ],
    python_requires='>=3.6'
)

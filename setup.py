from distutils.core import setup

setup(
    name="pickle-jar",
    packages=["jar"],
    version="0.1",  # update for new ver
    license="MIT",
    description="A container for pickle slices.",
    author="jkvc",
    author_email="kevinehc@gmail.com",
    url="https://github.com/jkvc/pickle-jar",
    download_url="https://github.com/jkvc/pickle-jar/archive/refs/tags/v_01.tar.gz",  # update for new ver
    keywords=["pickle", "jar", "container", "serialization"],
    install_requires=[],
    classifiers=[
        "Development Status :: 4 - Beta",  # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        "Intended Audience :: Developers",  # Define that your audience are developers
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
)

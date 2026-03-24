from setuptools import setup, find_packages

# Read the contents of your README file for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("VERSION") as f:
    version = f.read().strip()

setup(
    name="oracle-utils", # Replace with your preferred package name
    version=version,
    author="Josh Price",
    author_email="joshua.price@intellitrans.com",
    description="A utility for Oracle database interaction and SQL transformation using sqlglot",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # url="https://github.com/yourusername/your-repo-name",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Database",
    ],
    python_requires=">=3.8",
    install_requires=[
        "oracledb>=2.0.0",
        "sqlglot>=20.0.0",
        "python-dotenv>=1.0.0",
    ],
)
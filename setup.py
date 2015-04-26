from setuptools import setup, find_packages

setup(
    name='hv',
    version='0.1.1',
    description='postgresql connection manager for scalability freaks',
    url='http://github.com/tetsuo/hypervault',
    author='Onur Gunduz',
    author_email='ogunduz@gmail.com',
    packages=find_packages(),
    install_requires=['psycopg2'],
    license='mit',
)
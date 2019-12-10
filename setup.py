from setuptools import setup

setup(
   name='idsw',
   version='2.0',
   description='IDSW SDK',
   author='Brent',
   author_email='xuande@inspur.com',
   packages=['idsw'],  #same as name
   install_requires=['hdfs3', 'IPython', 'requests']
)

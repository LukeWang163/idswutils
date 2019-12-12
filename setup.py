from setuptools import setup

setup(
   name='idsw',
   version='3.0',
   description='IDSW SDK',
   author='Luke',
   author_email='wangj_lc@inspur.com',
   # package_data={"idsw": ["idsw-notebook.conf"]},
   # include_package_data=True,
   packages=['idsw'],  #same as name
   install_requires=['pandas', 'scikit-learn', 'pymysql']
)

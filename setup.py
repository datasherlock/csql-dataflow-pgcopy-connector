import setuptools

setuptools.setup(
    name='DataflowToCloudSQL',
    version='0.1',
    packages=setuptools.find_packages(),
    url='',
    license='',
    author='datasherlock',
    author_email='',
    description='',
    package_data={
          'DataflowToCloudSQL': ['common/*.ini'],
       },
    include_package_data=True,
)

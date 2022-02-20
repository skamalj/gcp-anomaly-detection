import setuptools
setuptools.setup(
    name='Anamoly-Detector',
    version='0.1',
    install_requires=['cassandra-driver==3.25.0','pg8000==1.23.0','pandas==1.3.5',
        'google-cloud-bigquery==2.31.0','google-cloud-bigquery-storage==2.10.1'],
    packages=setuptools.find_packages(),
)


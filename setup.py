import setuptools

setuptools.setup(
    name='pypipex',
    version='0.0.5',
    description='A Data Pipeline Framework using Apache Beam Pipelines, adding PTransforms and utility functions.',
    author='israelmartinez.data.engineer@gmail.com',
    packages=setuptools.find_packages(
        include=[
            'src'
            , 'src.*'
            ]
    ),
    install_requires=[
        'apache-beam[gcp]==2.65.0',
    ],
    python_requires='>=3.8',
    include_package_data=True,
    zip_safe=False,
)

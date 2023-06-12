from setuptools import setup

setup(
    name='greensense_backend',
    version='0.1.0',
    description='Backend service for Competence Project',
    url='https://github.com/competence-project/greensense-backend',
    author='Mikołaj Rajczyk',
    author_email='mikolajrajczyk01@gmail.com',
    packages=['greensense_backend'],
    install_requires=['paho.mqtt',
                      'fastapi',
                      'uvicorn',
                      ],

    classifiers=[],
)
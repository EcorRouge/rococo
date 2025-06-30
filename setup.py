from setuptools import find_packages, setup


extras_require = {}

extras_require["data"] = [
    'DBUtils==3.1.0',
    'surrealdb==1.0.4',
    'PyMySQL==1.1.1',
    'PyMongo==4.6.3',
    'psycopg2-binary==2.9.10'
]

extras_require["emailing"] = [
    'mailjet_rest==1.3.4'
]

extras_require["messaging"] = [
    'pika==1.3.2'
]

extras_require["all"] = [
    *extras_require["data"],
    *extras_require["emailing"],
    *extras_require["messaging"],
]


setup(
    name='rococo',
    version='1.0.36',
    packages=find_packages(),
    url='https://github.com/EcorRouge/rococo',
    license='MIT',
    author='Jay Grieves',
    author_email='jaygrieves@gmail.com',
    description='A Python library to help build things the way we want them built',
    entry_points={
        'console_scripts': [
            'rococo-mysql = rococo.migrations.mysql.cli:main',
            'rococo-postgres = rococo.migrations.postgres.cli:main',
            'rococo-mongo = rococo.migrations.mongo.cli:main',
        ],
    },
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'boto3>=1.28.55',
        'python-dotenv==1.0.0'
    ],
    extras_require=extras_require,
    python_requires=">=3.10"
)

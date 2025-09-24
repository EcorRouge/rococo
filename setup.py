from setuptools import find_packages, setup


extras_require = {}

extras_require["emailing"] = [
    'mailjet_rest>=1.4.0,<2.0'
]

extras_require["messaging"] = [
    'pika>=1.3.2,<1.4'
]

extras_require["faxing"] = [
    'requests>=2.31.0,<3.0'
]

extras_require["data-common"] = [
    'DBUtils>=3.1,<4.0'
]

extras_require["data-surreal"] = [
    'surrealdb>=1.0.4,<1.1'
]

extras_require["data-mysql"] = [
    'PyMySQL>=1.1.1,<1.2',
]

extras_require["data-mongo"] = [
    'PyMongo>=4.6.3,<5.0',
]

extras_require["data-postgres"] = [
    'psycopg2-binary>=2.9.10,<3.0'
]

extras_require["data"] = [
    *extras_require["data-common"],
    *extras_require["data-surreal"],
    *extras_require["data-mysql"],
    *extras_require["data-mongo"],
    *extras_require["data-postgres"],
]

extras_require["all"] = [
    *extras_require["data"],
    *extras_require["emailing"],
    *extras_require["messaging"],
    *extras_require["faxing"],
]


setup(
    name='rococo',
    version='1.1.8',
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
        'python-dotenv>=1.0.0,<2.0'
    ],
    extras_require=extras_require,
    python_requires=">=3.10"
)

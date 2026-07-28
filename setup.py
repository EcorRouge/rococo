from setuptools import find_packages, setup


extras_require = {}

extras_require["sms"] = [
    'twilio>=9.8.6,<10.0',
    'jinja2>=3.1.6,<4.0'
]

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
    'surrealdb>=1.0.7,<1.1'
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

extras_require["data-dynamodb"] = [
    'pynamodb>=6.0.0,<7.0'
]

extras_require["data"] = [
    *extras_require["data-common"],
    *extras_require["data-surreal"],
    *extras_require["data-mysql"],
    *extras_require["data-mongo"],
    *extras_require["data-postgres"],
    *extras_require["data-dynamodb"],
]

# Base — required by the logging handler regardless of whether tracing is
# used at all. Every service using rococo's observability module needs this.
# opentelemetry-api is included here because the logging handler always tries
# to correlate log records with the active trace, and the @traced/@traced_step
# decorators depend only on the API (not the full SDK/exporter stack).
extras_require["observability"] = [
    'requests>=2.31.0,<3.0',
    'opentelemetry-api>=1.24,<2.0',
]

# Required by anything that calls get_tracer_provider() / OpenObserveTracer,
# regardless of framework — the base building blocks for tracing itself.
extras_require["observability-tracing-core"] = [
    'opentelemetry-sdk>=1.24,<2.0',
    'opentelemetry-exporter-otlp-proto-http>=1.24,<2.0',
]

# The opentelemetry-instrumentation-* packages only ever publish prereleases
# (0.45b0 ... 0.65b0), so the lower bound has to name one explicitly: a plain
# >=0.45 excludes every existing release and installers resolve nothing.
extras_require["observability-flask-tracing"] = [
    'opentelemetry-instrumentation-flask>=0.45b0,<1.0',
    'opentelemetry-instrumentation-requests>=0.45b0,<1.0',
    # Used directly to detect streaming responses when wrapping view functions.
    'Werkzeug>=2.0,<4.0',
]

extras_require["observability-fastapi-tracing"] = [
    'opentelemetry-instrumentation-fastapi>=0.45b0,<1.0',
    'opentelemetry-instrumentation-httpx>=0.45b0,<1.0',
    'opentelemetry-instrumentation-requests>=0.45b0,<1.0',
]

extras_require["observability-postgres-tracing"] = [
    'opentelemetry-instrumentation-psycopg2>=0.45b0,<1.0',
]

# Python range capped per this package's own declared support window
extras_require["observability-langgraph-tracing"] = [
    'openinference-instrumentation-langchain>=0.1.67,<0.2.0; python_version < "3.15"',
]

extras_require["observability-tracing"] = [
    *extras_require["observability-tracing-core"],
    *extras_require["observability-flask-tracing"],
    *extras_require["observability-fastapi-tracing"],
    *extras_require["observability-postgres-tracing"],
    *extras_require["observability-langgraph-tracing"],
]

extras_require["all"] = [
    *extras_require["data"],
    *extras_require["emailing"],
    *extras_require["messaging"],
    *extras_require["faxing"],
    *extras_require["sms"],
    *extras_require["observability"],
    *extras_require["observability-tracing"],
]


setup(
    name='rococo',
    version='1.3.4',
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

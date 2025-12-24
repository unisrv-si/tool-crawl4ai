from setuptools import setup, find_packages

setup(
    name="tool-crawl4ai",
    version="0.1.0",
    py_modules=[
        "util",
        "simple_web_crawl",
        "table_unspanner",
        "test_removing_javascript",
    ],
    install_requires=[
        "Crawl4AI==0.7.7",
        "python-dotenv==1.2.1",
        "pandas==2.3.3",
        "tabulate==0.9.0"
    ],
)

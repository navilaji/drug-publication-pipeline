from setuptools import setup

mypackage = 'drug_analysis_pipeline'

setup(
    name=mypackage,
    version='0.0.1',
    packages=[mypackage],
    author='Navid SHARIAT RAZAVI',
    description='generates a graph showing the relation between the drugs and medical publications',
    url='https://github.com/navilaji/servier-medics-data-pipeline',
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)

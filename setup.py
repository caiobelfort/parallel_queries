from setuptools import setup

setup(
    name='parallel_queries',
    version='1.0.0',
    description='A project for parallelization of SQL statement in client side.',
    author='Caio Belfort',
    author_email='caiobelfort90@gmail.com',
    license='GPL',
    py_modules=['parallel_queries'],
    zip_safe=False,
    install_requirements=['sqlalchemy', 'joblib'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Software Development :: Database',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6'
        'Programming Language :: Python :: 3.7',
        'Operation System :: POSIX'
    ]
)

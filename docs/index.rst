.. tiny-blocks documentation master file, created by
   sphinx-quickstart on Tue Aug  9 09:38:22 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
.. _index:

Welcome to tiny-blocks' documentation!
=======================================

Tiny Blocks to build large and complex pipelines!
It is a library for streaming operations, composed using the >> operator. This allows for easy extract, transform and load operations.

Pipeline Components: Sources, Pipes, and Sinks
-----------------------------------------------
This library relies on a fundamental streaming abstraction consisting of three parts: extract, transform, and load.
You can view a pipeline as a extraction, followed by zero or more transformations, followed by a sink.
Visually, this looks like::

   source >> pipe1 >> pipe2 >> pipe3 >> ... >> pipeN >> sink


Basic usage example
-------------------

Make sure you had install the package by doing ``pip install tiny-blocks`` and then::

   from tiny_blocks.extract import FromCSV
   from tiny_blocks.transform import DropDuplicates
   from tiny_blocks.transform import Fillna
   from tiny_blocks.load import ToSQL
   from tiny_blocks import Pipeline

   # ETL Blocks
   from_csv = FromCSV(path='/path/to/file.csv')
   drop_duplicates = DropDuplicates()
   fill_na = Fillna(value="Hola Mundo")
   to_sql = ToSQL(dsn_conn='psycopg2+postgres:...')

   # Run it as a Pipeline
   with Pipeline(name="My Pipeline") as pipe:
       pipe >> from_csv >> drop_duplicates >> fill_na >> to_sql


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   tiny-blocks
   extract
   transform
   load
   license
   help


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
